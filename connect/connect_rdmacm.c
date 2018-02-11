#include "opal_config.h"

#include <rdma/rdma_cma.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <net/if.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <dirent.h>
#include <stddef.h>

#include "opal/util/output.h"
#include "opal/util/error.h"
#include "opal/util/show_help.h"
#include "opal/util/proc.h"
#include "opal/runtime/opal_progress_threads.h"

#include "btl_openib_proc.h"
#include "btl_openib_endpoint.h"
#include "connect/connect.h"
#include "btl_openib_ip.h"
#include "btl_openib_ini.h"

#include <stdio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <rdma/rsocket.h>
#include <infiniband/ib.h>

#define mymin(a, b) ((a) < (b) ? (a) : (b))

// ============== related data for osdh_connect_base_component_t ==================

static void rdmacm_component_register(void);
static int rdmacm_component_init(void);
static int rdmacm_component_query(sdh_module_t *openib_btl,
                                  osdh_connect_base_module_t **cpc);
static int rdmacm_component_finalize(void);

// overall file implement the following four functions.
osdh_connect_base_component_t osdh_connect_rdmacm = {
    "rdmacm",
    rdmacm_component_register,
    rdmacm_component_init,
    rdmacm_component_query,
    rdmacm_component_finalize
};

// ============== rdma_contents_t ====================================
//
// A single instance of this data structure is shared between one
// id_context_t for each BSRQ qp on an endpoint.
//
//   server==false means that this proc initiated the connection;
//   server==true means that this proc accepted the incoming
//   connection.  Note that this may be different than the "one way"
//   i_initiate() direction -- it is possible for server==false
//   and i_initiate() to return false; it means that this proc
//   initially initiated the connection, but we expect it to be
//   rejected. 
//
typedef struct {
    opal_list_item_t super;
    sdh_endpoint_t *endpoint;
    sdh_module_t *openib_btl;
    // Dummy QP only used when we expect the connection to be rejected 
    struct ibv_cq *dummy_cq;
    union ibv_gid gid;
    uint64_t service_id;
    bool server;
    // Whether this contents struct has been saved on the client list or not 
    bool on_client_list;
    // A list of all the id_context_t's that are using this rdmacm_contents_t 
    opal_list_t ids;
} rdmacm_contents_t;

static void rdmacm_contents_constructor(rdmacm_contents_t *contents);
static void rdmacm_contents_destructor(rdmacm_contents_t *contents);

OBJ_CLASS_INSTANCE(rdmacm_contents_t,opal_list_item_t, rdmacm_contents_constructor,rdmacm_contents_destructor);

// =================== modex_message_t =========================

typedef struct {
    int device_max_qp_rd_atom;
    int device_max_qp_init_rd_atom;
    uint8_t  gid[16];
    uint64_t service_id;
    uint8_t end;
} modex_message_t;

typedef struct {
    int rdmacm_counter;
} rdmacm_endpoint_local_cpc_data_t;


// =================== id_context_t =========================
//
// There are one of these for each RDMA CM ID.  Because of BSRQ, there
// can be multiple of these for one endpoint, so all the
// id_context_t's on a single endpoing share a single
// rdmacm_contents_t.
//
typedef struct {
    opal_list_item_t super;
    rdmacm_contents_t *contents; // 
    sdh_endpoint_t *endpoint;
    uint8_t qpnum;
    bool already_disconnected;
    uint16_t route_retry_count;
    struct rdma_cm_id *id;
} id_context_t;

static void id_context_constructor(id_context_t *context);
static void id_context_destructor(id_context_t *context);
OBJ_CLASS_INSTANCE(id_context_t, opal_list_item_t, id_context_constructor, id_context_destructor);

// =================== private_data_t =========================
/*
* According to infiniband spec a "Consumer Private Data" begings from 36th up
* to 91th byte (so the limit is 56 bytes) and first 36 bytes
* intended for lib RDMA CM header (sometimes not all of these bytes are used)
* so we must take into account that in case of AF_IB user private data pointer
* points to a header and not to a "Consumer Private Data".
*/

typedef struct {
    uint8_t  librdmacm_header[36]; // RDMA CM header
    uint64_t rem_port;
    uint32_t rem_index;
    uint8_t qpnum;
    opal_process_name_t rem_name;
} __opal_attribute_packed__ private_data_t;


static opal_list_t server_listener_list;
static opal_list_t client_list;
static opal_mutex_t client_list_lock;

static struct rdma_event_channel *event_channel = NULL;
static int rdmacm_priority = 30;
static unsigned int rdmacm_port = 0;

static int rdmacm_resolve_timeout = 30000;
static int rdmacm_resolve_max_retry_count = 20;
static bool rdmacm_reject_causes_connect_error = false;
static pthread_cond_t rdmacm_disconnect_cond;
static pthread_mutex_t rdmacm_disconnect_lock;
static volatile int disconnect_callbacks = 0;
static bool rdmacm_component_initialized = false;
static opal_event_base_t *rdmacm_event_base = NULL;
static opal_event_t rdmacm_event;

// Calculate the *real* length of the message (not aligned/rounded up) 
static int message_len = offsetof(modex_message_t, end);

// Rejection reasons 
typedef enum {
    REJECT_WRONG_DIRECTION,
    REJECT_TRY_AGAIN
} reject_reason_t;

static void id_context_constructor(id_context_t *context)
{
    context->already_disconnected = false;
    context->id = NULL;
    context->contents = NULL;
    context->endpoint = NULL;
    context->qpnum = 255;
    context->route_retry_count = 0;
}

static void id_context_destructor(id_context_t *context)
{
    if (NULL != context->id) 
    {
        rdma_destroy_id(context->id);
        context->id = NULL;
    }
    if (NULL != context->contents) 
    {
        OBJ_RELEASE(context->contents);
    }
}

static void rdmacm_contents_constructor(rdmacm_contents_t *contents)
{
    contents->endpoint = NULL;
    contents->openib_btl = NULL;
    contents->dummy_cq = NULL;
    contents->service_id = 0;
    contents->server = false;
    contents->on_client_list = false;
    OBJ_CONSTRUCT(&(contents->ids), opal_list_t);
}

static void rdmacm_contents_destructor(rdmacm_contents_t *contents)
{
    OBJ_DESTRUCT(&(contents->ids));
}

// ====================== rdma_cm_component_register ======================
/*
 * Invoked by main thread
 *
 * Sets up any rdma_cm specific commandline params
 */
static void rdmacm_component_register(void)
{
    // the priority is initialized in the declaration above 
    (void) mca_base_component_var_register(&sdh_component.super.btl_version,
                                           "connect_rdmacm_priority",
                                           "The selection method priority for rdma_cm",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &rdmacm_priority);
    if(rdmacm_priority > 100) 
    {
        rdmacm_priority = 100;
    } 
    else if(rdmacm_priority < 0) 
    {
        rdmacm_priority = 0;
    }

    rdmacm_port = 0;
    (void) mca_base_component_var_register(&sdh_component.super.btl_version,
                                           "connect_rdmacm_port",
                                           "The selection method port for rdma_cm",
                                           MCA_BASE_VAR_TYPE_UNSIGNED_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &rdmacm_port);
    if (rdmacm_port & ~0xfffful) {
        opal_show_help("help-mpi-btl-openib-cpc-rdmacm.txt",
                       "illegal tcp port", true, (int) rdmacm_port);
        rdmacm_port = 0;
    }

    rdmacm_resolve_timeout = 30000;
    (void) mca_base_component_var_register(&sdh_component.super.btl_version,
                                           "connect_rdmacm_resolve_timeout",
                                           "The timeout (in miliseconds) for address and route resolution",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &rdmacm_resolve_timeout);
    if (0 > rdmacm_resolve_timeout) 
    {
        opal_show_help("help-mpi-btl-openib-cpc-rdmacm.txt",
                       "illegal timeout", true, rdmacm_resolve_timeout);
        rdmacm_resolve_timeout = 30000;
    }

    rdmacm_resolve_max_retry_count = 20;
    (void) mca_base_component_var_register(&sdh_component.super.btl_version,
                                           "connect_rdmacm_retry_count",
                                           "Maximum number of times rdmacm will retry route resolution",
                                           MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &rdmacm_resolve_max_retry_count);
    if (0 > rdmacm_resolve_max_retry_count) 
    {
        opal_show_help("help-mpi-btl-openib-cpc-rdmacm.txt",
                       "illegal retry count", true, rdmacm_resolve_max_retry_count);
        rdmacm_resolve_max_retry_count = 20;
    }

    rdmacm_reject_causes_connect_error = false;
    (void) mca_base_component_var_register(&sdh_component.super.btl_version,
                                           "connect_rdmacm_reject_causes_connect_error",
                                           "The drivers for some devices are buggy such that 
                                           an RDMA REJECT action may result in 
                                           a CONNECT_ERROR event instead of a REJECTED event.  
                                           Setting this MCA parameter to true tells 
                                           Open MPI to treat CONNECT_ERROR events on connections 
                                           where a REJECT is expected as a REJECT (default: false)",
                                           MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                           OPAL_INFO_LVL_9,
                                           MCA_BASE_VAR_SCOPE_READONLY,
                                           &rdmacm_reject_causes_connect_error);
}


// Helper function for when we are debugging
static char *stringify(uint32_t addr)
{
    char *line = (char *) malloc(64);
    asprintf(&line, "%d.%d.%d.%d (0x%x)",
#if defined(WORDS_BIGENDIAN)
             (addr >> 24),
             (addr >> 16) & 0xff,
             (addr >> 8) & 0xff,
             addr & 0xff,
#else
             addr & 0xff,
             (addr >> 8) & 0xff,
             (addr >> 16) & 0xff,
             (addr >> 24),
#endif
             addr);
    return line;
}

// ================================ rdmacm_component_init =========================
/*
 *
 * Invoked by service thread
 *
 * This function traverses the list of endpoints associated with the
 * device and determines which of them the remote side is attempting
 * to connect to.  This is determined based on the local endpoint's
 * modex message recevied and the IP address and port associated with
 * the rdma_cm event id
 *
 * will be called by handle_connect_request
 *
 */
static sdh_endpoint_t *rdmacm_find_endpoint(rdmacm_contents_t *contents, opal_process_name_t rem_name)
{
    sdh_module_t *btl = contents->openib_btl;
    sdh_endpoint_t *ep = NULL;
    opal_proc_t *opal_proc;

    opal_proc = opal_proc_for_name (rem_name);
    if (NULL == opal_proc) 
    {
        //could not get proc associated with remote peer 
        return NULL;
    }

    ep = sdh_get_ep (&btl->super, opal_proc); // get endpoint. !!!
    if (NULL == ep) 
    {
        // could not find endpoint for peer
    }

    return ep;
}

/*
 * Returns max inlne size for qp #N
 *
 * called by rdma_cm_setup_qp.
 *
 */
static uint32_t max_inline_size(int qp, sdh_device_t *device)
{
    if (sdh_component.qp_infos[qp].size <= device->max_inline_data)
    {
        // If qp message size is smaller than max_inline_data,
        // we should enable inline messages 
        return sdh_component.qp_infos[qp].size;
    } else if (sdh_component.rdma_qp == qp || 0 == qp) {
        // If qp message size is bigger that max_inline_data, we
        // should enable inline messages only for RDMA QP (for PUT/GET
        // fin messages) and for the first qp 
        return device->max_inline_data;
    }
    // Otherwise it is no reason for inline 
    return 0;
}


/*
 * Invoked by both main and service threads
 *
 * 1: configure ibv_qp+init_attr accoring to rdmacm_contents_t.
 *
 * 2: create qp by call ibv_create_qp.
 * 
 */
static int rdmacm_setup_qp(rdmacm_contents_t *contents,
                           sdh_endpoint_t *endpoint,
                           struct rdma_cm_id *id,
                           int qpnum)
{
    struct ibv_qp_init_attr attr;
    struct ibv_qp *qp;
    struct ibv_srq *srq = NULL;
    int credits = 0, reserved = 0, max_recv_wr, max_send_wr;
    size_t req_inline;

    if (qpnum == sdh_component.credits_qp) {
        int qp;

        for (qp = 0; qp < sdh_component.num_qps; qp++) {
            if(BTL_OPENIB_QP_TYPE_PP(qp)) {
                reserved += sdh_component.qp_infos[qp].u.pp_qp.rd_rsv;
            }
        }
        credits = sdh_component.num_qps;
    }

    if (BTL_OPENIB_QP_TYPE_PP(qpnum)) {
        max_recv_wr = sdh_component.qp_infos[qpnum].rd_num + reserved;
        max_send_wr = sdh_component.qp_infos[qpnum].rd_num + credits;
    } else {
        srq = endpoint->endpoint_btl->qps[qpnum].u.srq_qp.srq;
        max_recv_wr = reserved;
        max_send_wr = sdh_component.qp_infos[qpnum].u.srq_qp.sd_max + credits;
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_type = IBV_QPT_RC;
    // where cq are created ????
    attr.send_cq = contents->openib_btl->device->ib_cq[BTL_OPENIB_LP_CQ]; 
    attr.recv_cq = contents->openib_btl->device->ib_cq[qp_cq_prio(qpnum)];
    attr.srq = srq;
    if(BTL_OPENIB_QP_TYPE_PP(qpnum)) {
        /// Add one for the CTS receive frag that will be posted 
        attr.cap.max_recv_wr = max_recv_wr + 1;
    } else {
        attr.cap.max_recv_wr = 0;
    }
    attr.cap.max_send_wr = max_send_wr;
    attr.cap.max_inline_data = req_inline =
                              max_inline_size(qpnum, contents->openib_btl->device);
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1; /* we do not use SG list */

    {
        /* JMS Temprary gross hack: we *must* use rdma_create_cp()
           (vs. ibv_create_qp()) because strange things happen on IB
           if we don't.  However, rdma_create_cp() wants us to use
           rdma_get_devices() (and therefore the pd that they have
           allocated).  In order to get v1.3 out the door, we're
           bypassing this functionality - we're temporarily overriding
           the device context cached on the ID with our own, so that
           our pd will match.  We need to fix this to properly get the
           pd from the RDMA CM and use that, etc. */
        struct ibv_context *temp = id->verbs;
        id->verbs = contents->openib_btl->device->ib_pd->context;
        // create qp
        if (0 != rdma_create_qp(id, contents->openib_btl->device->ib_pd, &attr)) {
            BTL_ERROR(("Failed to create qp with %d", qpnum));
            goto out;
        }
        qp = id->qp; 
        id->verbs = temp;
    }

    endpoint->qps[qpnum].qp->lcl_qp = qp;
    endpoint->qps[qpnum].credit_frag = NULL;
    if (attr.cap.max_inline_data < req_inline) {
        endpoint->qps[qpnum].ib_inline_max = attr.cap.max_inline_data;
        opal_show_help("help-mpi-btl-openib-cpc-base.txt",
                       "inline truncated", true,
                       opal_process_info.nodename,
                       ibv_get_device_name(contents->openib_btl->device->ib_dev),
                       contents->openib_btl->port_num,
                       req_inline, attr.cap.max_inline_data);
    } else {
        endpoint->qps[qpnum].ib_inline_max = req_inline;
    }
    id->qp = qp;

    return OPAL_SUCCESS;

out:
    return OPAL_ERROR;
}


/*
 * Invoked by both main and service threads
 *
 * To avoid all kinds of nasty race conditions, we only allow
 * connections to be made in one direction.  So use a simple
 * (arbitrary) test to decide which direction is allowed to initiate
 * the connection: the process with the lower IP address wins.  If the
 * IP addresses are the same (i.e., the MPI procs are on the same
 * node), then the process with the lower TCP port wins.
 */
static bool i_initiate(uint64_t local_port, uint64_t remote_port,
                       union ibv_gid *local_gid, union ibv_gid *remote_gid)
{
    if (local_gid->global.subnet_prefix < remote_gid->global.subnet_prefix ||
        (local_gid->global.subnet_prefix == remote_gid->global.subnet_prefix &&
         local_gid->global.interface_id < remote_gid->global.interface_id) ||
        (local_gid->global.subnet_prefix == remote_gid->global.subnet_prefix &&
         local_gid->global.interface_id == remote_gid->global.interface_id &&
              local_port < remote_port)) 
    {
        return true;
    }

    return false;
}

static int get_rdma_addr(char *src, char *dst,
                         struct rdma_addrinfo **rdma_addr,
                         int server)
{
    int rc;
    struct rdma_addrinfo hints, *sres, *dres;

    memset(&hints, 0, sizeof hints);

    hints.ai_family = AF_IB;
    hints.ai_port_space = RDMA_PS_TCP;
    hints.ai_flags = RAI_NUMERICHOST | RAI_FAMILY | RAI_PASSIVE;

    rc = rdma_getaddrinfo(src, NULL, &hints, &sres);
    if (0 != rc) 
    {
        return OPAL_ERROR;
    }

    if (server) 
    {
        *rdma_addr = sres;
        return OPAL_SUCCESS;
    }

    hints.ai_src_len  = sres->ai_src_len;
    hints.ai_src_addr = sres->ai_src_addr;

    hints.ai_flags &= ~RAI_PASSIVE;

    rc = rdma_getaddrinfo(dst, NULL, &hints, &dres);
    if (0 != rc) 
    {
        rdma_freeaddrinfo(sres);
        return OPAL_ERROR;
    }

    rdma_freeaddrinfo(sres);
    *rdma_addr = dres;

    return OPAL_SUCCESS;
}

/*
 * Invoked by main thread
 */
static int rdmacm_client_connect_one(rdmacm_contents_t *contents,
                                     modex_message_t *message,
                                     int num)
{
    int rc;
    id_context_t *context;
    char src_addr[32], dst_addr[32];
    struct rdma_addrinfo *rdma_addr;

    /* We'll need to access some data in the event handler.  We can
     * encapsulate it in this data struct and attach it to the id being
     * created below.  The event->id will contain this same pointer.
     */
    context = OBJ_NEW(id_context_t);
    if (NULL == context) 
    {
        BTL_ERROR(("malloc error"));
        goto out;
    }

    context->contents = contents;
    OBJ_RETAIN(contents);
    context->qpnum = num;
    context->endpoint = contents->endpoint;

    //create rdma_cm_id
    rc = rdma_create_id(event_channel, &(context->id), context, RDMA_PS_TCP);
    if (0 != rc) 
    {
        BTL_ERROR(("Failed to create a rdma id with %d", rc));
        goto out1;
    }

/*{{{*/

    /* This is odd an worth explaining: when we place the context on
       the ids list, we need to add an extra RETAIN to the context.
       The reason is because of a race condition.  Let's explain
       through a few cases:

       1. Normal termination: client side endpoint_finalize removes
          the context from the ids list, has its service thread call
          rdma_disconnect(), and then RELEASE.  A DISCONNECT event
          will occur on both sides; the client DISCONNECT will invoke
          RELEASE again on the context.  Note that the DISCONNECT
          event may occur *very* quickly on the client side, so the
          order of these two RELEASEs is not known.  The destructor
          will invoke rdma_destroy_id() -- we obviously can't have
          this happen before both actions complete.  Hence,
          refcounting (and the additional RETAIN) saves us.

          Note that the server side never had the context on the ids
          list, so it never had an extra RETAIN.  So the DISCONNECT on
          the server side will only invoke one RELEASE.

       2. Abnormal termination: if the server side terminates
          improperly (e.g., user's app segv's), then the kernel from
          the server side will send a DISCONNECT event to the client
          before the item has been removed from the ids list.  This
          will cause an assertion failure in debug builds (because
          we'll be trying to RELEASE an opal_list_item_t that is still
          on a list), and possibly other badness in optimized builds
          because we'll be traversing a freed opal_list_item_t in
          endpoint_finalize.  So the extra RETAIN here right when we
          put the item on the list prevents it from actually being
          released in the client until BOTH the endpoint_finalize
          occurs *and* the DISCONNECT event arrives.

       Asynchronous programming is fun!
     */
/*}}}*/

    OBJ_RETAIN(context);
    opal_list_append(&(contents->ids), &(context->super));
    if (NULL == inet_ntop(AF_INET6, contents->gid.raw, src_addr, sizeof src_addr))
    {
        BTL_ERROR(("local addr string creating fail"));
        goto out1;
    }

    if (NULL == inet_ntop(AF_INET6, message->gid, dst_addr, sizeof dst_addr))
    {
        BTL_ERROR(("remote addr string creating fail"));
        goto out1;
    }

    rc = get_rdma_addr(src_addr, dst_addr, &rdma_addr, 0);
    if (OPAL_SUCCESS != rc)
    {
        BTL_ERROR(("server: create rdma addr error"));
        goto out1;
    }

    ((struct sockaddr_ib *) (rdma_addr->ai_dst_addr))->sib_sid = message->service_id;
    rc = rdma_resolve_addr(context->id,
                           rdma_addr->ai_src_addr,
                           rdma_addr->ai_dst_addr,
                           rdmacm_resolve_timeout);

    if (0 != rc)
    {
        BTL_ERROR(("Failed to resolve the remote address with %d", rc));
        rdma_freeaddrinfo(rdma_addr);
        goto out1;
    }
    rdma_freeaddrinfo(rdma_addr);

    return OPAL_SUCCESS;

out1:
    OBJ_RELEASE(context);
out:
    return OPAL_ERROR;
}

/*
 * Invoked by main thread
 *
 * Connect method called by the upper layers to connect the local
 * endpoint to the remote endpoint by creating QP(s) to connect the two.
 * Already holding endpoint lock when this function is called.
 *
 * called by handle_connect_request
 *
 */
static int rdmacm_module_start_connect(osdh_connect_base_module_t *cpc,
                                       mca_btl_base_endpoint_t *endpoint)
{
    rdmacm_contents_t *contents;
    modex_message_t *message, *local_message;
    int rc, qp;
    opal_list_item_t *item;

    /* Don't use the CPC to get the message, because this function is
       invoked from the event_handler (to intitiate connections in the
       Right direction), where we don't have the CPC, so it'll be
       NULL. */
    local_message =
        (modex_message_t *) endpoint->endpoint_local_cpc->data.cbm_modex_message;

    message = (modex_message_t *)
        endpoint->endpoint_remote_cpc_data->cbm_modex_message;

    if (MCA_BTL_IB_CONNECTED == endpoint->endpoint_state ||
        MCA_BTL_IB_CONNECTING == endpoint->endpoint_state ||
        MCA_BTL_IB_CONNECT_ACK == endpoint->endpoint_state) {
        return OPAL_SUCCESS;
    }

    /* Set the endpoint state to "connecting" (this function runs in
       the main MPI thread; not the service thread, so we can set the
       endpoint_state here). */
    endpoint->endpoint_state = MCA_BTL_IB_CONNECTING;

    contents = OBJ_NEW(rdmacm_contents_t);
    if (NULL == contents) {
        BTL_ERROR(("malloc of contents failed"));
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto out;
    }

    contents->openib_btl = endpoint->endpoint_btl;
    contents->endpoint = endpoint;
    contents->server = false;
    /* Populate the port information with the local port the server is
     * listening on instead of the ephemerial port this client is
     * connecting with.  This port is used to determine which endpoint
     * is being connected from, in the case where there are multiple
     * listeners on the local system.
     */
    memcpy(contents->gid.raw, local_message->gid, sizeof(contents->gid));
    contents->service_id = local_message->service_id;

    /* Are we the initiator?  Or do we expect this connect request to
       be rejected? */
    endpoint->endpoint_initiator =
        i_initiate(
                   contents->service_id, message->service_id,
                   &contents->gid, (union ibv_gid *) message->gid);
    OPAL_OUTPUT((-1, "MAIN Start connect; ep=%p (%p), I %s the initiator to %s",
                 (void*) endpoint,
                 (void*) endpoint->endpoint_local_cpc,
                 endpoint->endpoint_initiator ? "am" : "am NOT",
                 opal_get_proc_hostname(endpoint->endpoint_proc->proc_opal)));

    /* If we're the initiator, then open all the QPs */
    if (contents->endpoint->endpoint_initiator) {
        /* Initiator needs a CTS frag (non-initiator will have a CTS
           frag allocated later) */
        if (OPAL_SUCCESS !=
            (rc = osdh_connect_base_alloc_cts(contents->endpoint))) {
            BTL_ERROR(("Failed to alloc CTS frag"));
            goto out;
        }

        for (qp = 0; qp < sdh_component.num_qps; qp++) {
            rc = rdmacm_client_connect_one(contents, message, qp);
            if (OPAL_SUCCESS != rc) {
                BTL_ERROR(("rdmacm_client_connect_one error (real QP %d)",
                           qp));
                goto out;
            }
        }
    }
    /* Otherwise, only open 1 QP that we expect to be rejected */
    else {
        rc = rdmacm_client_connect_one(contents, message, 0);
        if (OPAL_SUCCESS != rc) {
            BTL_ERROR(("rdmacm_client_connect_one error (bogus QP)"));
            goto out;
        }
    }

    return OPAL_SUCCESS;

out:
    while (NULL != (item = opal_list_remove_first (&contents->ids))) {
        OBJ_RELEASE(item);
    }

    return rc;
}

#if !BTL_OPENIB_RDMACM_IB_ADDR
static void *show_help_cant_find_endpoint(void *context)
{
    char *msg;
    cant_find_endpoint_context_t *c =
        (cant_find_endpoint_context_t*) context;

    if (NULL != c) {
        msg = stringify(c->peer_ip_addr);
        opal_show_help("help-mpi-btl-openib-cpc-rdmacm.txt",
                       "could not find matching endpoint", true,
                       opal_process_info.nodename,
                       c->device_name,
                       c->peer_tcp_port);
        free(msg);
    } else {
        opal_show_help("help-mpi-btl-openib-cpc-rdmacm.txt",
                       "could not find matching endpoint", true,
                       opal_process_info.nodename,
                       "<unknown>", "<unknown>", -1);
    }
    free(context);

    /* Now kill it */
    sdh_endpoint_invoke_error(NULL);
    return NULL;
}
#endif

/*
 * Invoked by service thread, namely server side. 
 *
 * The server thread will handle the incoming connection requests and
 * accept them or reject them based on a unidirectional connection method.  
 *
 * The choonections are allowed based on the IP address and
 * port values.  This determination is arbitrary, but is uniform in
 * allowing the connections only in 1 direction.  If the connection in
 * the requestion is disallowed by this rule, then the server will
 * reject the connection and make its own in the proper direction.
 */
static int handle_connect_request(struct rdma_cm_event *event)
{
    id_context_t *listener_context = (id_context_t*) event->id->context;
    id_context_t *new_context = NULL;
    rdmacm_contents_t *contents = listener_context->contents;
    sdh_endpoint_t *endpoint;
    struct rdma_conn_param conn_param;
    opal_process_name_t rem_name;
    modex_message_t *message;
    private_data_t msg;
    int rc = -1, qpnum;
    uint32_t rem_index;
    uint64_t rem_port;

    // get rdma_cm_event.private_data from initating rdma_connect side.
    qpnum = ((private_data_t *)event->param.conn.private_data)->qpnum;
    rem_port = ((private_data_t *)event->param.conn.private_data)->rem_port;
    rem_index = ((private_data_t *)event->param.conn.private_data)->rem_index;
    rem_name = ((private_data_t *)event->param.conn.private_data)->rem_name;

    // Determine which endpoint the remote side is trying to connect to; 
    // use the listener's context->contents to figure it out 
    endpoint = rdmacm_find_endpoint(contents, rem_name);
    if (NULL == endpoint) 
    {
        // Cannot find endpoint.
        goto out;
    }

    message = (modex_message_t *) endpoint->endpoint_remote_cpc_data->cbm_modex_message;
    endpoint->endpoint_initiator =
        i_initiate(contents->service_id, rem_port,&contents->gid, (union ibv_gid *) message->gid);

    if (endpoint->endpoint_initiator)
    {
        reject_reason_t reason = REJECT_WRONG_DIRECTION;

        // SERVICE Received a connect request from an endpoint in the wrong direction

        // This will cause a event on the remote system.  By passing in
        // a value in the second arg of rdma_reject, the remote side
        // can check for this to know if it was an intentional reject or
        // a reject based on an error.
        rc = rdma_reject(event->id, &reason, sizeof(reject_reason_t));
        if (0 != rc) 
        {
            // rdma_reject failed
            goto out;
        }

        // SERVICE Starting connection in other direction
        rdmacm_module_start_connect(NULL, endpoint);

        return OPAL_SUCCESS;
    }

    // Set the endpoint_state to "CONNECTING".  
    // This is running in the service thread, so we need to do a write barrier. 
    endpoint->endpoint_state = MCA_BTL_IB_CONNECTING;
    opal_atomic_wmb();

    endpoint->rem_info.rem_index = rem_index;

    // Setup QP for new connection 
    rc = rdmacm_setup_qp(contents, endpoint, event->id, qpnum);
    if (0 != rc)
    {
        BTL_ERROR(("rdmacm_setup_qp error %d", rc));
        goto out;
    }

    // Post a single receive buffer on the smallest QP for the CTS protocol 
    if (sdh_component.credits_qp == qpnum)
    {
        struct ibv_recv_wr *bad_wr, *wr;

        if (OPAL_SUCCESS !=
            osdh_connect_base_alloc_cts(endpoint)) {
            BTL_ERROR(("Failed to alloc CTS frag"));
            goto out1;
        }
        wr = &(endpoint->endpoint_cts_frag.rd_desc);
        assert(NULL != wr);
        wr->next = NULL;

        if (0 != ibv_post_recv(endpoint->qps[qpnum].qp->lcl_qp,
                               wr, &bad_wr)) {
            BTL_ERROR(("failed to post CTS recv buffer"));
            goto out1;
        }
    }

    // Since the event id is already created (since we're the server),
    // the context that was passed to us was the listen server's
    // context -- which is no longer useful to us.  So allocate a new
    // context and populate it just for this connection. 
    event->id->context = new_context = OBJ_NEW(id_context_t);
    if (NULL == new_context)
    {
        BTL_ERROR(("malloc error"));
        goto out1;
    }

    new_context->contents = contents;
    OBJ_RETAIN(contents);
    new_context->qpnum = qpnum;
    new_context->endpoint = endpoint;

    memset(&conn_param, 0, sizeof(conn_param));
    /* See rdma_connect(3) for a description of these 2 values.  We
       ensure to pass these values around via the modex so that we can
       compute the values properly. */
    conn_param.responder_resources =
        mymin(contents->openib_btl->device->ib_dev_attr.max_qp_rd_atom, message->device_max_qp_init_rd_atom);

    conn_param.initiator_depth =
        mymin(contents->openib_btl->device->ib_dev_attr.max_qp_init_rd_atom, message->device_max_qp_rd_atom);

    conn_param.retry_count = sdh_component.ib_retry_count;
    conn_param.rnr_retry_count = BTL_OPENIB_QP_TYPE_PP(qpnum) ? 0 : sdh_component.ib_rnr_retry;
    conn_param.srq = BTL_OPENIB_QP_TYPE_SRQ(qpnum);
    conn_param.private_data = &msg;
    conn_param.private_data_len = sizeof(private_data_t);

    // Fill the private data being sent to the other side 
    msg.qpnum = qpnum;
    msg.rem_index = endpoint->index;
    msg.rem_name = OPAL_PROC_MY_NAME;

    // Accepting the connection will result in a RDMA_CM_EVENT_ESTABLISHED event 
    // on both the client and server side. 
    rc = rdma_accept(event->id, &conn_param);
    if (0 != rc) 
    {
        BTL_ERROR(("rdma_accept error %d", rc));
        goto out2;
    }

    return OPAL_SUCCESS;

out2:
    OBJ_RELEASE(new_context);
out1:
    ibv_destroy_qp(endpoint->qps[qpnum].qp->lcl_qp);
out:
    return OPAL_ERROR;
}

/*
 * Runs in service thread
 *
 * We call rdma_disconnect() here in the service thread so that there
 * is zero chance that the DISCONNECT event is delivered and executed
 * in the service thread while rdma_disconnect() is still running in
 * the main thread (which causes all manner of Bad Things to occur).
 */
static void *call_disconnect_callback(int fd, int flags, void *v)
{
    rdmacm_contents_t *contents = (rdmacm_contents_t *) v;
    id_context_t *context;
    opal_list_item_t *item;

    pthread_mutex_lock (&rdmacm_disconnect_lock);
    while (NULL != (item = opal_list_remove_first(&contents->ids))) 
    {
        context = (id_context_t *) item;
        if (!context->already_disconnected) 
        {
            // disconnect
            rdma_disconnect(context->id);
            context->already_disconnected = true;
        }
        OBJ_RELEASE(context);
    }

    // Tell the main thread that we're done 
    pthread_cond_signal(&rdmacm_disconnect_cond);
    pthread_mutex_unlock(&rdmacm_disconnect_lock);

    return NULL;
}

/*
 * Invoked by main thread
 *
 * Runs *while* the progress thread is running.  We can't stop the
 * progress thread because this function may be invoked to kill a
 * specific endpoint that was the result of MPI-2 dynamics (i.e., this
 * is not during MPI_FINALIZE).
 */
static int rdmacm_endpoint_finalize(struct mca_btl_base_endpoint_t *endpoint)
{
    rdmacm_contents_t *contents = NULL, *item;
    opal_event_t event;

    // Start disconnecting...

    if (NULL == endpoint)
    {
        // Attempting to shutdown a NULL endpoint
        return OPAL_SUCCESS;
    }

    /* Determine which rdmacm_contents_t correlates to the endpoint
     * we are shutting down.  By disconnecting instead of simply
     * destroying the QPs, we are shutting down in a more graceful way
     * thus preventing errors on the line.
     *
     * Need to lock because the client_list is accessed in both the
     * main thread and service thread.
     */
    opal_mutex_lock(&client_list_lock);
    OPAL_LIST_FOREACH(item, &client_list, rdmacm_contents_t) 
    {
        if (endpoint == item->endpoint) 
        {
            contents = item;
            opal_list_remove_item(&client_list, (opal_list_item_t *) contents);
            contents->on_client_list = false;

            /* Fun race condition: we cannot call
               rdma_disconnect() in this thread, because
               if we do, there is a nonzero chance that the
               DISCONNECT event will be delivered and get executed
               in the rdcm event thread immediately.  If this all
               happens before rdma_disconnect() returns, all
               manner of Bad Things can/will occur.  So just
               invoke rdma_disconnect() in the rdmacm event thread
               where we guarantee that we won't be processing an
               event when it is called. */

            opal_event_set (rdmacm_event_base, &event, -1, OPAL_EV_READ,
                            call_disconnect_callback, contents);
            opal_event_active (&event, OPAL_EV_READ, 1);

            /* remove_item returns the item before the item removed,
               meaning that the for list is still safe */
            break;
        }
    }

    /* Flush writes to ensure we sync across threads */
    opal_atomic_wmb();
    opal_mutex_unlock(&client_list_lock);

    if (NULL != contents) 
    {
        /* Now wait for all the disconnect callbacks to occur */
        pthread_mutex_lock(&rdmacm_disconnect_lock);
        while (opal_list_get_size (&contents->ids)) 
        {
            pthread_cond_wait (&rdmacm_disconnect_cond, &rdmacm_disconnect_lock);
        }
        pthread_mutex_unlock(&rdmacm_disconnect_lock);
    }

    OPAL_OUTPUT((-1, "MAIN Endpoint finished finalizing"));
    return OPAL_SUCCESS;
}

/*
 * Callback (from main thread) when the endpoint has been connected
 */
static void *local_endpoint_cpc_complete(void *context)
{
    sdh_endpoint_t *endpoint = (sdh_endpoint_t *)context;
    OPAL_THREAD_LOCK(&endpoint->endpoint_lock);
    sdh_endpoint_cpc_complete(endpoint);

    return NULL;
}

/*
 * Runs in service thread
 *
 */
static int rdmacm_connect_endpoint(id_context_t *context, struct rdma_cm_event *event)
{
    rdmacm_contents_t *contents = context->contents;
    rdmacm_endpoint_local_cpc_data_t *data;

    sdh_endpoint_t *endpoint;

    if (contents->server) 
    {
        endpoint = context->endpoint;
    } 
    else 
    {
        endpoint = contents->endpoint;
        endpoint->rem_info.rem_index = ((private_data_t *)event->param.conn.private_data)->rem_index;

        if (!contents->on_client_list) 
        {
            opal_mutex_lock(&client_list_lock);
            opal_list_append(&client_list, &(contents->super));
            // Flush writes to ensure we sync across threads 
            opal_atomic_wmb();
            opal_mutex_unlock(&client_list_lock);
            contents->on_client_list = true;
        }
    }

    if (NULL == endpoint)
    {
        // Can't find endpoint
        return OPAL_ERR_NOT_FOUND;
    }

    data = (rdmacm_endpoint_local_cpc_data_t *)endpoint->endpoint_local_cpc_data;

    // Only notify the upper layers after the last QP has been connected 
    if (++data->rdmacm_counter < sdh_component.num_qps)
    {
        return OPAL_SUCCESS;
    }

    // Ensure that all the writes back to the endpoint and associated data structures have completed 
    opal_atomic_wmb();
    sdh_run_in_main (local_endpoint_cpc_complete, endpoint);

    return OPAL_SUCCESS;
}

/*
 * Runs in service thread
 */
static int rdmacm_disconnected(id_context_t *context)
{
    // If this was a client thread, then it *may* still be listed in a
    // contents->ids list. 

    OBJ_RELEASE(context);

    return OPAL_SUCCESS;
}

/*
 * Runs in service thread
 */
static int rdmacm_destroy_dummy_qp(id_context_t *context)
{
    // We need to check id pointer because of retransmitions.
    // Maybe the reject was already done. 

    if (NULL != context->id) 
    {
        if (NULL != context->id->qp) 
        {
            ibv_destroy_qp(context->id->qp);
            context->id->qp = NULL;
        }
    }

    if (NULL != context->contents->dummy_cq)
    {
        ibv_destroy_cq(context->contents->dummy_cq);
    }
    // This item was appended to the contents->ids list (the list will
    // only have just this one item), so remove it before RELEASEing the item 
    opal_list_remove_first(&(context->contents->ids));
    OBJ_RELEASE(context);

    return OPAL_SUCCESS;
}

/*
 * Runs in service thread
 */
static int rdmacm_rejected(id_context_t *context, struct rdma_cm_event *event)
{
    if (NULL != event->param.conn.private_data) {
        // Why were we rejected? 
        switch (*((reject_reason_t*) event->param.conn.private_data)) 
        {
            case REJECT_WRONG_DIRECTION:
                rdmacm_destroy_dummy_qp(context);
                break;

            default:
                // Just so compilers won't complain 
                break;
        }
    }
    return OPAL_SUCCESS;
}

/*
 * Runs in service thread
 */
static int resolve_route(id_context_t *context)
{
    int rc;
    // Resolve the route to the remote system.  Once established, the
    // local system will get a RDMA_CM_EVENT_ROUTE_RESOLVED event.
    // timeout is const value
    rc = rdma_resolve_route(context->id, rdmacm_resolve_timeout);
    if (0 != rc) 
    {
        //Failed to resolve the route 
        goto out;
    }
    return OPAL_SUCCESS;
out:
    return OPAL_ERROR;
}

/*
 * Runs in service thread
 */
static int create_dummy_cq(rdmacm_contents_t *contents, sdh_module_t *openib_btl)
{
    contents->dummy_cq = ibv_create_cq(openib_btl->device->ib_dev_context, 1, NULL, NULL, 0);
    if (NULL == contents->dummy_cq) 
    {
        // dummy_cq not created
        goto out;
    }
    return OPAL_SUCCESS;
out:
    return OPAL_ERROR;
}

/*
 * Runs in service thread
 */
static int create_dummy_qp(rdmacm_contents_t *contents, struct rdma_cm_id *id, int qpnum)
{
    struct ibv_qp_init_attr attr;

    memset(&attr, 0, sizeof(attr));
    attr.qp_type = IBV_QPT_RC;
    attr.send_cq = contents->dummy_cq;
    attr.recv_cq = contents->dummy_cq;
    attr.cap.max_recv_wr = 1;
    attr.cap.max_send_wr = 1;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;

    {
        /* JMS Temprary gross hack: we *must* use rdma_create_cp()
           (vs. ibv_create_qp()) because strange things happen on IB
           if we don't.  However, rdma_create_cp() wants us to use
           rdma_get_devices() (and therefore the pd that they have
           allocated).  In order to get v1.3 out the door, we're
           bypassing this functionality - we're temporarily overriding
           the device context cached on the ID with our own, so that
           our pd will match.  We need to fix this to properly get the
           pd from the RDMA CM and use that, etc. */
        struct ibv_context *temp = id->verbs;
        id->verbs = contents->openib_btl->device->ib_pd->context;
        if (0 != rdma_create_qp(id, contents->openib_btl->device->ib_pd, &attr)) 
        {
            BTL_ERROR(("Failed to create qp with %d", qpnum));
            goto out;
        }
        id->verbs = temp;
    }
    BTL_VERBOSE(("dummy qp created %d", qpnum));
    return OPAL_SUCCESS;
out:
    return OPAL_ERROR;
}

/*
 * Runs in service thread
 */
static int finish_connect(id_context_t *context)
{
    rdmacm_contents_t *contents = context->contents;
    struct rdma_conn_param conn_param;
    private_data_t msg;
    int rc;
    modex_message_t *message;

    message = (modex_message_t *)context->endpoint->endpoint_remote_cpc_data->cbm_modex_message;

    // If we're the initiator, then setup the QP's and post the CTS message buffer 
    if (contents->endpoint->endpoint_initiator) 
    {
        rc = rdmacm_setup_qp(contents, contents->endpoint, context->id, context->qpnum);
        if (0 != rc) 
        {
            BTL_ERROR(("rdmacm_setup_qp error %d", rc));
            goto out;
        }

        if (sdh_component.credits_qp == context->qpnum) 
        {
        // Post a single receive buffer on the smallest QP for the CTS protocol 
            struct ibv_recv_wr *bad_wr, *wr;
            assert(NULL != contents->endpoint->endpoint_cts_frag.super.super.base.super.ptr);
            wr = &(contents->endpoint->endpoint_cts_frag.rd_desc);
            assert(NULL != wr);
            wr->next = NULL;
            if (0 != ibv_post_recv(contents->endpoint->qps[context->qpnum].qp->lcl_qp, wr, &bad_wr)) 
            {
                BTL_ERROR(("failed to post CTS recv buffer"));
                goto out1;
            }
        }
    } 
    else 
    {
        // If we are establishing a connection in the "wrong" direction,
        // setup a dummy CQ and QP and do NOT post any recvs on them.
        // Otherwise this will screwup the recv accounting and will
        // result in not posting recvs when you really really wanted to.
        // All of the dummy cq and qps will be cleaned up on the reject event.
        rc = create_dummy_cq(contents, contents->openib_btl);
        if (0 != rc) 
        {
            BTL_ERROR(("create_dummy_cq error %d", rc));
            goto out;
        }

        rc = create_dummy_qp(contents, context->id, context->qpnum);
        if (0 != rc) 
        {
            BTL_ERROR(("create_dummy_qp error %d", rc));
            goto out;
        }
    }

    memset(&conn_param, 0, sizeof(conn_param));
    // See above comment about rdma_connect(3) and these two values. 
    conn_param.responder_resources =
        mymin(contents->openib_btl->device->ib_dev_attr.max_qp_rd_atom, message->device_max_qp_init_rd_atom);

    conn_param.initiator_depth =
        mymin(contents->openib_btl->device->ib_dev_attr.max_qp_init_rd_atom, message->device_max_qp_rd_atom);

    conn_param.flow_control = 0;
    conn_param.retry_count = sdh_component.ib_retry_count;
    conn_param.rnr_retry_count = BTL_OPENIB_QP_TYPE_PP(context->qpnum) ? 0 : sdh_component.ib_rnr_retry;
    conn_param.srq = BTL_OPENIB_QP_TYPE_SRQ(context->qpnum);
    conn_param.private_data = &msg;
    conn_param.private_data_len = sizeof(private_data_t);

    msg.qpnum = context->qpnum;
    msg.rem_index = contents->endpoint->index;
    msg.rem_name = OPAL_PROC_MY_NAME;
    memset(msg.librdmacm_header, 0, sizeof(msg.librdmacm_header));
    msg.rem_port = contents->service_id;

    // Now all of the local setup has been done.  
    // The remote system should now get a RDMA_CM_EVENT_CONNECT_REQUEST event 
    // to further the setup of the QP. 
    rc = rdma_connect(context->id, &conn_param);
    if (0 != rc) 
    {
        BTL_ERROR(("rdma_connect Failed with %d", rc));
        goto out1;
    }

    return OPAL_SUCCESS;

out1:
    ibv_destroy_qp(context->id->qp);
out:
    OBJ_RELEASE(contents);

    return OPAL_ERROR;
}

/*
 * Runs in main thread
 */
static void *show_help_rdmacm_event_error (struct rdma_cm_event *event)
{
    id_context_t *context = (id_context_t*) event->id->context;

    if (RDMA_CM_EVENT_DEVICE_REMOVAL == event->event) {
        opal_show_help("help-mpi-btl-openib-cpc-rdmacm.txt",
                       "rdma cm device removal", true,
                       opal_process_info.nodename,
                       ibv_get_device_name(event->id->verbs->device));
    } else {
        const char *device = "Unknown";
        if (NULL != event->id &&
            NULL != event->id->verbs &&
            NULL != event->id->verbs->device) {
            device = ibv_get_device_name(event->id->verbs->device);
        }
        opal_show_help("help-mpi-btl-openib-cpc-rdmacm.txt",
                       "rdma cm event error", true,
                       opal_process_info.nodename,
                       device,
                       rdma_event_str(event->event),
                       opal_get_proc_hostname(context->endpoint->endpoint_proc->proc_opal));
    }

    return NULL;
}

/*
 * Runs in service thread
 */
static int event_handler(struct rdma_cm_event *event)
{
    id_context_t *context = (id_context_t*) event->id->context;
    int rc = -1;
    osdh_ini_values_t ini;
    bool found;

    if (NULL == context) {
        return rc;
    }

    switch (event->event) 
    {
        // client side
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            rc = resolve_route(context);
            break;

        // client side 
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
            rc = finish_connect(context);
            break;

        // server side
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            rc = handle_connect_request(event);
            break;

        // client/server side
        case RDMA_CM_EVENT_ESTABLISHED:
            rc = rdmacm_connect_endpoint(context, event);
            break;

        case RDMA_CM_EVENT_DISCONNECTED:
            rc = rdmacm_disconnected(context);
            break;

        case RDMA_CM_EVENT_REJECTED:
            rc = rdmacm_rejected(context, event);
            break;

        case RDMA_CM_EVENT_CONNECT_ERROR:
        /* Some adapters have broken REJECT behavior; the recipient
           gets a CONNECT_ERROR event instead of the expected REJECTED
           event.  So if we get a CONNECT_ERROR, see if it's on a
           connection that we're expecting a REJECT (i.e., we have a
           dummy_cq setup).  If it is, and if a) the MCA param
           btl_openib_connect_rdmacm_reject_causes_connect_error is
           true, or b) if rdmacm_reject_causes_connect_error set on
           the device INI values, then just treat this CONNECT_ERROR
           as if it were the REJECT. */
            if (NULL != context->contents->dummy_cq) 
            {
                struct ibv_device_attr *attr =
                    &(context->endpoint->endpoint_btl->device->ib_dev_attr);
                found = false;
                if (OPAL_SUCCESS == osdh_ini_query(attr->vendor_id,
                                                   attr->vendor_part_id,
                                                   &ini) &&
                                                   ini.rdmacm_reject_causes_connect_error) 
                {
                    found = true;
                }

                if (rdmacm_reject_causes_connect_error) 
                {
                    found = true;
                }

                if (found) 
                {
                    rc = rdmacm_destroy_dummy_qp(context);
                    break;
                }
            }

        //Otherwise, fall through and handle the error as normal 

        case RDMA_CM_EVENT_UNREACHABLE:
        case RDMA_CM_EVENT_CONNECT_RESPONSE:
        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_DEVICE_REMOVAL:
            show_help_rdmacm_event_error (event);
            rc = OPAL_ERROR;
            break;

        case RDMA_CM_EVENT_ROUTE_ERROR:
        // Route lookup does not necessarily handle retries, and there
        // appear to be cases where the subnet manager node can no
        // longer handle incoming requests.  The rdma connection
        // manager and lower level code doesn't handle retries, so we have to. 
            if (context->route_retry_count < rdmacm_resolve_max_retry_count) 
            {
                context->route_retry_count++;
                rc = resolve_route(context);
                break;
            }
            show_help_rdmacm_event_error (event);
            rc = OPAL_ERROR;
            break;

        default:
            // Unknown error 
            rc = OPAL_ERROR;
            break;
    }
    return rc;
}

/*
 * Runs in event thread
 */
static inline void rdmamcm_event_error(struct rdma_cm_event *event)
{
    mca_btl_base_endpoint_t *endpoint = NULL;

    if (event->id->context) 
    {
        endpoint = ((id_context_t *)event->id->context)->contents->endpoint;
    }

    sdh_run_in_main (sdh_endpoint_invoke_error, endpoint);
}

/*
 * Runs in event thread
 */
static void *rdmacm_event_dispatch(int fd, int flags, void *context)
{
    struct rdma_cm_event *event, ecopy;
    void *data = NULL;
    int rc;

    // blocks until next cm_event 
    rc = rdma_get_cm_event(event_channel, &event);
    if (0 != rc) 
    {
        BTL_ERROR(("rdma_get_cm_event error %d", rc));
        return NULL;
    }

    /* If the incoming event is not acked in a sufficient amount of
     * time, there will be a timeout error and the connection will be
     * torndown.  Also, the act of acking the event destroys the
     * included data in the event.  In certain circumstances, the time
     * it takes to handle a incoming event could approach or exceed
     * this time.  To prevent this from happening, we will copy the
     * event and all of its data, ack the event, and process the copy
     * of the event.
     */
    memcpy(&ecopy, event, sizeof(struct rdma_cm_event));
    if (event->param.conn.private_data_len > 0) 
    {
        data = malloc(event->param.conn.private_data_len);
        if (NULL == data) 
        {
           BTL_ERROR(("error mallocing memory"));
           return NULL;
        }
        memcpy(data, event->param.conn.private_data, event->param.conn.private_data_len);
        ecopy.param.conn.private_data = data;
    }
    rdma_ack_cm_event(event);

    // handing request
    rc = event_handler(&ecopy);

    if (OPAL_SUCCESS != rc) 
    {
        rdmamcm_event_error(&ecopy);
    }

    if (NULL != data) 
    {
        free(data);
    }

    return NULL;
}

/*
 * Runs in main thread
 *
 * CPC init function - Setup all globals here
 */
static int rdmacm_init(sdh_endpoint_t *endpoint)
{
    void *data;

    data = calloc(1, sizeof(rdmacm_endpoint_local_cpc_data_t));
    if (NULL == data) 
    {
        BTL_ERROR(("malloc failed"));
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    endpoint->endpoint_local_cpc_data = data;

    return OPAL_SUCCESS;
}

static int create_message(rdmacm_contents_t *server,
                          sdh_module_t *openib_btl,
                          osdh_connect_base_module_data_t *data)
{
    modex_message_t *message;

    message = (modex_message_t *) malloc(sizeof(modex_message_t));
    if (NULL == message) 
    {
        BTL_ERROR(("malloc failed"));
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    message->device_max_qp_rd_atom = openib_btl->device->ib_dev_attr.max_qp_rd_atom;
    message->device_max_qp_init_rd_atom = openib_btl->device->ib_dev_attr.max_qp_init_rd_atom;

    memcpy(message->gid, server->gid.raw, sizeof(server->gid));
    message->service_id = server->service_id;
    data->cbm_modex_message = message;
    data->cbm_modex_message_len = message_len;

    return OPAL_SUCCESS;
}

// ========================= rdmacm_component_query ======================

/*
 * Runs in main thread
 *
 * This function determines if the RDMACM is a possible cpc method and
 * sets it up accordingly.
 * n
 */
static int rdmacm_component_query(sdh_module_t *openib_btl, osdh_connect_base_module_t **cpc)
{
    int rc;

    id_context_t *context;
    rdmacm_contents_t *server = NULL;

    char rdmacm_addr_str[32];
    struct rdma_addrinfo *rdma_addr;

    // RDMACM is not supported for MPI_THREAD_MULTIPLE 
    if (opal_using_threads()) 
    {
        rc = OPAL_ERR_NOT_SUPPORTED;
        goto out;
    }

    // RDMACM is not supported if we have any XRC QPs 
    if (sdh_component.num_xrc_qps > 0) 
    {
        rc = OPAL_ERR_NOT_SUPPORTED;
        goto out;
    }

    if (!BTL_OPENIB_QP_TYPE_PP(0))
    {
        rc = OPAL_ERR_NOT_SUPPORTED;
        goto out;
    }

    BTL_VERBOSE(("rdmacm_component_query"));

    // osdh_connect_base_module_t  !!!!!
    *cpc = (osdh_connect_base_module_t *) malloc(sizeof(osdh_connect_base_module_t));
    if (NULL == *cpc)
    {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto out;
    }
    
    // osdh_connect_base_module_t --> cpc
    // osdh_connect_base_module_data_t --> cpc.data
    // osdh_conect_base_component_t --> cpc.data.cbm_component

    // init osdh_connect_base_module_data_t
    (*cpc)->data.cbm_component = &osdh_connect_rdmacm;
    (*cpc)->data.cbm_priority = rdmacm_priority;
    (*cpc)->data.cbm_modex_message     = NULL;
    (*cpc)->data.cbm_modex_message_len = 0;
    // init other term : function pointer.
    (*cpc)->cbm_endpoint_init = rdmacm_init;
    (*cpc)->cbm_start_connect = rdmacm_module_start_connect;
    (*cpc)->cbm_endpoint_finalize = rdmacm_endpoint_finalize;
    (*cpc)->cbm_finalize = NULL;
    // Setting uses_cts = true also guarantees that we'll only be
    // selected if QP 0 is PP 
    (*cpc)->cbm_uses_cts = true;

    // Start monitoring the fd associated with the cm_device 
    server = OBJ_NEW(rdmacm_contents_t);
    if (NULL == server) 
    {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto out1;
    }
    server->server = true;
    server->openib_btl = openib_btl;

    context = OBJ_NEW(id_context_t);
    if (NULL == context) 
    {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto out3;
    }
    context->contents = server;
    OBJ_RETAIN(context->contents);
    opal_list_append(&(server->ids), &(context->super));
    context->qpnum = 0;

    rc = rdma_create_id(event_channel, &(context->id), context, RDMA_PS_TCP);
    if (0 != rc) 
    {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto out4;
    }

    rc = ibv_query_gid(openib_btl->device->ib_pd->context, 
                       openib_btl->port_num,
                       sdh_component.gid_index, 
                       &server->gid);
    if (0 != rc) 
    {
        BTL_ERROR(("local gid query failed"));
        goto out4;
    }

    if (NULL == inet_ntop(AF_INET6, 
                         server->gid.raw,
                         rdmacm_addr_str, 
                         sizeof rdmacm_addr_str)) 
    {
        BTL_ERROR(("local gaddr string creating fail"));
        goto out4;
    }

    rc = get_rdma_addr(rdmacm_addr_str, NULL, &rdma_addr, 1);
    if (OPAL_SUCCESS != rc) 
    {
        BTL_ERROR(("server: create rdma addr error"));
        goto out4;
    }

    // Bind the rdmacm server to the local IP address and an ephemerial
    // port or one specified by a comand arg.
    rc = rdma_bind_addr(context->id, rdma_addr->ai_src_addr);
    if (0 != rc) 
    {
        rc = OPAL_ERR_UNREACH;
        rdma_freeaddrinfo(rdma_addr);
        goto out5;
    }

    server->service_id = ((struct sockaddr_ib *) (&context->id->route.addr.src_addr))->sib_sid;

    rdma_freeaddrinfo(rdma_addr);
    // Listen on the specified address/port with the rdmacm, limit the
    // amount of incoming connections to 1024 
    // FIXME - 1024 should be (num of connectors sdh_component.num_qps) 
    rc = rdma_listen(context->id, 1024);
    if (0 != rc) 
    {
        rc = OPAL_ERR_UNREACH;
        goto out5;
    }

    rc = create_message(server, openib_btl, &(*cpc)->data);
    if (0 != rc) 
    {
        rc = OPAL_ERR_OUT_OF_RESOURCE;
        goto out5;
    }

    opal_list_append(&server_listener_list, &(server->super));

    return OPAL_SUCCESS;

out5:
    /*
     * Since rdma_create_id() succeeded, we need "rdma_destroy_id(context->id)".
     * But don't do it here since it's part of out4:OBJ_RELEASE(context),
     * and we don't want to do it twice.
     */
out4:
    opal_list_remove_first(&(server->ids));
    OBJ_RELEASE(context);
out3:
    OBJ_RELEASE(server);
out1:
    free(*cpc);
out:
    if (OPAL_ERR_NOT_SUPPORTED == rc) 
    {}
    else 
    {}
    return rc;
}

// ====================== rdmacm_component_finalize ================================

/*
 * Invoked by main thread
 *
 * Shutting down the whole thing.
 */
static int rdmacm_component_finalize(void)
{
    opal_list_item_t *item, *item2;

    // If we're just trolling through ompi_info, don't bother doing anything 
    if (!rdmacm_component_initialized) {
        return OPAL_SUCCESS;
    }

    if (rdmacm_event_base) 
    {
        opal_event_del (&rdmacm_event);
        opal_progress_thread_finalize (NULL);
        rdmacm_event_base = NULL;
    }

    // The event thread is no longer running; no need to lock access
    //   to the client_list 
    OPAL_LIST_DESTRUCT(&client_list);

    // For each of the items in the server list, there's only one item
    // in the "ids" list -- the server listener.  So explicitly
    // destroy its RDMA ID context. 
    while (NULL != (item = opal_list_remove_first(&server_listener_list))) 
    {
        rdmacm_contents_t *contents = (rdmacm_contents_t*) item;
        item2 = opal_list_remove_first(&(contents->ids));
        OBJ_RELEASE(item2);
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&server_listener_list);

    // Now we're all done -- destroy the event channel 
    if (NULL != event_channel) 
    {
        rdma_destroy_event_channel(event_channel);
        event_channel = NULL;
    }

    sdh_free_rdma_addr_list();

    pthread_cond_destroy (&rdmacm_disconnect_cond);
    pthread_mutex_destroy (&rdmacm_disconnect_lock);

    return OPAL_SUCCESS;
}

// ======================================== rdmacm_component_init ====================

static int rdmacm_check_ibaddr_support(void)
{
    int rsock;
    rsock = rsocket(AF_IB, SOCK_STREAM, 0);
    if (rsock < 0) 
    {
        return OPAL_ERROR;
    }

    rclose(rsock);

    return OPAL_SUCCESS;
}

static int rdmacm_component_init(void)
{
    int rc;

    OBJ_CONSTRUCT(&server_listener_list, opal_list_t);
    OBJ_CONSTRUCT(&client_list, opal_list_t);
    OBJ_CONSTRUCT(&client_list_lock, opal_mutex_t);

    rc = rdmacm_check_ibaddr_support();
    if (OPAL_SUCCESS != rc) 
    {
        return OPAL_ERR_NOT_SUPPORTED;
    }

    event_channel = rdma_create_event_channel();
    if (NULL == event_channel) 
    {
        return OPAL_ERR_UNREACH;
    }

    rdmacm_event_base = opal_progress_thread_init (NULL);
    if (NULL == rdmacm_event_base) 
    {
        //openib BTL: could not create rdmacm event thread
        return OPAL_ERR_UNREACH;
    }

    opal_event_set (rdmacm_event_base, 
                    &rdmacm_event, 
                    event_channel->fd,
                    OPAL_EV_READ | OPAL_EV_PERSIST,  
                    rdmacm_event_dispatch, 
                    NULL);

    opal_event_add (&rdmacm_event, 0);

    pthread_cond_init (&rdmacm_disconnect_cond, NULL);
    pthread_mutex_init (&rdmacm_disconnect_lock, NULL);

    rdmacm_component_initialized = true;

    return OPAL_SUCCESS;
}
