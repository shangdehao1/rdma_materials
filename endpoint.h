#ifndef MCA_BTL_IB_ENDPOINT_H
#define MCA_BTL_IB_ENDPOINT_H

#include <errno.h>
#include <string.h>
#include "opal/class/opal_list.h"
#include "opal/mca/event/event.h"
#include "opal/util/output.h"
#include "opal/mca/btl/btl.h"
#include "opal/mca/btl/base/btl_base_error.h"
#include "btl_openib.h"
#include "btl_openib_frag.h"
#include "btl_openib_eager_rdma.h"
#include "connect/base.h"

#define QP_TX_BATCH_COUNT 64
#define QP_TX_BATCH_COUNT 64

struct sdh_frag_t;
struct sdh_proc_modex_t;

// ========================== endpoint_state_t =======================
//
typedef enum 
{
    /* Defines the state in which this BTL instance
     * has started the process of connection */
    MCA_BTL_IB_CONNECTING,

    // Waiting for ack from endpoint 
    MCA_BTL_IB_CONNECT_ACK,

    //Waiting for final connection ACK from endpoint
    MCA_BTL_IB_WAITING_ACK,

    // Connected ... both sender & receiver have
    // buffers associated with this connection 
    MCA_BTL_IB_CONNECTED,

    // Connection is closed, there are no resources
    // associated with this 
    MCA_BTL_IB_CLOSED,

    // Maximum number of retries have been used.
    // Report failure on send to upper layer 
    MCA_BTL_IB_FAILED
} sdh_endpoint_state_t;

// ===================== remote qp information =============================
//
typedef struct sdh_rem_qp_info_t 
{
    uint32_t rem_qp_num;
    // Remote QP number 
    uint32_t rem_psn;
    // Remote processes port sequence number 
} sdh_rem_qp_info_t;

// ====================== remote srq num ================================
typedef struct sdh_rem_srq_info_t 
{
    // Remote SRQ number 
    uint32_t rem_srq_num;
} sdh_rem_srq_info_t;

// ======================= remote information ========================
typedef struct sdh_rem_info_t 
{
    // Local identifier of the remote process 
    uint16_t rem_lid;
    // subnet id of remote process 
    uint64_t rem_subnet_id;
    // MTU of remote process 
    uint32_t rem_mtu;
    // index of remote endpoint in endpoint array 
    uint32_t rem_index;
    // Remote QPs 
    sdh_rem_qp_info_t *rem_qps;
    // Remote xrc_srq info, used only with XRC connections 
    sdh_rem_srq_info_t *rem_srqs;
    // Vendor id of remote HCA 
    uint32_t rem_vendor_id;
    // Vendor part id of remote HCA 
    uint32_t rem_vendor_part_id;
    // Transport type of remote port 
    sdh_transport_type_t rem_transport_type;
} sdh_rem_info_t;


// ====================== peer qp information ============
/*
 *  Agggregates all per peer qp info for an endpoint
 */
typedef struct sdh_endpoint_pp_qp_t 
{
    int32_t sd_credits;  /** this rank's view of the credits
                          *  available for sending:
                          *  this is the credits granted by the
                          *  remote peer which has some relation to the
                          *  number of receive buffers posted remotely
                          */
    int32_t  rd_posted;   /**< number of descriptors posted to the nic*/
    int32_t  rd_credits;  /**< number of credits to return to peer */
    int32_t  cm_received; /**< Credit messages received */
    int32_t  cm_return;   /**< how may credits to return */
    int32_t  cm_sent;     /**< Outstanding number of credit messages */
} sdh_endpoint_pp_qp_t;


// ======================= srq qp information ============
/**
 *  Aggregates all srq qp info for an endpoint
 */
typedef struct sdh_endpoint_srq_qp_t 
{
    int32_t dummy;
} sdh_endpoint_srq_qp_t;

typedef struct sdh_qp_t 
{
    struct ibv_qp *lcl_qp;
    uint32_t lcl_psn;
    volatile int32_t  sd_wqe;      /**< number of available send wqe entries */
    int32_t  sd_wqe_inflight;
    int wqe_count;
    int users;
    opal_mutex_t lock;
} sdh_qp_t;

typedef struct sdh_endpoint_qp_t 
{
    sdh_qp_t *qp;
    opal_list_t no_credits_pending_frags[2]; /**< put fragment here if there is no credits
                                     available */
    opal_list_t no_wqe_pending_frags[2]; /**< put fragments here if there is no wqe
                                    available  */
    int32_t  rd_credit_send_lock;  /**< Lock credit send fragment */
    sdh_send_control_frag_t *credit_frag;
    size_t ib_inline_max;          /**< max size of inline send*/
    union {
        sdh_endpoint_srq_qp_t srq_qp;
        sdh_endpoint_pp_qp_t pp_qp;
    } u;
} sdh_endpoint_qp_t;

/**
 * An abstraction that represents a connection to a endpoint process.
 * An instance of mca_btl_base_endpoint_t is associated w/ each process
 * and BTL pair at startup. However, connections to the endpoint
 * are established dynamically on an as-needed basis:
 */

struct mca_btl_base_endpoint_t 
{
    opal_list_item_t            super;

    /** BTL module that created this connection */
    struct sdh_module_t* endpoint_btl;

    /** proc structure corresponding to endpoint */
    struct sdh_proc_t*   endpoint_proc;

    /** local CPC to connect to this endpoint */
    osdh_connect_base_module_t *endpoint_local_cpc;

    /** hook for local CPC to hang endpoint-specific data */
    void *endpoint_local_cpc_data;

    /** If endpoint_local_cpc->cbm_uses_cts is true and this endpoint
        is iWARP, then endpoint_initiator must be true on the side
        that actually initiates the QP, false on the other side.  This
        bool is used to know which way to send the first CTS
        message. */
    bool endpoint_initiator;

    /** pointer to remote proc's CPC data (essentially its CPC modex
        message) */
    osdh_connect_base_module_data_t *endpoint_remote_cpc_data;

    /** current state of the connection */
    sdh_endpoint_state_t     endpoint_state;

    /** number of connection retries attempted */
    size_t                      endpoint_retries;

    /** timestamp of when the first connection was attempted */
    double                      endpoint_tstamp;

    /** lock for concurrent access to endpoint state */
    opal_mutex_t                endpoint_lock;

    /** list of pending frags due to lazy connection establishment
        for this endpotint */
    opal_list_t                 pending_lazy_frags;

    sdh_endpoint_qp_t *qps;
#if OPAL_HAVE_CONNECTX_XRC_DOMAINS
    struct ibv_qp *xrc_recv_qp;
#else
    uint32_t xrc_recv_qp_num; /* in xrc we will use it as recv qp */
#endif
    uint32_t xrc_recv_psn;

    /** list of pending rget ops */
    opal_list_t                 pending_get_frags;
    /** list of pending rput ops */
    opal_list_t                 pending_put_frags;

    /** number of available get tokens */
    int32_t                     get_tokens;

    /** subnet id of this endpoint*/
    uint64_t subnet_id;
    /** used only for xrc; pointer to struct that keeps remote port
        info */
    struct ib_address_t *ib_addr;

    /** number of eager received */
    int32_t eager_recv_count;
    /** info about remote RDMA buffer */
    sdh_eager_rdma_remote_t eager_rdma_remote;
    /** info about local RDMA buffer */
    sdh_eager_rdma_local_t eager_rdma_local;
    /** index of the endpoint in endpoints array */
    int32_t index;

    /** does the endpoint require network byte ordering? */
    bool nbo;
    /** use eager rdma for this peer? */
    bool use_eager_rdma;

    /** information about the remote port */
    sdh_rem_info_t rem_info;

    /** Frag for initial wireup CTS protocol; will be NULL if CPC
        indicates that it does not want to use CTS */
    sdh_recv_frag_t endpoint_cts_frag;
    /** Memory registration info for the CTS frag */
    struct ibv_mr *endpoint_cts_mr;

    /** Whether we've posted receives on this EP or not (only used in
        CTS protocol) */
    bool endpoint_posted_recvs;

    /** Whether we've received the CTS from the peer or not (only used
        in CTS protocol) */
    bool endpoint_cts_received;

    /** Whether we've send out CTS to the peer or not (only used in
        CTS protocol) */
    bool endpoint_cts_sent;
};

// ================================= interface =================================

typedef struct mca_btl_base_endpoint_t mca_btl_base_endpoint_t;
typedef mca_btl_base_endpoint_t  sdh_endpoint_t;

OBJ_CLASS_DECLARATION(sdh_endpoint_t);

static inline int32_t qp_get_wqe(sdh_endpoint_t *ep, const int qp)
{
    return OPAL_THREAD_ADD_FETCH32(&ep->qps[qp].qp->sd_wqe, -1);
}

static inline int32_t qp_put_wqe(sdh_endpoint_t *ep, const int qp)
{
    return OPAL_THREAD_ADD_FETCH32(&ep->qps[qp].qp->sd_wqe, 1);
}


static inline int32_t qp_inc_inflight_wqe(sdh_endpoint_t *ep, const int qp, sdh_com_frag_t *frag)
{
    frag->n_wqes_inflight = 0;
    return OPAL_THREAD_ADD_FETCH32(&ep->qps[qp].qp->sd_wqe_inflight, 1);
}

static inline void qp_inflight_wqe_to_frag(sdh_endpoint_t *ep, const int qp, sdh_com_frag_t *frag)
{

    frag->n_wqes_inflight = ep->qps[qp].qp->sd_wqe_inflight;
    ep->qps[qp].qp->sd_wqe_inflight = 0;
}

static inline int qp_frag_to_wqe(sdh_endpoint_t *ep, const int qp, sdh_com_frag_t *frag)
{
    int n;
    n = frag->n_wqes_inflight;
    OPAL_THREAD_ADD_FETCH32(&ep->qps[qp].qp->sd_wqe, n);
    frag->n_wqes_inflight = 0;

    return n;
}

static inline int qp_need_signal(sdh_endpoint_t *ep, const int qp, size_t size, int rdma)
{

    /* note that size here is payload only */
    if (ep->qps[qp].qp->sd_wqe <= 0  ||
            size + sizeof(sdh_header_t) + (rdma ? sizeof(sdh_footer_t) : 0) > ep->qps[qp].ib_inline_max ||
             (!BTL_OPENIB_QP_TYPE_PP(qp) && ep->endpoint_btl->qps[qp].u.srq_qp.sd_credits <= 0)) 
    {
        ep->qps[qp].qp->wqe_count = QP_TX_BATCH_COUNT;
        return 1;
    }

    if (0 < --ep->qps[qp].qp->wqe_count) 
    {
        return 0;
    }

    ep->qps[qp].qp->wqe_count = QP_TX_BATCH_COUNT;
    return 1;
}

static inline void qp_reset_signal_count(sdh_endpoint_t *ep, const int qp)
{
    ep->qps[qp].qp->wqe_count = QP_TX_BATCH_COUNT;
}



int sdh_endpoint_send(mca_btl_base_endpoint_t*, sdh_send_frag_t*);

int sdh_endpoint_post_send(sdh_endpoint_t*, sdh_send_frag_t*);

void sdh_endpoint_send_credits(mca_btl_base_endpoint_t*, const int);

void sdh_endpoint_connect_eager_rdma(sdh_endpoint_t*);

int sdh_endpoint_post_recvs(sdh_endpoint_t*);

// the endpoint lock must be held with OPAL_THREAD_LOCK for both CTS and cpc complete 
void sdh_endpoint_send_cts(sdh_endpoint_t *endpoint);

void sdh_endpoint_cpc_complete(sdh_endpoint_t*);

void sdh_endpoint_connected(sdh_endpoint_t*);

void sdh_endpoint_init(sdh_module_t*,
                       mca_btl_base_endpoint_t*,
                       osdh_connect_base_module_t *local_cpc,
                       struct sdh_proc_modex_t *remote_proc_info,
                       osdh_connect_base_module_data_t *remote_cpc_data);

/*
 * Invoke an error on the btl associated with an endpoint.  If we
 * don't have an endpoint, then just use the first one on the
 * component list of BTLs.
 */
void *sdh_endpoint_invoke_error(void *endpoint);

static inline int post_recvs(mca_btl_base_endpoint_t *ep, const int qp, const int num_post)
{
    int i, rc;
    struct ibv_recv_wr *bad_wr, *wr_list = NULL, *wr = NULL;
    sdh_module_t *openib_btl = ep->endpoint_btl;

    if(0 == num_post)
        return OPAL_SUCCESS;

    for(i = 0; i < num_post; i++) {
        opal_free_list_item_t* item;
        item = opal_free_list_wait (&openib_btl->device->qps[qp].recv_free);
        to_base_frag(item)->base.order = qp;
        to_com_frag(item)->endpoint = ep;
        if(NULL == wr)
            wr = wr_list = &to_recv_frag(item)->rd_desc;
        else
            wr = wr->next = &to_recv_frag(item)->rd_desc;
        OPAL_OUTPUT((-1, "Posting recv (QP num %d): WR ID %p, SG addr %p, len %d, lkey %d",
                     ep->qps[qp].qp->lcl_qp->qp_num,
                     (void*) ((uintptr_t*)wr->wr_id),
                     (void*)((uintptr_t*) wr->sg_list[0].addr),
                     wr->sg_list[0].length,
                     wr->sg_list[0].lkey));
    }

    wr->next = NULL;

    rc = ibv_post_recv(ep->qps[qp].qp->lcl_qp, wr_list, &bad_wr);
    if (0 == rc)
        return OPAL_SUCCESS;

    BTL_ERROR(("error %d posting receive on qp %d", rc, qp));
    return OPAL_ERROR;
}

static inline int sdh_endpoint_post_rr_nolock(
        mca_btl_base_endpoint_t *ep, const int qp)
{
    int rd_rsv = sdh_component.qp_infos[qp].u.pp_qp.rd_rsv;
    int rd_num = sdh_component.qp_infos[qp].rd_num;
    int rd_low = sdh_component.qp_infos[qp].rd_low;
    int cqp = sdh_component.credits_qp, rc;
    int cm_received = 0, num_post = 0;

    assert(BTL_OPENIB_QP_TYPE_PP(qp));

    if(ep->qps[qp].u.pp_qp.rd_posted <= rd_low)
        num_post = rd_num - ep->qps[qp].u.pp_qp.rd_posted;

    assert(num_post >= 0);

    if(ep->qps[qp].u.pp_qp.cm_received >= (rd_rsv >> 2))
        cm_received = ep->qps[qp].u.pp_qp.cm_received;

    if((rc = post_recvs(ep, qp, num_post)) != OPAL_SUCCESS) {
        return rc;
    }
    OPAL_THREAD_ADD_FETCH32(&ep->qps[qp].u.pp_qp.rd_posted, num_post);
    OPAL_THREAD_ADD_FETCH32(&ep->qps[qp].u.pp_qp.rd_credits, num_post);

    /* post buffers for credit management on credit management qp */
    if((rc = post_recvs(ep, cqp, cm_received)) != OPAL_SUCCESS) {
        return rc;
    }
    OPAL_THREAD_ADD_FETCH32(&ep->qps[qp].u.pp_qp.cm_return, cm_received);
    OPAL_THREAD_ADD_FETCH32(&ep->qps[qp].u.pp_qp.cm_received, -cm_received);

    assert(ep->qps[qp].u.pp_qp.rd_credits <= rd_num &&
            ep->qps[qp].u.pp_qp.rd_credits >= 0);

    return OPAL_SUCCESS;
}

static inline int sdh_endpoint_post_rr(
        mca_btl_base_endpoint_t *ep, const int qp)
{
    int ret;
    OPAL_THREAD_LOCK(&ep->endpoint_lock);
    ret =  sdh_endpoint_post_rr_nolock(ep, qp);
    OPAL_THREAD_UNLOCK(&ep->endpoint_lock);
    return ret;
}

static inline __opal_attribute_always_inline__ bool btl_openib_credits_send_trylock (sdh_endpoint_t *ep, int qp)
{
    int32_t _tmp_value = 0;
    return OPAL_ATOMIC_COMPARE_EXCHANGE_STRONG_32(&ep->qps[qp].rd_credit_send_lock, &_tmp_value, 1);
}

#define BTL_OPENIB_CREDITS_SEND_UNLOCK(E, Q)                    \
    OPAL_ATOMIC_SWAP_32 (&(E)->qps[(Q)].rd_credit_send_lock, 0)
#define BTL_OPENIB_GET_CREDITS(FROM, TO)        \
    TO = OPAL_ATOMIC_SWAP_32(&FROM, 0)


static inline bool check_eager_rdma_credits(const sdh_endpoint_t *ep)
{
    return (ep->eager_rdma_local.credits > ep->eager_rdma_local.rd_win) ? true :
        false;
}

static inline bool
check_send_credits(const sdh_endpoint_t *ep, const int qp)
{

    if(!BTL_OPENIB_QP_TYPE_PP(qp))
        return false;

    return (ep->qps[qp].u.pp_qp.rd_credits >=
            sdh_component.qp_infos[qp].u.pp_qp.rd_win) ? true : false;
}

static inline void send_credits(sdh_endpoint_t *ep, int qp)
{
    if(BTL_OPENIB_QP_TYPE_PP(qp)) {
        if(check_send_credits(ep, qp))
            goto try_send;
    } else {
        qp = sdh_component.credits_qp;
    }

    if(!check_eager_rdma_credits(ep))
        return;

try_send:
    if(btl_openib_credits_send_trylock(ep, qp))
        sdh_endpoint_send_credits(ep, qp);
}

static inline int check_endpoint_state(sdh_endpoint_t *ep,
        mca_btl_base_descriptor_t *des, opal_list_t *pending_list)
{
    int rc = OPAL_ERR_RESOURCE_BUSY;

    switch(ep->endpoint_state) {
        case MCA_BTL_IB_CLOSED:
            rc = ep->endpoint_local_cpc->cbm_start_connect(ep->endpoint_local_cpc, ep);
            if (OPAL_SUCCESS == rc) {
                rc = OPAL_ERR_RESOURCE_BUSY;
            }
            /* fall through */
        default:
            opal_list_append(pending_list, (opal_list_item_t *)des);
            break;
        case MCA_BTL_IB_FAILED:
            rc = OPAL_ERR_UNREACH;
            break;
        case MCA_BTL_IB_CONNECTED:
            rc = OPAL_SUCCESS;
            break;
    }

    return rc;
}

static inline __opal_attribute_always_inline__ int
ib_send_flags(uint32_t size, sdh_endpoint_qp_t *qp, int do_signal)
{
    if (do_signal) {
        return IBV_SEND_SIGNALED |
            ((size <= qp->ib_inline_max) ? IBV_SEND_INLINE : 0);
    } else {
        return   ((size <= qp->ib_inline_max) ? IBV_SEND_INLINE : 0);
    }
}

static inline int
acquire_eager_rdma_send_credit(sdh_endpoint_t *endpoint)
{
    if(OPAL_THREAD_ADD_FETCH32(&endpoint->eager_rdma_remote.tokens, -1) < 0) {
        OPAL_THREAD_ADD_FETCH32(&endpoint->eager_rdma_remote.tokens, 1);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    return OPAL_SUCCESS;
}

static inline int post_send(sdh_endpoint_t *ep,
        sdh_send_frag_t *frag, const bool rdma, int do_signal)
{
    sdh_module_t *openib_btl = ep->endpoint_btl;
    mca_btl_base_segment_t *seg = &to_base_frag(frag)->segment;
    struct ibv_sge *sg = &to_com_frag(frag)->sg_entry;
    struct ibv_send_wr *sr_desc = &to_out_frag(frag)->sr_desc;
    struct ibv_send_wr *bad_wr;
    int qp = to_base_frag(frag)->base.order;

    sg->length = seg->seg_len + sizeof(sdh_header_t) +
        (rdma ? sizeof(sdh_footer_t) : 0) + frag->coalesced_length;

    sr_desc->send_flags = ib_send_flags(sg->length, &(ep->qps[qp]), do_signal);

    if(ep->nbo)
        BTL_OPENIB_HEADER_HTON(*frag->hdr);

    if(rdma) {
        int32_t head;
        sdh_footer_t* ftr =
            (sdh_footer_t*)(((char*)frag->hdr) + sg->length +
                    BTL_OPENIB_FTR_PADDING(sg->length) - sizeof(sdh_footer_t));
        sr_desc->opcode = IBV_WR_RDMA_WRITE;
        MCA_BTL_OPENIB_RDMA_FRAG_SET_SIZE(ftr, sg->length);
        MCA_BTL_OPENIB_RDMA_MAKE_LOCAL(ftr);
#if OPAL_ENABLE_DEBUG
        /* NTH: generate the sequence from the remote head index to ensure that the
         * wrong sequence isn't set. The way this code used to look the sequence number
         * and head were updated independently and it led to false positives for incorrect
         * sequence numbers. */
        MCA_BTL_OPENIB_RDMA_MOVE_INDEX(ep->eager_rdma_remote.head, head, ftr->seq);
#else
        MCA_BTL_OPENIB_RDMA_MOVE_INDEX(ep->eager_rdma_remote.head, head);
#endif
        if(ep->nbo)
            BTL_OPENIB_FOOTER_HTON(*ftr);

        sr_desc->wr.rdma.rkey = ep->eager_rdma_remote.rkey;
        sr_desc->wr.rdma.remote_addr =
            ep->eager_rdma_remote.base.lval +
            head * openib_btl->eager_rdma_frag_size +
            sizeof(sdh_header_t) +
            sdh_component.eager_limit +
            sizeof(sdh_footer_t);
        sr_desc->wr.rdma.remote_addr -= sg->length + BTL_OPENIB_FTR_PADDING(sg->length);
    } else {
        if(BTL_OPENIB_QP_TYPE_PP(qp)) {
            sr_desc->opcode = IBV_WR_SEND;
        } else {
            sr_desc->opcode = IBV_WR_SEND_WITH_IMM;
#if !defined(WORDS_BIGENDIAN) && OPAL_ENABLE_HETEROGENEOUS_SUPPORT
            sr_desc->imm_data = htonl(ep->rem_info.rem_index);
#else
            sr_desc->imm_data = ep->rem_info.rem_index;
#endif
        }
    }

#if HAVE_XRC
#if OPAL_HAVE_CONNECTX_XRC_DOMAINS
    if(BTL_OPENIB_QP_TYPE_XRC(qp))
        sr_desc->qp_type.xrc.remote_srqn = ep->rem_info.rem_srqs[qp].rem_srq_num;
#else
    if(BTL_OPENIB_QP_TYPE_XRC(qp))
        sr_desc->xrc_remote_srq_num = ep->rem_info.rem_srqs[qp].rem_srq_num;
#endif
#endif
    assert(sg->addr == (uint64_t)(uintptr_t)frag->hdr);

    if (sr_desc->send_flags & IBV_SEND_SIGNALED) {
        qp_inflight_wqe_to_frag(ep, qp, to_com_frag(frag));
    } else {
        qp_inc_inflight_wqe(ep, qp, to_com_frag(frag));
    }

    return ibv_post_send(ep->qps[qp].qp->lcl_qp, sr_desc, &bad_wr);
}

/* called with the endpoint lock held */
static inline int sdh_endpoint_credit_acquire (struct mca_btl_base_endpoint_t *endpoint, int qp,
                                                          int prio, size_t size, bool *do_rdma,
                                                          sdh_send_frag_t *frag, bool queue_frag)
{
    sdh_module_t *openib_btl = endpoint->endpoint_btl;
    sdh_header_t *hdr = frag->hdr;
    size_t eager_limit;
    int32_t cm_return;

    eager_limit = sdh_component.eager_limit +
        sizeof(sdh_header_coalesced_t) +
        sizeof(sdh_control_header_t);

    if (!(prio && size < eager_limit && acquire_eager_rdma_send_credit(endpoint) == OPAL_SUCCESS)) {
        *do_rdma = false;
        prio = !prio;

        if (BTL_OPENIB_QP_TYPE_PP(qp)) {
            if (OPAL_THREAD_ADD_FETCH32(&endpoint->qps[qp].u.pp_qp.sd_credits, -1) < 0) {
                OPAL_THREAD_ADD_FETCH32(&endpoint->qps[qp].u.pp_qp.sd_credits, 1);
                if (queue_frag) {
                    opal_list_append(&endpoint->qps[qp].no_credits_pending_frags[prio],
                                     (opal_list_item_t *)frag);
                }

                return OPAL_ERR_OUT_OF_RESOURCE;
            }
        } else {
            if(OPAL_THREAD_ADD_FETCH32(&openib_btl->qps[qp].u.srq_qp.sd_credits, -1) < 0) {
                OPAL_THREAD_ADD_FETCH32(&openib_btl->qps[qp].u.srq_qp.sd_credits, 1);
                if (queue_frag) {
                    OPAL_THREAD_LOCK(&openib_btl->ib_lock);
                    opal_list_append(&openib_btl->qps[qp].u.srq_qp.pending_frags[prio],
                                     (opal_list_item_t *)frag);
                    OPAL_THREAD_UNLOCK(&openib_btl->ib_lock);
                }

                return OPAL_ERR_OUT_OF_RESOURCE;
            }
        }
    } else {
        /* High priority frag. Try to send over eager RDMA */
        *do_rdma = true;
    }

    /* Set all credits */
    BTL_OPENIB_GET_CREDITS(endpoint->eager_rdma_local.credits, hdr->credits);
    if (hdr->credits) {
        hdr->credits |= BTL_OPENIB_RDMA_CREDITS_FLAG;
    }

    if (!*do_rdma) {
        if (BTL_OPENIB_QP_TYPE_PP(qp) && 0 == hdr->credits) {
            BTL_OPENIB_GET_CREDITS(endpoint->qps[qp].u.pp_qp.rd_credits, hdr->credits);
        }
    } else {
        hdr->credits |= (qp << 11);
    }

    BTL_OPENIB_GET_CREDITS(endpoint->qps[qp].u.pp_qp.cm_return, cm_return);
    /* cm_seen is only 8 bytes, but cm_return is 32 bytes */
    if(cm_return > 255) {
        hdr->cm_seen = 255;
        cm_return -= 255;
        OPAL_THREAD_ADD_FETCH32(&endpoint->qps[qp].u.pp_qp.cm_return, cm_return);
    } else {
        hdr->cm_seen = cm_return;
    }

    return OPAL_SUCCESS;
}

/* called with the endpoint lock held. */
static inline void sdh_endpoint_credit_release (struct mca_btl_base_endpoint_t *endpoint, int qp,
                                                           bool do_rdma, sdh_send_frag_t *frag)
{
    sdh_header_t *hdr = frag->hdr;

    if (BTL_OPENIB_IS_RDMA_CREDITS(hdr->credits)) {
        OPAL_THREAD_ADD_FETCH32(&endpoint->eager_rdma_local.credits, BTL_OPENIB_CREDITS(hdr->credits));
    }

    if (do_rdma) {
        OPAL_THREAD_ADD_FETCH32(&endpoint->eager_rdma_remote.tokens, 1);
    } else {
        if(BTL_OPENIB_QP_TYPE_PP(qp)) {
            OPAL_THREAD_ADD_FETCH32 (&endpoint->qps[qp].u.pp_qp.rd_credits, hdr->credits);
            OPAL_THREAD_ADD_FETCH32(&endpoint->qps[qp].u.pp_qp.sd_credits, 1);
        } else if BTL_OPENIB_QP_TYPE_SRQ(qp){
            sdh_module_t *openib_btl = endpoint->endpoint_btl;
            OPAL_THREAD_ADD_FETCH32(&openib_btl->qps[qp].u.srq_qp.sd_credits, 1);
        }
    }
}

END_C_DECLS

#endif
