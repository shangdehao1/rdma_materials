#include "btl_openib.h"
#include "btl_openib_frag.h"
#include "btl_openib_eager_rdma.h"

int sdh_frag_init(opal_free_list_item_t* item, void* ctx)
{
    sdh_frag_init_data_t* init_data = (sdh_frag_init_data_t *) ctx;
    sdh_frag_t *frag = to_base_frag(item);

    if(MCA_BTL_OPENIB_FRAG_RECV == frag->type) {
        to_recv_frag(frag)->qp_idx = init_data->order;
        to_com_frag(frag)->sg_entry.length =
            sdh_component.qp_infos[init_data->order].size +
            sizeof(sdh_header_t) +
            sizeof(sdh_header_coalesced_t) +
            sizeof(sdh_control_header_t);
    }

    if(MCA_BTL_OPENIB_FRAG_SEND == frag->type)
        to_send_frag(frag)->qp_idx = init_data->order;

    frag->list = init_data->list;

    return OPAL_SUCCESS;
}

static void base_constructor(sdh_frag_t *frag)
{
    frag->base.order = MCA_BTL_NO_ORDER;
}

static void com_constructor(sdh_com_frag_t *frag)
{
    sdh_frag_t *base_frag = to_base_frag(frag);
    sdh_reg_t* reg =
        (sdh_reg_t*)base_frag->base.super.registration;

    frag->registration = reg;

    if(reg) {
        frag->sg_entry.lkey = reg->mr->lkey;
    }
    frag->n_wqes_inflight = 0;
}

static void out_constructor(sdh_out_frag_t *frag)
{
    sdh_frag_t *base_frag = to_base_frag(frag);

    base_frag->base.des_segments = &base_frag->segment;
    base_frag->base.des_segment_count = 1;

    frag->sr_desc.wr_id = (uint64_t)(uintptr_t)frag;
    frag->sr_desc.sg_list = &to_com_frag(frag)->sg_entry;
    frag->sr_desc.num_sge = 1;
    frag->sr_desc.opcode = IBV_WR_SEND;
    frag->sr_desc.send_flags = IBV_SEND_SIGNALED;
    frag->sr_desc.next = NULL;
}

static void in_constructor(sdh_in_frag_t *frag)
{
    sdh_frag_t *base_frag = to_base_frag(frag);

    base_frag->base.des_segments = &base_frag->segment;
    base_frag->base.des_segment_count = 1;
}

static void send_constructor(sdh_send_frag_t *frag)
{
    sdh_frag_t *base_frag = to_base_frag(frag);

    base_frag->type = MCA_BTL_OPENIB_FRAG_SEND;

    frag->chdr = (sdh_header_t*)base_frag->base.super.ptr;
    frag->hdr = (sdh_header_t*)
        (((unsigned char*)base_frag->base.super.ptr) +
        sizeof(sdh_header_coalesced_t) +
        sizeof(sdh_control_header_t));
    base_frag->segment.seg_addr.pval = frag->hdr + 1;
    to_com_frag(frag)->sg_entry.addr = (uint64_t)(uintptr_t)frag->hdr;
    frag->coalesced_length = 0;
    OBJ_CONSTRUCT(&frag->coalesced_frags, opal_list_t);
}

static void recv_constructor(sdh_recv_frag_t *frag)
{
    sdh_frag_t *base_frag = to_base_frag(frag);

    base_frag->type = MCA_BTL_OPENIB_FRAG_RECV;

    frag->hdr = (sdh_header_t*)base_frag->base.super.ptr;
    base_frag->segment.seg_addr.pval =
        ((unsigned char* )frag->hdr) + sizeof(sdh_header_t);
    to_com_frag(frag)->sg_entry.addr = (uint64_t)(uintptr_t)frag->hdr;

    frag->rd_desc.wr_id = (uint64_t)(uintptr_t)frag;
    frag->rd_desc.sg_list = &to_com_frag(frag)->sg_entry;
    frag->rd_desc.num_sge = 1;
    frag->rd_desc.next = NULL;
}

static void send_control_constructor(sdh_send_control_frag_t *frag)
{
    to_base_frag(frag)->type = MCA_BTL_OPENIB_FRAG_CONTROL;
    /* adjusting headers because there is no coalesce header in control messages */
    frag->hdr = frag->chdr;
    to_base_frag(frag)->segment.seg_addr.pval = frag->hdr + 1;
    to_com_frag(frag)->sg_entry.addr = (uint64_t)(uintptr_t)frag->hdr;
}

static void put_constructor(sdh_put_frag_t *frag)
{
    to_base_frag(frag)->type = MCA_BTL_OPENIB_FRAG_SEND_USER;
    to_out_frag(frag)->sr_desc.opcode = IBV_WR_RDMA_WRITE;
    frag->cb.func = NULL;
}

static void get_constructor(sdh_get_frag_t *frag)
{
    to_base_frag(frag)->type = MCA_BTL_OPENIB_FRAG_RECV_USER;

    frag->sr_desc.wr_id = (uint64_t)(uintptr_t)frag;
    frag->sr_desc.sg_list = &to_com_frag(frag)->sg_entry;
    frag->sr_desc.num_sge = 1;
    frag->sr_desc.opcode = IBV_WR_RDMA_READ;
    frag->sr_desc.send_flags = IBV_SEND_SIGNALED;
    frag->sr_desc.next = NULL;
}

static void coalesced_constructor(sdh_coalesced_frag_t *frag)
{
    sdh_frag_t *base_frag = to_base_frag(frag);

    base_frag->type = MCA_BTL_OPENIB_FRAG_COALESCED;

    base_frag->base.des_segments = &base_frag->segment;
    base_frag->base.des_segment_count = 1;
}

OBJ_CLASS_INSTANCE(
                   sdh_frag_t,
                   mca_btl_base_descriptor_t,
                   base_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_com_frag_t,
                   sdh_frag_t,
                   com_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_out_frag_t,
                   sdh_com_frag_t,
                   out_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_in_frag_t,
                   sdh_com_frag_t,
                   in_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_send_frag_t,
                   sdh_out_frag_t,
                   send_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_recv_frag_t,
                   sdh_in_frag_t,
                   recv_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_send_control_frag_t,
                   sdh_send_frag_t,
                   send_control_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_put_frag_t,
                   sdh_out_frag_t,
                   put_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_get_frag_t,
                   sdh_in_frag_t,
                   get_constructor,
                   NULL);

OBJ_CLASS_INSTANCE(
                   sdh_coalesced_frag_t,
                   sdh_frag_t,
                   coalesced_constructor,
                   NULL);
