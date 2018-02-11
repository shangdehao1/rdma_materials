
// opal_btl_openib --> osdh
// mca_btl_openib -->sdh

#ifndef BTL_OPENIB_CONNECT_BASE_H
#define BTL_OPENIB_CONNECT_BASE_H

#include "connect.h"

#ifdef OPAL_HAVE_RDMAOE
#define BTL_OPENIB_CONNECT_BASE_CHECK_IF_NOT_IB(btl)                       \
        (((IBV_TRANSPORT_IB != ((btl)->device->ib_dev->transport_type)) || \
        (IBV_LINK_LAYER_ETHERNET == ((btl)->ib_port_attr.link_layer))) ?   \
        true : false)
#else
#define BTL_OPENIB_CONNECT_BASE_CHECK_IF_NOT_IB(btl)                       \
        ((IBV_TRANSPORT_IB != ((btl)->device->ib_dev->transport_type)) ?   \
        true : false)
#endif


// Forward declaration to resolve circular dependency
struct mca_btl_base_endpoint_t;

// Open function
int osdh_connect_base_register(void);

// Component-wide CPC init
int osdh_connect_base_init(void);

// Query CPCs to see if they want to run on a specific module
int osdh_connect_base_select_for_local_port(sdh_module_t *btl);

// Forward reference to avoid an include file loop
struct sdh_proc_modex_t;

// Select function
int osdh_connect_base_find_match(sdh_module_t *btl,
                                 struct sdh_proc_modex_t *peer_port,
                                 osdh_connect_base_module_t **local_cpc,
                                 osdh_connect_base_module_data_t **remote_cpc_data);

// Find a CPC's index so that we can send it in the modex
int osdh_connect_base_get_cpc_index(osdh_connect_base_component_t *cpc);

// Lookup a CPC by its index (received from the modex)
osdh_connect_base_component_t* osdh_connect_base_get_cpc_byindex(uint8_t index);

// Allocate a CTS frag
int osdh_connect_base_alloc_cts(struct mca_btl_base_endpoint_t *endpoint);

// Free a CTS frag
int osdh_connect_base_free_cts(struct mca_btl_base_endpoint_t *endpoint);

// Start a new connection to an endpoint
int osdh_connect_base_start(osdh_connect_base_module_t *cpc, 
                            struct mca_btl_base_endpoint_t *endpoint);

// Component-wide CPC finalize
void osdh_connect_base_finalize(void);

#endif
