#include "opal_config.h"

#include "btl_openib.h"
#include "btl_openib_endpoint.h"
#include "connect/connect.h"

// forward declear
static void empty_component_register(void);
static int empty_component_init(void);
static int empty_component_query(sdh_module_t *btl,
                                 osdh_connect_base_module_t **cpc);

// instance for osdh_connect_base_component_t
osdh_connect_base_component_t osdh_connect_empty = {
    "empty",
    empty_component_register,
    empty_component_init,
    empty_component_query,
    NULL
};

static void empty_component_register(void)
{
    // Nothing to do 
}

static int empty_component_init(void)
{
    // Never let this CPC run 
    return OPAL_ERR_NOT_SUPPORTED;
}

static int empty_component_query(sdh_module_t *btl,
                                 opal_btl_openib_connect_base_module_t **cpc)
{
    // Never let this CPC run 
    return OPAL_ERR_NOT_SUPPORTED;
}
