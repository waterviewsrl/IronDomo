/*  =====================================================================
 *  idcliapi2.h - Irondomo Protocol Client API
 *  Implements the MDP/Worker spec at http://rfc.zeromq.org/spec:7.
 *  ===================================================================== */

#ifndef __idcLIAPI_H_INCLUDED__
#define __idcLIAPI_H_INCLUDED__

#include "czmq.h"
#include "idp.h"

#ifdef __cplusplus
extern "C" {
#endif

//  Opaque class structure
typedef struct _idcli_t idcli_t;

idcli_t *
    idcli_new (char *broker, int verbose);
void
    idcli_destroy (idcli_t **self_p);
void
    idcli_set_timeout (idcli_t *self, int timeout);
int
    idcli_send (idcli_t *self, char *service, zmsg_t **request_p);
zmsg_t *
    idcli_recv (idcli_t *self);

#ifdef __cplusplus
}
#endif

#endif
