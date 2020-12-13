/*  =====================================================================
 *  idcliapi.h - Irondomo Protocol Client API
 *  Implements the IDP/Worker spec at http://rfc.zeromq.org/spec:7.
 *  ===================================================================== */

#pragma once

#include "czmq.h"
#include "idp.h"

#ifdef __cplusplus
extern "C"
{
#endif

    //  Opaque class structure
    typedef struct _idcli_t idcli_t;

    idcli_t *
    idcli_new(char *broker, char *identity, int verbose);
    idcli_t *
    idcli_new2(char *broker, char *identity, int verbose);
    void
    idcli_destroy(idcli_t **self_p);
    void
    idcli_set_timeout(idcli_t *self, int timeout);
    void
    idcli_set_retries(idcli_t *self, int retries);
    zmsg_t *
    idcli_send(idcli_t *self, char *service, zmsg_t **request_p);
    int
    idcli_send2(idcli_t *self, char *service, zmsg_t **request_p);
    zmsg_t *
    idcli_recv2(idcli_t *self);

#ifdef __cplusplus
}
#endif

//  Structure of our class
//  We access these properties only via class methods

struct _idcli_t
{
    unsigned char api_version;
    char *_broker_host;
    char *_identity;
    bool _has_curve;
    char *_client_public_key;
    char *_client_secret_key;
    char *_server_public_key;
    zsock_t *_client; //  Socket to broker
    int _verbose;     //  Print activity to stdout
    int _timeout;     //  Request timeout
    int _retries;     //  Request retries
    zcert_t *_client_cert;
    zpoller_t *_poller;
};

//  ---------------------------------------------------------------------
//  Connect or reconnect to broker

void idcli_connect_to_broker(idcli_t *self)
{
    if (self->_client)
    {
        zsock_destroy((zsock_t **)&self->_client);
        self->_client = NULL;
    }

    if (self->_poller)
    {
        zpoller_destroy(&(self->_poller));
        self->_poller = NULL;
    }

    self->_client = zsock_new(self->api_version == 1 ? ZMQ_REQ : ZMQ_DEALER);
    zsock_set_identity(self->_client, self->_identity);

    self->_poller = zpoller_new(self->_client, NULL);

    if (self->_client_cert != NULL)
    {
        zcert_apply(self->_client_cert, self->_client);
        zsock_set_curve_serverkey(self->_client, self->_server_public_key);
    }

    zsock_connect(self->_client, "%s", self->_broker_host);
    if (self->_verbose)
        zclock_log("I: connecting to broker at %s...", self->_broker_host);
}

//  .split constructor and destructor
//  Here we have the constructor and destructor for our idcli class:

//  ---------------------------------------------------------------------
//  Constructor

idcli_t *
idcli_new(char *broker_host, char *identity, int verbose)
{
    assert(broker_host);

    idcli_t *self = (idcli_t *)zmalloc(sizeof(idcli_t));
    self->_broker_host = strdup(broker_host);
    self->_verbose = verbose;
    self->_identity = strdup(identity);
    self->_timeout = 2500; //  msecs
    self->_retries = 3;    //  Before we abandon
    self->_client_cert = NULL;
    self->_client = NULL;
    self->_poller = NULL;
    self->_client_public_key = NULL;
    self->_client_secret_key = NULL;
    self->_server_public_key = NULL;

    self->api_version = 1;

    return self;
}

idcli_t *
idcli_new2(char *broker_host, char *identity, int verbose)
{
    idcli_t *self = idcli_new(broker_host, identity, verbose);
    self->api_version = 2;

    return self;
}

void idcli_setup_curve(idcli_t *self, const char *client_public_key, const char *client_secret_key, const char *server_public_key)
{
    self->_has_curve = true;
    self->_client_public_key = strdup(client_public_key);
    self->_client_secret_key = strdup(client_secret_key);
    self->_server_public_key = strdup(server_public_key);

    uint8_t sec[32];
    uint8_t pub[32];
    zmq_z85_decode(sec, self->_client_secret_key);
    zmq_z85_decode(pub, self->_client_public_key);
    self->_client_cert = zcert_new_from(pub, sec);
}

//  ---------------------------------------------------------------------
//  Destructor

void idcli_destroy(idcli_t **self_p)
{
    assert(self_p);
    if (*self_p)
    {
        idcli_t *self = *self_p;

        if (self->_client)
        {
            zsock_destroy(&(self->_client));
            self->_client = NULL;
        }
        if (self->_poller)
        {
            zpoller_destroy(&(self->_poller));
            self->_poller = NULL;
        }
        if (self->_client_cert)
        {
            zcert_destroy(&(self->_client_cert));
            self->_client_cert = NULL;
        }

        free(self->_broker_host);
        free(self->_identity);
        if (self->_client_public_key)
        {
            free(self->_client_public_key);
            self->_client_public_key = NULL;
        }

        if (self->_client_secret_key)
        {
            free(self->_client_secret_key);
            self->_client_secret_key = NULL;
        }

        if (self->_server_public_key)
        {
            free(self->_server_public_key);
            self->_server_public_key = NULL;
        }

        free(self);
        *self_p = NULL;
    }
}

//  .split configure retry behavior
//  These are the class methods. We can set the request timeout and number
//  of retry attempts, before sending requests:

//  ---------------------------------------------------------------------
//  Set request timeout

void idcli_set_timeout(idcli_t *self, int timeout)
{
    assert(self);
    self->_timeout = timeout;
}

//  ---------------------------------------------------------------------
//  Set request retries

void idcli_set_retries(idcli_t *self, int retries)
{
    assert(self);
    self->_retries = retries;
}

//  .split send request and wait for reply
//  Here is the send method. It sends a request to the broker and gets a
//  reply even if it has to retry several times. It takes ownership of the
//  request message, and destroys it when sent. It returns the reply
//  message, or NULL if there was no reply after multiple attempts:

zmsg_t *
idcli_send(idcli_t *self, char *service, zmsg_t **request_p)
{
    assert(self);
    assert(request_p);
    zmsg_t *request = *request_p;

    //  Prefix request with protocol frames
    //  Frame 1: "IDPCxy" (six bytes, IDP/Client x.y)
    //  Frame 2: Service name (printable string)
    zmsg_pushstr(request, service);
    zmsg_pushstr(request, IDPC_CLIENT);
    if (self->_verbose)
    {
        zclock_log("I: send request to '%s' service:", service);
        zmsg_dump(request);
    }
    int retries_left = self->_retries;
    while (retries_left && !zctx_interrupted)
    {
        zmsg_t *msg = zmsg_dup(request);
        int zres = zmsg_send(&msg, self->_client);

        if (zres >= 0)
        {
            if (self->_verbose)
                zclock_log("I: zmsg_send %p", msg);
        }
        else
        {
            if (self->_verbose)
                zclock_log("W: zmsg_send ERROR ! %s %p", zmq_strerror(errno), msg);
            if (msg != NULL)
            {
                zmsg_destroy(&msg);
            }
        }

        zsock_t *which = (zsock_t *)zpoller_wait(self->_poller, self->_timeout * ZMQ_POLL_MSEC);

        if (which == NULL)
        {
            if (zpoller_terminated(self->_poller))
            {
                break; //  Interrupted
            }
        }

        //  If we got a reply, process it
        if (which == self->_client)
        {
            zmsg_t *msg = zmsg_recv(self->_client);
            if (self->_verbose)
            {
                zclock_log("I: received reply:");
                zmsg_dump(msg);
            }
            //  We would handle malformed replies better in real code
            assert(zmsg_size(msg) >= 3);

            zframe_t *header = zmsg_pop(msg);
            assert(zframe_streq(header, IDPC_CLIENT));
            zframe_destroy(&header);

            zframe_t *reply_service = zmsg_pop(msg);
            assert(zframe_streq(reply_service, service));
            zframe_destroy(&reply_service);

            zmsg_destroy(&request);
            return msg; //  Success
        }
        else if (--retries_left)
        {
            if (self->_verbose)
                zclock_log("W: no reply, reconnecting...");
            idcli_connect_to_broker(self);
        }
        else
        {
            if (self->_verbose)
                zclock_log("W: permanent error, abandoning");
            break; //  Give up
        }
    }
    if (zctx_interrupted)
        printf("W: interrupt received, killing client...\n");
    zmsg_destroy(&request);

    return NULL;
}

//  .until
//  .skip
//  The send method now just sends one message, without waiting for a
//  reply. Since we're using a DEALER socket we have to send an empty
//  frame at the start, to create the same envelope that the REQ socket
//  would normally make for us:

int idcli_send2(idcli_t *self, char *service, zmsg_t **request_p)
{
    assert(self);
    assert(request_p);
    zmsg_t *request = *request_p;

    //  Prefix request with protocol frames
    //  Frame 0: empty (REQ emulation)
    //  Frame 1: "IDPCxy" (six bytes, IDP/Client x.y)
    //  Frame 2: Service name (printable string)
    zmsg_pushstr(request, service);
    zmsg_pushstr(request, IDPC_CLIENT);
    zmsg_pushstr(request, "");
    if (self->_verbose)
    {
        zclock_log("I: send request to '%s' service:", service);
        zmsg_dump(request);
    }
    zmsg_send(&request, self->_client);
    return 0;
}

//  .skip
//  The recv method takes BOOKMARK
//  ---------------------------------------------------------------------
//  Returns the reply message or NULL if there was no reply. Does not
//  attempt to recover from a broker failure, this is not possible
//  without storing all unanswered requests and resending them all...

zmsg_t *
idcli_recv2(idcli_t *self)
{
    assert(self);

    zsock_t *which = (zsock_t *)zpoller_wait(self->_poller, self->_timeout * ZMQ_POLL_MSEC);

    if (which == NULL)
    {
        if (zpoller_terminated(self->_poller))
        {
            return NULL; //  Interrupted
        }
    }

    //  If we got a reply, process it
    if (which == self->_client)
    {
        zmsg_t *msg = zmsg_recv(self->_client);
        if (self->_verbose)
        {
            zclock_log("I: received reply:");
            zmsg_dump(msg);
        }
        //  Don't try to handle errors, just assert noisily
        assert(zmsg_size(msg) >= 4);

        zframe_t *empty = zmsg_pop(msg);
        assert(zframe_streq(empty, ""));
        zframe_destroy(&empty);

        zframe_t *header = zmsg_pop(msg);
        assert(zframe_streq(header, IDPC_CLIENT));
        zframe_destroy(&header);

        zframe_t *service = zmsg_pop(msg);
        zframe_destroy(&service);

        return msg; //  Success
    }
    if (zctx_interrupted)
        printf("W: interrupt received, killing client...\n");
    else if (self->_verbose)
        zclock_log("W: permanent error, abandoning request");

    return NULL;
}
