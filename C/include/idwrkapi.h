/*  =====================================================================
 *  idwrkapi.h - Irondomo Protocol Worker API
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
    typedef struct _idwrk_t idwrk_t;

    idwrk_t *
    idwrk_new(char *broker, char *service, char *identity, int verbose);
    void
    idwrk_destroy(idwrk_t **self_p);
    void
    idwrk_set_liveness(idwrk_t *self, int liveness);
    void
    idwrk_set_heartbeat(idwrk_t *self, int heartbeat);
    void
    idwrk_set_reconnect(idwrk_t *self, int reconnect);
    zmsg_t *
    idwrk_recv(idwrk_t *self, zmsg_t **reply_p);

#ifdef __cplusplus
}
#endif

//  Reliability parameters
#define HEARTBEAT_LIVENESS 3 //  3-5 is reasonable

//  .split worker class structure
//  This is the structure of a worker API instance. We use a pseudo-OO
//  approach in a lot of the C examples, as well as the CZMQ binding:

//  Structure of our class
//  We access these properties only via class methods

struct _idwrk_t
{
    char *_broker_host;
    char *_identity;
    bool _has_curve;
    char *_worker_public_key;
    char *_worker_secret_key;
    char *_server_public_key;
    zsock_t *_worker; //  Socket to broker
    zpoller_t *_poller;
    zcert_t *_worker_cert;
    char *_service;
    //void *_worker;
    int _verbose; //  Print activity to stdout

    //  Heartbeat management
    uint64_t _heartbeat_at; //  When to send HEARTBEAT
    size_t _liveness;       //  How many attempts left
    int _heartbeat;         //  Heartbeat delay, msecs
    int _reconnect;         //  Reconnect delay, msecs

    int _expect_reply;         //  Zero only at start
    zframe_t *_reply_to_clear; //  Return address, if any
    zframe_t *_reply_to_curve; //  Return address, if any
};

//  .split utility functions
//  We have two utility functions; to send a message to the broker and
//  to (re-)connect to the broker:

//  ---------------------------------------------------------------------
//  Send message to broker
//  If no msg is provided, creates one internally

static void
idwrk_send_to_broker(idwrk_t *self, char *command, char *option,
                     zmsg_t *msg)
{
    msg = msg ? zmsg_dup(msg) : zmsg_new();

    //  Stack protocol envelope to start of message
    if (option)
        zmsg_pushstr(msg, option);
    zmsg_pushstr(msg, command);
    zmsg_pushstr(msg, IDPW_WORKER);
    zmsg_pushstr(msg, "");

    if (self->_verbose)
    {
        zclock_log("I: sending %s to broker",
                   idps_commands[(int)*command]);
        zmsg_dump(msg);
    }
    zmsg_send(&msg, self->_worker);
}

//  ---------------------------------------------------------------------
//  Connect or reconnect to broker

void idwrk_connect_to_broker(idwrk_t *self)
{
    if (self->_worker)
    {
        zsock_destroy((zsock_t **)&(self->_worker));
        self->_worker = NULL;
    }
    if (self->_poller)
    {
        zpoller_destroy(&(self->_poller));
    }
    self->_worker = zsock_new(ZMQ_DEALER);
    zsock_set_identity(self->_worker, self->_identity);

    self->_poller = zpoller_new(self->_worker, NULL);

    if (self->_worker_cert != NULL)
    {
        zcert_apply(self->_worker_cert, self->_worker);
        zsock_set_curve_serverkey(self->_worker, self->_server_public_key);
    }

    zsock_connect(self->_worker, "%s", self->_broker_host);
    if (self->_verbose)
        zclock_log("I: connecting to broker at %s...", self->_broker_host);

    //  Register service with broker
    idwrk_send_to_broker(self, IDPW_READY, self->_service, NULL);

    //  If liveness hits zero, queue is considered disconnected
    self->_liveness = HEARTBEAT_LIVENESS;
    self->_heartbeat_at = zclock_time() + self->_heartbeat;
}

//  .split constructor and destructor
//  Here we have the constructor and destructor for our idwrk class:

//  ---------------------------------------------------------------------
//  Constructor

idwrk_t *
idwrk_new(char *broker_host, char *service, char *identity, int verbose)
{
    assert(broker_host);
    assert(service);

    idwrk_t *self = (idwrk_t *)zmalloc(sizeof(idwrk_t));

    self->_broker_host = strdup(broker_host);
    self->_service = strdup(service);
    self->_identity = identity;
    self->_has_curve = false;
    self->_verbose = verbose;
    self->_heartbeat = 2500; //  msecs
    self->_reconnect = 2500; //  msecs

    self->_expect_reply = 0;
    self->_worker_cert = NULL;
    self->_worker_cert = NULL;
    self->_poller = NULL;

    self->_worker_public_key = NULL;
    self->_worker_secret_key = NULL;
    self->_server_public_key = NULL;

    return self;
}

void idwrk_setup_curve(idwrk_t *self, const char *worker_public_key, const char *worker_secret_key, const char *server_public_key)
{
    self->_has_curve = true;
    self->_worker_public_key = strdup(worker_public_key);
    self->_worker_secret_key = strdup(worker_secret_key);
    self->_server_public_key = strdup(server_public_key);

    uint8_t sec[32];
    uint8_t pub[32];
    zmq_z85_decode(sec, self->_worker_secret_key);
    zmq_z85_decode(pub, self->_worker_public_key);
    self->_worker_cert = zcert_new_from(pub, sec);
}

//  ---------------------------------------------------------------------
//  Destructor

void idwrk_destroy(idwrk_t **self_p)
{
    assert(self_p);
    if (*self_p)
    {
        idwrk_t *self = *self_p;
        if (self->_worker)
        {
            zsock_destroy(&(self->_worker));
            self->_worker = NULL;
        }
        if (self->_poller)
        {
            zpoller_destroy(&(self->_poller));
            self->_poller = NULL;
        }
        if (self->_worker_cert)
        {
            zcert_destroy(&(self->_worker_cert));
            self->_worker_cert = NULL;
        }

        if (self->_worker_public_key)
        {
            free(self->_worker_public_key);
            self->_worker_public_key = NULL;
        }

        if (self->_worker_secret_key)
        {
            free(self->_worker_secret_key);
            self->_worker_secret_key = NULL;
        }

        if (self->_server_public_key)
        {
            free(self->_server_public_key);
            self->_server_public_key = NULL;
        }

        free(self->_broker_host);
        free(self->_service);
        free(self);
        *self_p = NULL;
    }
}

//  .split configure worker
//  We provide two methods to configure the worker API. You can set the
//  heartbeat interval and retries to match the expected network performance.

//  ---------------------------------------------------------------------
//  Set heartbeat delay

void idwrk_set_heartbeat(idwrk_t *self, int heartbeat)
{
    self->_heartbeat = heartbeat;
}

//  ---------------------------------------------------------------------
//  Set reconnect delay

void idwrk_set_reconnect(idwrk_t *self, int reconnect)
{
    self->_reconnect = reconnect;
}

//  .split recv method
//  This is the recv method; it's a little misnamed since it first sends
//  any reply and then waits for a new request. If you have a better name
//  for this, let me know:

//  ---------------------------------------------------------------------
//  Send reply, if any, to broker and wait for next request.

zmsg_t *
idwrk_recv(idwrk_t *self, zmsg_t **reply_p)
{
    //  Format and send the reply if we were provided one
    assert(reply_p);
    zmsg_t *reply = *reply_p;
    assert(reply || !self->_expect_reply);
    if (reply)
    {
        if (self->_reply_to_clear)
        {
            zmsg_wrap(reply, self->_reply_to_clear);
            idwrk_send_to_broker(self, IDPW_REPLY, NULL, reply);
        }
        else if (self->_reply_to_curve)
        {
            zmsg_wrap(reply, self->_reply_to_curve);
            idwrk_send_to_broker(self, IDPW_REPLY_CURVE, NULL, reply);
        }
        else
        {
            zclock_log("E: MISSING REPLY!");
        }

        zmsg_destroy(reply_p);
    }
    self->_expect_reply = 1;

    while (1)
    {
        zsock_t *which = (zsock_t *)zpoller_wait(self->_poller, self->_heartbeat * ZMQ_POLL_MSEC);

        if (which == NULL)
        {
            if (zpoller_terminated(self->_poller))
            {

                break; //  Interrupted
            }
        }

        if (which == self->_worker)
        {
            zmsg_t *msg = zmsg_recv(self->_worker);
            if (!msg)
                break; //  Interrupted
            if (self->_verbose)
            {
                zclock_log("I: received message from broker:");
                zmsg_dump(msg);
            }
            self->_liveness = HEARTBEAT_LIVENESS;

            //  Don't try to handle errors, just assert noisily
            assert(zmsg_size(msg) >= 3);

            zframe_t *empty = zmsg_pop(msg);
            assert(zframe_streq(empty, ""));
            zframe_destroy(&empty);

            zframe_t *header = zmsg_pop(msg);
            assert(zframe_streq(header, IDPW_WORKER));
            zframe_destroy(&header);

            zframe_t *command = zmsg_pop(msg);
            if (zframe_streq(command, IDPW_REQUEST) || zframe_streq(command, IDPW_REQUEST_CURVE))
            {
                //  We should pop and save as many addresses as there are
                //  up to a null part, but for now, just save one...
                self->_reply_to_clear = zframe_streq(command, IDPW_REQUEST) ? zmsg_unwrap(msg) : NULL;
                self->_reply_to_curve = zframe_streq(command, IDPW_REQUEST_CURVE) ? zmsg_unwrap(msg) : NULL;
                zframe_destroy(&command);
                //  .split process message
                //  Here is where we actually have a message to process; we
                //  return it to the caller application:
                return msg; //  We have a request to process
            }
            else if (zframe_streq(command, IDPW_HEARTBEAT))
                ; //  Do nothing for heartbeats
            else if (zframe_streq(command, IDPW_DISCONNECT))
                idwrk_connect_to_broker(self);
            else
            {
                zclock_log("E: invalid input message");
                zmsg_dump(msg);
            }
            zframe_destroy(&command);
            zmsg_destroy(&msg);
        }
        else if (--self->_liveness == 0)
        {
            if (self->_verbose)
                zclock_log("W: disconnected from broker - retrying...");
            zclock_sleep(self->_reconnect);
            idwrk_connect_to_broker(self);
        }
        //  Send HEARTBEAT if it's time
        if (zclock_time() > self->_heartbeat_at)
        {
            idwrk_send_to_broker(self, IDPW_HEARTBEAT, NULL, NULL);
            self->_heartbeat_at = zclock_time() + self->_heartbeat;
        }
    }
    if (zctx_interrupted)
        printf("W: interrupt received, killing worker...\n");
    return NULL;
}
