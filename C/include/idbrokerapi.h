//
//  Irondomo Protocol broker
//  A minimal C implementation of the Irondomo Protocol as defined in
//  http://rfc.zeromq.org/spec:7 and http://rfc.zeromq.org/spec:8.
//
#pragma once
#include "czmq.h"
#include "idp.h"

//  We'd normally pull these from config data

#define HEARTBEAT_LIVENESS 3    //  3-5 is reasonable
#define HEARTBEAT_INTERVAL 2500 //  msecs
#define HEARTBEAT_EXPIRY HEARTBEAT_INTERVAL *HEARTBEAT_LIVENESS

//  .split broker class structure
//  The broker class defines a single broker instance:

typedef struct
{
    void *_clear_socket;          //  Socket for clients & workers
    void *_curve_socket;          //  Socket for clients & workers
    int _verbose;                 //  Print activity to stdout
    char *_clear_endpoint;        //  Broker binds to this endpoint for clear channel
    char *_curve_endpoint;        //  Broker binds to this endpoint for curve channel
    char *_curve_secretkey;       //  Broker binds to this endpoint for curve channel
    char *_curve_publickey;       //  Broker binds to this endpoint for curve channel
    zhash_t *_services;           //  Hash of known services
    zhash_t *_workers;            //  Hash of known workers
    zlist_t *_waiting;            //  List of waiting workers
    uint64_t _heartbeat_at;       //  When to send HEARTBEAT
    uint64_t _heartbeat_interval; //  Interval between HEARTBEATs
    int _heartbeat_liveness;
    bool _authenticate; // Should we look for CURVE keys in keys archive for authentication, or accept all keys?
} broker_t;

static broker_t *
s_broker_new(const char *clear_endpoint, const char *curve_endpoint, const char *curve_publickey, const char *curve_secretkey, int _verbose);
static void
s_broker_destroy(broker_t **self_p);

static void
s_broker_worker_msg(broker_t *self, zframe_t *sender, zmsg_t *msg, bool clear);
static void
s_broker_client_msg(broker_t *self, zframe_t *sender, zmsg_t *msg, bool clear);
static void
s_broker_purge(broker_t *self);

//  .split service class structure
//  The service class defines a single service instance:

typedef struct
{
    broker_t *_broker;  //  Broker instance
    char *_name;        //  Service name
    zlist_t *_requests; //  List of client requests
    zlist_t *_waiting;  //  List of waiting workers
    size_t _workers;    //  How many workers we have
} service_t;

typedef struct
{
    zmsg_t *_msg;
    bool _clear;

} request_t;

static service_t *
s_service_require(broker_t *self, zframe_t *service_frame);
static void
s_service_destroy(void *argument);
static void
s_service_dispatch(service_t *service, zmsg_t *msg, bool clear);

//  .split worker class structure
//  The worker class defines a single worker, idle or active:

typedef struct
{
    broker_t *_broker;   //  Broker instance
    void **_socket;      //  Worker socket
    char *_identity;     //  Identity of worker
    zframe_t *_address;  //  Address frame to route to
    service_t *_service; //  Owning service, if known
    int64_t _expiry;     //  Expires at unless heartbeat
} worker_t;

static worker_t *
s_worker_require(broker_t *self, zframe_t *address, bool clear);
static void
s_worker_delete(worker_t *self, int disconnect);
static void
s_worker_destroy(void *argument);
static void
s_worker_send(worker_t *self, char *command, char *option,
              zmsg_t *msg);
static void
s_worker_waiting(worker_t *self);

//  .split broker constructor and destructor
//  Here are the constructor and destructor for the broker:

static broker_t *
s_broker_new(const char *clear_endpoint, const char *curve_endpoint, const char *curve_publickey, const char *curve_secretkey, int _verbose)
{
    broker_t *self = (broker_t *)zmalloc(sizeof(broker_t));

    //  Initialize broker state
    self->_clear_endpoint = strdup(clear_endpoint);
    self->_curve_endpoint = NULL;
    self->_curve_publickey = NULL;
    self->_curve_secretkey = NULL;
    self->_clear_socket = zsock_new(ZMQ_ROUTER);
    zclock_log("I: IDP broker/0.2.0 CLEAR socket active at %s", self->_clear_endpoint);
    if (curve_endpoint != NULL)
    {
        self->_curve_endpoint = strdup(curve_endpoint);
        self->_curve_publickey = strdup(curve_publickey);
        self->_curve_secretkey = strdup(curve_secretkey);
        self->_curve_socket = zsock_new(ZMQ_ROUTER); //(ZMQ_ROUTER);
        zclock_log("I: IDP broker/0.2.0 CURVE socket active at %s", self->_curve_endpoint);
        if (curve_secretkey != NULL && curve_publickey != NULL)
        {
            zclock_log("I: Setting up CURVE credentials for socket active at %s", self->_curve_endpoint);
            zactor_t *auth = zactor_new(zauth, NULL);
            zstr_send(auth, "VERBOSE");
            zsock_wait(auth);
            zsock_set_curve_publickey(self->_curve_socket, self->_curve_publickey);
            zsock_set_curve_secretkey(self->_curve_socket, self->_curve_secretkey);
            zsock_set_curve_server(self->_curve_socket, 1);
            zstr_sendx(auth, "CURVE", CURVE_ALLOW_ANY, NULL);
            zsock_wait(auth);
        }
        zsock_bind((zsock_t *)self->_curve_socket, "%s", self->_curve_endpoint);
    }
    else
    {
        self->_curve_endpoint = NULL;
        self->_curve_socket = NULL;
        zclock_log("I: IDP broker/0.2.0 CURVE socket NOT active");
    }
    zsock_bind((zsock_t *)self->_clear_socket, "%s", self->_clear_endpoint);

    self->_verbose = _verbose;
    self->_services = zhash_new();
    self->_workers = zhash_new();
    self->_waiting = zlist_new();
    self->_heartbeat_interval = HEARTBEAT_INTERVAL;
    self->_heartbeat_liveness = HEARTBEAT_LIVENESS;
    self->_heartbeat_at = zclock_time() + HEARTBEAT_INTERVAL;
    return self;
}

static void
s_broker_destroy(broker_t **self_p)
{
    assert(self_p);
    if (*self_p)
    {
        broker_t *self = *self_p;
        zhash_destroy(&self->_services);
        zhash_destroy(&self->_workers);
        zlist_destroy(&self->_waiting);
        if (self->_clear_socket)
            zsock_destroy((zsock_t **)&self->_clear_socket);
        if (self->_curve_socket)
            zsock_destroy((zsock_t **)&self->_curve_socket);

        free(self);
        *self_p = NULL;
    }
}

//  .split broker worker_msg method
//  The worker_msg method processes one READY, REPLY, HEARTBEAT or
//  DISCONNECT message sent to the broker by a worker:

static void
s_broker_worker_msg(broker_t *self, zframe_t *sender, zmsg_t *msg, bool clear)
{
    assert(zmsg_size(msg) >= 1); //  At least, command

    zframe_t *command = zmsg_pop(msg);
    char *identity = zframe_strhex(sender);
    int worker_ready = (zhash_lookup(self->_workers, identity) != NULL);
    free(identity);
    worker_t *worker = s_worker_require(self, sender, clear);

    if (zframe_streq(command, IDPW_READY))
    {
        if (worker_ready) //  Not first command in session
            s_worker_delete(worker, 1);
        else if (zframe_size(sender) >= 4 //  Reserved service name
                 && memcmp(zframe_data(sender), "mmi.", 4) == 0)
            s_worker_delete(worker, 1);
        else
        {
            //  Attach worker to service and mark as idle
            zframe_t *service_frame = zmsg_pop(msg);
            worker->_service = s_service_require(self, service_frame);
            worker->_service->_workers++;
            s_worker_waiting(worker);
            zframe_destroy(&service_frame);
        }
    }
    else if (zframe_streq(command, IDPW_REPLY) || zframe_streq(command, IDPW_REPLY_CURVE))
    {
        if (worker_ready)
        {
            //  Remove & save client return envelope and insert the
            //  protocol header and service name, then rewrap envelope.
            zframe_t *client = zmsg_unwrap(msg);
            zmsg_pushstr(msg, worker->_service->_name);
            zmsg_pushstr(msg, IDPC_CLIENT);
            zmsg_wrap(msg, client);
            zmsg_send(&msg, zframe_streq(command, IDPW_REPLY) ? self->_clear_socket : self->_curve_socket);
            s_worker_waiting(worker);
        }
        else
            s_worker_delete(worker, 1);
    }
    else if (zframe_streq(command, IDPW_HEARTBEAT))
    {
        if (worker_ready)
            worker->_expiry = zclock_time() + HEARTBEAT_EXPIRY;
        else
            s_worker_delete(worker, 1);
    }
    else if (zframe_streq(command, IDPW_DISCONNECT))
        s_worker_delete(worker, 0);
    else
    {
        zclock_log("E: invalid input message");
        zmsg_dump(msg);
    }
    free(command);
    zmsg_destroy(&msg);
}

//  .split broker client_msg method
//  Process a request coming from a client. We implement MMI requests
//  directly here (at present, we implement only the mmi.service request):

static void
s_broker_client_msg(broker_t *self, zframe_t *sender, zmsg_t *msg, bool clear)
{
    assert(zmsg_size(msg) >= 2); //  Service name + body

    zframe_t *service_frame = zmsg_pop(msg);
    service_t *service = s_service_require(self, service_frame);

    //  Set reply return address to client sender
    zmsg_wrap(msg, zframe_dup(sender));

    //  If we got a MMI service request, process that internally
    if (zframe_size(service_frame) >= 4 && memcmp(zframe_data(service_frame), "mmi.", 4) == 0)
    {
        char *return_code;
        if (zframe_streq(service_frame, "mmi.service"))
        {
            char *name = zframe_strdup(zmsg_last(msg));
            service_t *service =
                (service_t *)zhash_lookup(self->_services, name);
            return_code = service && service->_workers ? "200" : "404";
            free(name);
        }
        else
            return_code = "501";

        zframe_reset(zmsg_last(msg), return_code, strlen(return_code));

        //  Remove & save client return envelope and insert the
        //  protocol header and service name, then rewrap envelope.
        zframe_t *client = zmsg_unwrap(msg);
        zmsg_push(msg, zframe_dup(service_frame));
        zmsg_pushstr(msg, IDPC_CLIENT);
        zmsg_wrap(msg, client);
        zmsg_send(&msg, clear ? self->_clear_socket : self->_curve_socket);
    }
    else
        //  Else dispatch the message to the requested service
        s_service_dispatch(service, msg, clear);
    zframe_destroy(&service_frame);
}

//  .split broker purge method
//  The purge method deletes any idle workers that haven't pinged us in a
//  while. We hold workers from oldest to most recent, so we can stop
//  scanning whenever we find a live worker. This means we'll mainly stop
//  at the first worker, which is essential when we have large numbers of
//  workers (since we call this method in our critical path):

static void
s_broker_purge(broker_t *self)
{
    worker_t *worker = (worker_t *)zlist_first(self->_waiting);
    while (worker)
    {
        if (zclock_time() < worker->_expiry)
            break; //  Worker is alive, we're done here
        if (self->_verbose)
            zclock_log("I: deleting expired worker: %s",
                       worker->_identity);

        s_worker_delete(worker, 0);
        worker = (worker_t *)zlist_first(self->_waiting);
    }
}

//  .split service methods
//  Here is the implementation of the methods that work on a service:

//  Lazy constructor that locates a service by name, or creates a new
//  service if there is no service already with that name.

static service_t *
s_service_require(broker_t *self, zframe_t *service_frame)
{
    assert(service_frame);
    char *name = zframe_strdup(service_frame);

    service_t *service =
        (service_t *)zhash_lookup(self->_services, name);
    if (service == NULL)
    {
        service = (service_t *)zmalloc(sizeof(service_t));
        service->_broker = self;
        service->_name = name;
        service->_requests = zlist_new();
        service->_waiting = zlist_new();
        zhash_insert(self->_services, name, service);
        zhash_freefn(self->_services, name, s_service_destroy);
        if (self->_verbose)
            zclock_log("I: added service: %s", name);
    }
    else
        free(name);

    return service;
}

//  Service destructor is called automatically whenever the service is
//  removed from broker->_services.

static void
s_service_destroy(void *argument)
{
    service_t *service = (service_t *)argument;
    while (zlist_size(service->_requests))
    {
        request_t *req = zlist_pop(service->_requests);
        zmsg_t *msg = req->_msg;
        zmsg_destroy(&msg);
        free(req);
    }
    zlist_destroy(&service->_requests);
    zlist_destroy(&service->_waiting);
    free(service->_name);
    free(service);
}

//  .split service dispatch method
//  The dispatch method sends requests to waiting workers:

static void
s_service_dispatch(service_t *self, zmsg_t *msg, bool clear)
{
    assert(self);
    if (msg) //  Queue message if any
    {
        request_t *req = (request_t *)malloc(sizeof(request_t));
        req->_msg = msg;
        req->_clear = clear;
        zlist_append(self->_requests, req);
    }

    s_broker_purge(self->_broker);
    while (zlist_size(self->_waiting) && zlist_size(self->_requests))
    {
        worker_t *worker = zlist_pop(self->_waiting);
        zlist_remove(self->_broker->_waiting, worker);
        request_t *req = zlist_pop(self->_requests);
        ;
        zmsg_t *msg = req->_msg;
        s_worker_send(worker, (req->_clear ? IDPW_REQUEST : IDPW_REQUEST_CURVE), NULL, msg);
        zmsg_destroy(&msg);
        free(req);
    }
}

//  .split worker methods
//  Here is the implementation of the methods that work on a worker:

//  Lazy constructor that locates a worker by identity, or creates a new
//  worker if there is no worker already with that identity.

static worker_t *
s_worker_require(broker_t *self, zframe_t *address, bool clear)
{
    assert(address);

    //  self->_workers is keyed off worker identity
    char *identity = zframe_strhex(address);
    worker_t *worker =
        (worker_t *)zhash_lookup(self->_workers, identity);

    if (worker == NULL)
    {
        worker = (worker_t *)zmalloc(sizeof(worker_t));
        worker->_broker = self;
        worker->_identity = identity;
        worker->_address = zframe_dup(address);
        worker->_socket = clear ? &self->_clear_socket : &self->_curve_socket;
        zhash_insert(self->_workers, identity, worker);
        zhash_freefn(self->_workers, identity, s_worker_destroy);
        if (self->_verbose)
            zclock_log("I: registering new worker: %s", identity);
    }
    else
        free(identity);
    return worker;
}

//  The delete method deletes the current worker.

static void
s_worker_delete(worker_t *self, int disconnect)
{
    assert(self);
    if (disconnect)
        s_worker_send(self, IDPW_DISCONNECT, NULL, NULL);

    if (self->_service)
    {
        zlist_remove(self->_service->_waiting, self);
        self->_service->_workers--;
    }
    zlist_remove(self->_broker->_waiting, self);
    //  This implicitly calls s_worker_destroy
    zhash_delete(self->_broker->_workers, self->_identity);
}

//  Worker destructor is called automatically whenever the worker is
//  removed from broker->_workers.

static void
s_worker_destroy(void *argument)
{
    worker_t *self = (worker_t *)argument;
    zframe_destroy(&self->_address);
    free(self->_identity);
    free(self);
}

//  .split worker send method
//  The send method formats and sends a command to a worker. The caller may
//  also provide a command option, and a message payload:

static void
s_worker_send(worker_t *self, char *command, char *option, zmsg_t *msg)
{
    msg = msg ? zmsg_dup(msg) : zmsg_new();

    //  Stack protocol envelope to start of message
    if (option)
        zmsg_pushstr(msg, option);
    zmsg_pushstr(msg, command);
    zmsg_pushstr(msg, IDPW_WORKER);

    //  Stack routing envelope to start of message
    zmsg_wrap(msg, zframe_dup(self->_address));

    if (self->_broker->_verbose)
    {
        zclock_log("I: sending %s to worker",
                   idps_commands[(int)*command]);
        zmsg_dump(msg);
    }
    zmsg_send(&msg, *(self->_socket));
}

//  This worker is now waiting for work

static void
s_worker_waiting(worker_t *self)
{
    //  Queue to broker and service waiting lists
    assert(self->_broker);
    zlist_append(self->_broker->_waiting, self);
    zlist_append(self->_service->_waiting, self);
    self->_expiry = zclock_time() + HEARTBEAT_EXPIRY;
    s_service_dispatch(self->_service, NULL, true);
}

//  .split main task
//  Finally here is the main task. We create a new broker instance and
//  then processes messages on the broker socket:

int s_broker_loop(broker_t *self)
{
    while (true)
    {

        zpoller_t *poller = zpoller_new(NULL);
        assert(poller);

        // Add a reader to the existing poller
        int rc = zpoller_add(poller, self->_clear_socket);
        assert(rc == 0);
        if (self->_curve_socket)
        {
            rc = zpoller_add(poller, self->_curve_socket);
            assert(rc == 0);
        }

        zsock_t *which = (zsock_t *)zpoller_wait(poller, HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);

        //int rc = zmq_poll (items, 1, HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
        if (which == NULL)
        {
            if (zpoller_terminated(poller))
            {
                break; //  Interrupted
            }
        }

        bool clear = (which == self->_clear_socket);

        //  Process next input message, if any
        if (which)
        {
            zmsg_t *msg = zmsg_recv(which);
            if (!msg)
                break; //  Interrupted
            if (self->_verbose)
            {
                zclock_log("I: received message:");
                zmsg_dump(msg);
            }
            zframe_t *sender = zmsg_pop(msg);
            zframe_t *empty = zmsg_pop(msg);
            zframe_t *header = zmsg_pop(msg);

            if (zframe_streq(header, IDPC_CLIENT))
                s_broker_client_msg(self, sender, msg, clear);
            else if (zframe_streq(header, IDPW_WORKER))
                s_broker_worker_msg(self, sender, msg, clear);
            else
            {
                zclock_log("E: invalid message:");
                zmsg_dump(msg);
                zmsg_destroy(&msg);
            }
            zframe_destroy(&sender);
            zframe_destroy(&empty);
            zframe_destroy(&header);
        }
        //  Disconnect and delete any expired workers
        //  Send heartbeats to idle workers if needed
        if (zclock_time() > self->_heartbeat_at)
        {
            s_broker_purge(self);
            worker_t *worker = (worker_t *)zlist_first(self->_waiting);
            while (worker)
            {
                s_worker_send(worker, IDPW_HEARTBEAT, NULL, NULL);
                worker = (worker_t *)zlist_next(self->_waiting);
            }
            self->_heartbeat_at = zclock_time() + HEARTBEAT_INTERVAL;
        }
    }
    if (zctx_interrupted)
        printf("W: interrupt received, shutting down...\n");

    return 0;
}
