/*  =====================================================================
 *  idpbroker.h - Irondomo Protocol Broker API
 *  ===================================================================== */

#pragma once

#include <string>
#include <vector>
#include <iostream>

#include "czmq.h"
#include "idp_common.h"

#define HEARTBEAT_LIVENESS 3    //  3-5 is reasonable
#define HEARTBEAT_INTERVAL 2500 //  msecs
#define HEARTBEAT_EXPIRY HEARTBEAT_INTERVAL *HEARTBEAT_LIVENESS

namespace IDP
{

  class IDPBroker
  {

    //  .split service class structure
    //  The service class defines a single service instance:

    typedef struct
    {
      IDP::IDPBroker *broker; //  Broker instance
      char *name;             //  Service name
      zlist_t *requests;      //  List of client requests
      zlist_t *waiting;       //  List of waiting workers
      size_t workers;         //  How many workers we have
    } service_t;

    //  .split worker class structure
    //  The worker class defines a single worker, idle or active:

    typedef struct
    {
      IDP::IDPBroker *broker; //  Broker instance
      void **socket;          //  Worker socket
      char *identity;         //  Identity of worker
      zframe_t *address;      //  Address frame to route to
      service_t *service;     //  Owning service, if known
      int64_t expiry;         //  Expires at unless heartbeat
    } worker_t;

  public:
    IDPBroker(const std::string &clear_endpoint, const std::string &curve_endpoint, std::pair<std::string, std::string> *credentials = NULL, const std::string &credentials_path = "", bool authenticate = false, bool verbose = false)
    {

      //  Initialize broker state
      _clear_endpoint = strdup(clear_endpoint.c_str());
      //_clear_socket = zsock_new_router(_clear_endpoint); //(ZMQ_ROUTER);
      _clear_socket = zsock_new(ZMQ_ROUTER); //(ZMQ_ROUTER);
      _credentials = credentials;
      zclock_log("I: IDP broker/0.2.0 clear socket active at %s", _clear_endpoint);
      if (curve_endpoint != "")
      {
        _curve_endpoint = strdup(curve_endpoint.c_str());
        _curve_socket = zsock_new(ZMQ_ROUTER); //(ZMQ_ROUTER);
        zclock_log("I: IDP broker/0.2.0 CURVE socket active at %s", _curve_endpoint);
        if (_credentials)
        {
          zclock_log("I: Setting up CURVE credentials for socket active at %s", _curve_endpoint);
          zactor_t *auth = zactor_new(zauth, NULL);
          zstr_sendx(auth, "VERBOSE", NULL);
          zsock_wait(auth);

          //zstr_sendx(auth, "ALLOW", "*", NULL);
          //zsock_wait(auth);
          //zcert_t *c = zcert_new_from((const byte *)_credentials->first.c_str(), (const byte *)_credentials->second.c_str());
          //zcert_apply(c, _curve_socket);
          //zsock_set_curve_server(_curve_socket, 1);
          //zsock_set_zap_domain(_curve_socket, "*");

          zsock_set_curve_publickey(_curve_socket, strdup(_credentials->first.c_str()));
          zsock_set_curve_secretkey(_curve_socket, strdup(_credentials->second.c_str()));
          zsock_set_curve_server(_curve_socket, 1);
          zstr_sendx(auth, "CURVE", CURVE_ALLOW_ANY, NULL);
          zsock_wait(auth);
          //zsock_set_curve_server(_curve_socket, 1);
        }
        zsock_bind((zsock_t *)_curve_socket, "%s", _curve_endpoint);
      }
      else
      {
        _curve_endpoint = NULL;
        _curve_socket = NULL;
        zclock_log("I: IDP broker/0.2.0 curve socket NOT active");
      }
      zsock_bind((zsock_t *)_clear_socket, "%s", _clear_endpoint);

      _verbose = verbose;
      _authenticate = authenticate;
      _services = zhash_new();
      _workers = zhash_new();
      _waiting = zlist_new();
      _heartbeat_interval = HEARTBEAT_INTERVAL;
      _heartbeat_liveness = HEARTBEAT_LIVENESS;
      _heartbeat_at = zclock_time() + _heartbeat_interval;
    }
    ~IDPBroker()
    {
      if (_clear_socket)
        zsock_destroy((zsock_t **)&_clear_socket);

      if (_curve_socket)
        zsock_destroy((zsock_t **)&_curve_socket);

      zhash_destroy(&_services);
      zhash_destroy(&_workers);
      zlist_destroy(&_waiting);
      if (_clear_endpoint)
        delete _clear_endpoint;
      if (_curve_endpoint)
        delete _curve_endpoint;
    }

    int loop(void)
    {
      while (true)
      {

        zpoller_t *poller = zpoller_new(NULL);
        assert(poller);

        // Add a reader to the existing poller
        int rc = zpoller_add(poller, _clear_socket);
        assert(rc == 0);
        if (_curve_socket)
        {
          rc = zpoller_add(poller, _curve_socket);
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

        bool clear = (which == _clear_socket);

        //  Process next input message, if any
        if (which)
        {
          zmsg_t *msg = zmsg_recv(which);
          if (!msg)
            break; //  Interrupted
          if (_verbose)
          {
            zclock_log("I: received message:");
            zmsg_dump(msg);
          }
          zframe_t *sender = zmsg_pop(msg);
          zframe_t *empty = zmsg_pop(msg);
          zframe_t *header = zmsg_pop(msg);

          if (zframe_streq(header, IDPC_CLIENT))
            broker_client_msg(sender, msg, clear);
          else if (zframe_streq(header, IDPW_WORKER))
            broker_worker_msg(sender, msg, clear);
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
        if (zclock_time() > _heartbeat_at)
        {
          broker_purge();
          worker_t *worker = (worker_t *)zlist_first(_waiting);
          while (worker)
          {
            worker_send(worker, IDPW_HEARTBEAT, NULL, NULL);
            worker = (worker_t *)zlist_next(_waiting);
          }
          _heartbeat_at = zclock_time() + HEARTBEAT_INTERVAL;
        }
      }
      if (zctx_interrupted)
        printf("W: interrupt received, shutting down...\n");

      return 0;
    }

  private:
    //  .split broker worker_msg method
    //  The worker_msg method processes one READY, REPLY, HEARTBEAT or
    //  DISCONNECT message sent to the broker by a worker:

    void broker_worker_msg(zframe_t *sender, zmsg_t *msg, bool clear)
    {
      assert(zmsg_size(msg) >= 1); //  At least, command

      zframe_t *command = zmsg_pop(msg);
      char *identity = zframe_strhex(sender);
      int worker_ready = (zhash_lookup(_workers, identity) != NULL);
      free(identity);
      worker_t *worker = worker_require(sender, clear);

      if (zframe_streq(command, IDPW_READY))
      {
        if (worker_ready) //  Not first command in session
          worker_delete(worker, 1);
        else if (zframe_size(sender) >= 4 //  Reserved service name
                 && memcmp(zframe_data(sender), "mmi.", 4) == 0)
          worker_delete(worker, 1);
        else
        {
          //  Attach worker to service and mark as idle
          zframe_t *service_frame = zmsg_pop(msg);
          worker->service = service_require(service_frame);
          worker->service->workers++;
          worker_waiting(worker);
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
          zmsg_pushstr(msg, worker->service->name);
          zmsg_pushstr(msg, IDPC_CLIENT);
          zmsg_wrap(msg, client);
          zmsg_send(&msg, zframe_streq(command, IDPW_REPLY) ? _clear_socket : _curve_socket);
          worker_waiting(worker);
        }
        else
          worker_delete(worker, 1);
      }
      else if (zframe_streq(command, IDPW_HEARTBEAT))
      {
        if (worker_ready)
          worker->expiry = zclock_time() + HEARTBEAT_EXPIRY;
        else
          worker_delete(worker, 1);
      }
      else if (zframe_streq(command, IDPW_DISCONNECT))
        worker_delete(worker, 0);
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

    void broker_client_msg(zframe_t *sender, zmsg_t *msg, bool clear)
    {
      assert(zmsg_size(msg) >= 2); //  Service name + body

      zframe_t *service_frame = zmsg_pop(msg);
      service_t *service = service_require(service_frame);

      //  Set reply return address to client sender
      zmsg_wrap(msg, zframe_dup(sender));

      //  If we got a MMI service request, process that internally
      if (zframe_size(service_frame) >= 4 && memcmp(zframe_data(service_frame), "mmi.", 4) == 0)
      {
        char const *return_code;
        if (zframe_streq(service_frame, "mmi.service"))
        {
          char *name = zframe_strdup(zmsg_last(msg));
          service_t *service =
              (service_t *)zhash_lookup(_services, name);
          return_code = service && service->workers ? "200" : "404";
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
        zmsg_send(&msg, clear ? _clear_socket : _curve_socket);
      }
      else
        //  Else dispatch the message to the requested service
        service_dispatch(service, msg, clear);
      zframe_destroy(&service_frame);
    }

    //  .split broker purge method
    //  The purge method deletes any idle workers that haven't pinged us in a
    //  while. We hold workers from oldest to most recent, so we can stop
    //  scanning whenever we find a live worker. This means we'll mainly stop
    //  at the first worker, which is essential when we have large numbers of
    //  workers (since we call this method in our critical path):

    void broker_purge()
    {
      worker_t *worker = (worker_t *)zlist_first(_waiting);
      while (worker)
      {
        if (zclock_time() < worker->expiry)
          break; //  Worker is alive, we're done here
        if (_verbose)
          zclock_log("I: deleting expired worker: %s",
                     worker->identity);

        worker_delete(worker, 0);
        worker = (worker_t *)zlist_first(_waiting);
      }
    }

    //  .split service methods
    //  Here is the implementation of the methods that work on a service:

    //  Lazy constructor that locates a service by name, or creates a new
    //  service if there is no service already with that name.

    service_t *service_require(zframe_t *service_frame)
    {
      assert(service_frame);
      char *name = zframe_strdup(service_frame);

      service_t *service =
          (service_t *)zhash_lookup(_services, name);
      if (service == NULL)
      {
        service = (service_t *)zmalloc(sizeof(service_t));
        service->broker = this;
        service->name = name;
        service->requests = zlist_new();
        service->waiting = zlist_new();
        zhash_insert(_services, name, service);
        zhash_freefn(_services, name, service_destroy);
        if (_verbose)
          zclock_log("I: added service: %s", name);
      }
      else
        free(name);

      return service;
    }

    //  Service destructor is called automatically whenever the service is
    //  removed from broker->services.

    static void service_destroy(void *argument)
    {
      service_t *service = (service_t *)argument;
      while (zlist_size(service->requests))
      {
        std::pair<zmsg_t *, bool> *request = (std::pair<zmsg_t *, bool> *)zlist_pop(service->requests);
        zmsg_t *msg = request->first;
        zmsg_destroy(&msg);
        delete request;
      }
      zlist_destroy(&service->requests);
      zlist_destroy(&service->waiting);
      free(service->name);
      free(service);
    }

    //  .split service dispatch method
    //  The dispatch method sends requests to waiting workers:

    void service_dispatch(service_t *service, zmsg_t *msg, bool clear)
    {
      assert(service);
      if (msg) //  Queue message if any
      {
        std::pair<zmsg_t *, bool> *request = new std::pair<zmsg_t *, bool>(msg, clear);
        zlist_append(service->requests, request);
      }

      broker_purge();
      while (zlist_size(service->waiting) && zlist_size(service->requests))
      {
        worker_t *worker = (worker_t *)zlist_pop(service->waiting);
        zlist_remove(service->broker->_waiting, worker);
        std::pair<zmsg_t *, bool> *request = (std::pair<zmsg_t *, bool> *)zlist_pop(service->requests);
        zmsg_t *msg = request->first;
        worker_send(worker, (request->second ? IDPW_REQUEST : IDPW_REQUEST_CURVE), NULL, msg);
        zmsg_destroy(&msg);
        delete request;
      }
    }

    //  .split worker methods
    //  Here is the implementation of the methods that work on a worker:

    //  Lazy constructor that locates a worker by identity, or creates a new
    //  worker if there is no worker already with that identity.

    worker_t *worker_require(zframe_t *address, bool clear)
    {
      assert(address);

      //  self->workers is keyed off worker identity
      char *identity = zframe_strhex(address);
      worker_t *worker =
          (worker_t *)zhash_lookup(_workers, identity);

      if (worker == NULL)
      {
        worker = (worker_t *)zmalloc(sizeof(worker_t));
        worker->broker = this;
        worker->identity = identity;
        worker->address = zframe_dup(address);
        worker->socket = clear ? &_clear_socket : &_curve_socket;
        zhash_insert(_workers, identity, worker);
        zhash_freefn(_workers, identity, worker_destroy);
        if (_verbose)
          zclock_log("I: registering new worker: %s", identity);
      }
      else
        free(identity);
      return worker;
    }

    //  The delete method deletes the current worker.

    void worker_delete(worker_t *worker, int disconnect)
    {
      assert(worker);
      if (disconnect)
        worker_send(worker, IDPW_DISCONNECT, NULL, NULL);

      if (worker->service)
      {
        zlist_remove(worker->service->waiting, worker);
        worker->service->workers--;
      }
      zlist_remove(worker->broker->_waiting, worker);
      //  This implicitly calls s_worker_destroy
      zhash_delete(worker->broker->_workers, worker->identity);
    }

    //  Worker destructor is called automatically whenever the worker is
    //  removed from broker->workers.

    static void worker_destroy(void *worker)
    {
      worker_t *self = (worker_t *)worker;
      zframe_destroy(&self->address);
      free(self->identity);
      free(self);
    }

    //  .split worker send method
    //  The send method formats and sends a command to a worker. The caller may
    //  also provide a command option, and a message payload:

    void worker_send(worker_t *worker, const char *command, char *option, zmsg_t *msg)
    {
      msg = msg ? zmsg_dup(msg) : zmsg_new();

      //  Stack protocol envelope to start of message
      if (option)
        zmsg_pushstr(msg, option);
      zmsg_pushstr(msg, command);
      zmsg_pushstr(msg, IDPW_WORKER);

      //  Stack routing envelope to start of message
      zmsg_wrap(msg, zframe_dup(worker->address));

      if (worker->broker->_verbose)
      {
        zclock_log("I: sending %s to worker",
                   idps_commands[(int)*command]);
        zmsg_dump(msg);
      }
      zmsg_send(&msg, *(worker->socket));
    }

    //  This worker is now waiting for work

    void worker_waiting(worker_t *worker)
    {
      //  Queue to broker and service waiting lists
      assert(worker->broker);
      zlist_append(worker->broker->_waiting, worker);
      zlist_append(worker->service->waiting, worker);
      worker->expiry = zclock_time() + HEARTBEAT_EXPIRY;
      service_dispatch(worker->service, NULL, true);
    }

    void *_clear_socket;                               //  Socket for clients & workers
    void *_curve_socket;                               //  Socket for clients & workers
    std::pair<std::string, std::string> *_credentials; // Server keys
    int _verbose;                                      //  Print activity to stdout
    char *_clear_endpoint;                             //  Broker binds to this endpoint for clear channel
    char *_curve_endpoint;                             //  Broker binds to this endpoint for curve channel
    zhash_t *_services;                                //  Hash of known services
    zhash_t *_workers;                                 //  Hash of known workers
    zlist_t *_waiting;                                 //  List of waiting workers
    uint64_t _heartbeat_at;                            //  When to send HEARTBEAT
    uint64_t _heartbeat_interval;                      //  Interval between HEARTBEATs
    int _heartbeat_liveness;
    bool _authenticate; // Should we look for CURVE keys in keys archive for authentication, or accept all keys?
  };
} // namespace IDP
