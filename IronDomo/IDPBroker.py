"""
Irondomo Protocol broker
A minimal implementation of IDP 

Author: Matteo Ferrabone <matteo.ferrabone@gmail.com>
"""

import os, os.path
import sys
import logging
import time
from binascii import hexlify

import zmq

# local
from IronDomo import IDP
from IronDomo.IDPHelpers import dump
from IronDomo.IDPHelpers import Router
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

class Service(object):
    """a single Service"""
    name = None # Service name
    requests = None # List of client requests
    waiting = None # List of waiting workers

    def __init__(self, name):
        self.name = name
        self.requests = []
        self.waiting = []
        self.idle = []

class Worker(object):
    """a Worker, idle or active"""
    identity = None # hex Identity of worker
    address = None # Address to route to
    service = None # Owning service, if known
    expiry = None # expires at this point, unless heartbeat
    clear = None

    def __init__(self, identity, address, lifetime, clear):
        self.identity = identity
        self.address = address
        self.expiry = time.time() + 1e-3*lifetime
        self.clear = clear

def countFiles(path):
    return len([name for name in os.listdir(path) if os.path.isfile(os.path.join(path, name))])

class IronDomoBroker(object):
    """
    Irondomo Protocol broker
    """

    # We'd normally pull these from config data
    INTERNAL_SERVICE_PREFIX = b"mmi."
    HEARTBEAT_LIVENESS = 3 # 3-5 is reasonable
    HEARTBEAT_INTERVAL = 2500 # msecs
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    # ---------------------------------------------------------------------

    ctx = None # Our context
    socketclear = None # Socket for clients & workers
    socketcurve = None # Socket for clients & workers
    poller = None # our Poller

    heartbeat_at = None# When to send HEARTBEAT
    services = None # known services
    workers = None # known workers
    waiting = None # idle workers

    verbose = False # Print activity to stdout

    credentials = None
    credentialsPath = None
    credentialsNum = None
    credentialsCallback = None

    # ---------------------------------------------------------------------

    def loadKeys(self):
        cnt= countFiles(self.credentialsPath)
        if (cnt is not self.credentialsNum):
            self.credentialsNum = cnt
            logging.info('Updating Certificates on Location: {0} ({1} Files)'.format(self.credentialsPath, self.credentialsNum))
            self.auth.configure_curve(domain='*', location=self.credentialsPath)


    def __init__(self, clear_connection_string, curve_connection_string, verbose=False, credentials=None, credentialsPath=None, credentialsCallback=None):
        """Initialize broker state."""
        self.verbose = verbose
        logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.INFO)
        self.credentials = credentials
        self.services = {}
        self.workers = {}
        self.waiting = []
        self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL
        self.ctx = zmq.Context()
        self.socketclear = Router(clear_connection_string, self.ctx)
        self.socketcurve = Router(curve_connection_string, self.ctx, keys=self.credentials)
        if (self.credentials is not None):
            # Start an authenticator for this context.
            self.auth = ThreadAuthenticator(self.ctx)
            self.auth.start()
            self.auth.allow('127.0.0.1')
            # Tell authenticator to use the certificate in a directory
            self.credentialsPath = credentialsPath
            self.credentialsCallback = credentialsCallback
            if (self.credentialsCallback is not None):
                self.auth.configure_curve_callback('*', credentials_provider=self.credentialsCallback)
            elif (self.credentialsPath is not None):
                self.loadKeys()
            else:
                self.auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)
        self.poller = zmq.Poller()
        self.poller.register(self.socketclear.socket, zmq.POLLIN)
        self.poller.register(self.socketcurve.socket, zmq.POLLIN)

    # ---------------------------------------------------------------------

    def route(self, socket, clear = None):
       msg = socket.recv()
       if self.verbose:
           logging.info("I: received message:")
           dump(msg)

       sender = msg.pop(0)
       empty = msg.pop(0)
       assert empty == b''
       header = msg.pop(0)

       if (IDP.C_CLIENT == header):
           self.process_client(sender, msg, clear)
       elif (IDP.W_WORKER == header):
           self.process_worker(sender, msg, clear)
       else:
           logging.error("E: invalid message:")
           dump(msg)


    def mediate(self):
        """Main broker work happens here"""
        while True:
            try:
                socks = dict(self.poller.poll(self.HEARTBEAT_INTERVAL))
            except KeyboardInterrupt:
                break # Interrupted
            if (self.socketclear.socket in socks):
                self.route(self.socketclear, True)
            elif (self.socketcurve.socket in socks):
                self.route(self.socketcurve, False)
            self.purge_workers()
            self.send_heartbeats()

    def destroy(self):
        """Disconnect all workers, destroy context."""
        while self.workers:
            self.delete_worker(self.workers.values()[0], True)
        self.ctx.destroy(0)

    def process_client(self, sender, msg, clear = None):
        """Process a request coming from a client."""
        assert len(msg) >= 2 # Service name + body
        service = msg.pop(0)
        # Set reply return address to client sender
        msg = [sender, b''] + msg
        if service.startswith(self.INTERNAL_SERVICE_PREFIX):
            logging.warning('self.INTERNAL_SERVICE_PREFIX')
            self.service_internal(service, msg)
        else:
            self.dispatch(self.require_service(service), msg, clear)

    def process_worker(self, sender, msg, clear):
        """Process message sent to us by a worker."""
        assert len(msg) >= 1 # At least, command

        command = msg.pop(0)

        worker_ready = hexlify(sender) in self.workers

        worker = self.require_worker(sender, clear)

        if (IDP.W_REPLY == command):
            if (worker_ready):
                # Remove & save client return envelope and insert the
                # protocol header and service name, then rewrap envelope.
                client = msg.pop(0)
                empty = msg.pop(0) # ?
                msg = [client, b'', IDP.C_CLIENT, worker.service.name] + msg
                self.socketclear.send(msg)
                self.worker_waiting(worker)
            else:
                logging.warning('IDP.W_REPLY expected, got: {0} from {1}'.format(command, worker.identity))
                self.delete_worker(worker, True)

        elif (IDP.W_REPLY_CURVE == command):
            if (worker_ready):
                # Remove & save client return envelope and insert the
                # protocol header and service name, then rewrap envelope.
                client = msg.pop(0)
                empty = msg.pop(0) # ?
                msg = [client, b'', IDP.C_CLIENT, worker.service.name] + msg
                self.socketcurve.send(msg)
                self.worker_waiting(worker)
            else:
                logging.warning('IDP.W_REPLY expected, got: {0} from {1}'.format(command, worker.identity))
                self.delete_worker(worker, True)



        elif (IDP.W_READY == command):
            assert len(msg) >= 1 # At least, a service name
            service = msg.pop(0)
            # Not first command in session or Reserved service name
            if (worker_ready or service.startswith(self.INTERNAL_SERVICE_PREFIX)):
                logging.warning('Not first command in session or Reserved service name: {0}'.format(worker.identity))
                self.delete_worker(worker, True)
            else:
                # Attach worker to service and mark as idle
                logging.warning('Attaching: {0}'.format(worker.identity))
                worker.service = self.require_service(service)
                self.worker_waiting(worker)

        elif (IDP.W_HEARTBEAT == command):
            if (worker_ready):
                worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
            else:
                logging.warning('Received Heartbeat from unintialized Worker: {0}'.format(worker.identity))
                self.delete_worker(worker, True)

        elif (IDP.W_DISCONNECT == command):
            logging.warning('Received disconnect: {0}'.format(worker.identity))
            self.delete_worker(worker, False)
        else:
            logging.error("E: invalid message:")
            dump(msg)

    def delete_worker(self, worker, disconnect):
        """Deletes worker from all data structures, and deletes worker."""
        assert worker is not None
        if disconnect:
            self.send_to_worker(worker, IDP.W_DISCONNECT, None, None)

        if worker.service is not None:
            worker.service.waiting.remove(worker)
        self.workers.pop(worker.identity)

    def require_worker(self, address, clear):
        """Finds the worker (creates if necessary)."""
        assert (address is not None)
        identity = hexlify(address)
        worker = self.workers.get(identity)
        if (worker is None):
            worker = Worker(identity, address, self.HEARTBEAT_EXPIRY, clear)
            self.workers[identity] = worker
            if self.verbose:
                logging.info("I: registering new worker: %s", identity)

        return worker

    def require_service(self, name):
        """Locates the service (creates if necessary)."""
        assert (name is not None)
        service = self.services.get(name)
        if (service is None):
            service = Service(name)
            self.services[name] = service

        return service

    def bind(self):
        """Bind broker to endpoint, can call this multiple times.

        We use a single socket for both clients and workers.
        """
        self.socketclear.bind()
        self.socketcurve.bind()
        logging.info("I: IDP broker/0.1.1 is active at {0} / {1}".format(self.socketclear.connection_string, self.socketcurve.connection_string))

    def service_internal(self, service, msg):
        """Handle internal service according to 8/MMI specification"""
        returncode = b"501"
        if b"mmi.service" == service:
            name = msg[-1]
            returncode = b"200" if name in self.services else b"404"
        msg[-1] = returncode

        # insert the protocol header and service name after the routing envelope ([client, ''])
        msg = msg[:2] + [IDP.C_CLIENT, service] + msg[2:]
        self.socketclear.send(msg)

    def send_heartbeats(self):
        """Send heartbeats to idle workers if it's time"""
        now =  time.time()
        if (now > self.heartbeat_at):
            if((self.credentialsPath is not None) and (self.credentialsCallback is None)):
               self.loadKeys()
            for worker in self.waiting:
                logging.warning('Heart: {0}'.format(worker.identity))
                self.send_to_worker(worker, IDP.W_HEARTBEAT, None, None)

            self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL

    def purge_workers(self):
        """Look for & kill expired workers.

        Workers are oldest to most recent, so we stop at the first alive worker.
        """
        while self.waiting:
            w = self.waiting[0]
            if w.expiry < time.time():
                logging.warning("I: deleting expired worker: %s", w.identity)
                self.delete_worker(w,False)
                self.waiting.pop(0)
            else:
                break

    def worker_waiting(self, worker):
        """This worker is now waiting for work."""
        # Queue to broker and service waiting lists
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
        self.dispatch(worker.service, None)

    def dispatch(self, service, msg, clear=None):
        """Dispatch requests to waiting workers as possible"""
        assert (service is not None)
        if msg is not None:# Queue message if any
            service.requests.append((msg, clear))
        self.purge_workers()
        while service.waiting and service.requests:
            msg, cl = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            if (cl): 
               self.send_to_worker(worker, IDP.W_REQUEST, None, msg)
            else: 
               self.send_to_worker(worker, IDP.W_REQUEST_CURVE, None, msg)

    def send_to_worker(self, worker, command, option, msg=None):
        """Send message to worker.

        If message is provided, sends that message.
        """

        if msg is None:
            msg = []
        elif not isinstance(msg, list):
            msg = [msg]

        # Stack routing and protocol envelopes to start of message
        # and routing envelope
        if option is not None:
            msg = [option] + msg
        msg = [worker.address, b'', IDP.W_WORKER, command] + msg

        if self.verbose:
            logging.info("I: sending %r to worker", command)
            dump(msg)
        
        if (worker.clear):
            self.socketclear.send(msg)
        else:
            self.socketcurve.send(msg)

