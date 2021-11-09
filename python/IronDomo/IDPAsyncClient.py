"""Irondomo Protocol Client API, Python version.
Implements the IDP/Worker.
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com>
"""

import logging

import zmq

from IronDomo import IDP

from IronDomo.IDPHelpers import dump
from IronDomo.IDPHelpers import Dealer


class IronDomoAsyncClient(object):
    """Irondomo Protocol Client API, Python version.
    """
    #broker = None
    #ctx = None
    #client = None
    #poller = None
    #timeout = 2500
    retries = 3
    verbose = True
    credentials = None

    def __init__(self, broker, verbose=False, credentials=None, identity=None, ctx=None, timeout=2500):
        self.timeout = timeout
        self.client = None
        self.conncnt = 0
        self.broker = broker
        self.verbose = verbose
        self.identity = identity
        self.ctx = zmq.Context() if ctx == None else ctx
        self.poller = zmq.Poller()
        self.credentials = credentials
        logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                            level=logging.INFO)
        self.reconnect_to_broker()

    def reconnect_to_broker(self):
        """Connect or reconnect to broker"""
        if self.client:
            self.poller.unregister(self.client.socket)
            self.client.close()
        self.client = Dealer(self.broker, ctx=self.ctx,
                          identity="{0}_{1}".format(self.identity, self.conncnt))
        self.conncnt = self.conncnt + 1
        if (self.credentials is not None):
            self.client.setup_curve(
                (self.credentials[1], self.credentials[2]), self.credentials[0])
        self.client.connect()
        self.poller.register(self.client.socket, zmq.POLLIN)
        if self.verbose:
            logging.info("I: connecting to broker at %s...", self.broker)

    def send(self, service, request):
        """Send request to broker and get reply by hook or crook.
        Takes ownership of request message and destroys it when sent.
        Returns the reply message or None if there was no reply.
        """
        if not isinstance(request, list):
            request = [request]
        request = [b'', IDP.C_CLIENT, service] + request
        if self.verbose:
            logging.info("I: send request to '%s' service: ", service)
            dump(request)

        self.client.send(request)

    def recv(self):

        try:
            items = self.poller.poll(self.timeout)
        except KeyboardInterrupt:
            return None  # interrupted

        if items:
            msg = self.client.recv()
            if self.verbose:
                logging.info("I: received reply:")
                dump(msg)

            # Don't try to handle errors, just assert noisily
            assert len(msg) >= 4

            msg.pop(0)
            header = msg.pop(0)
            assert IDP.C_CLIENT == header

            msg.pop(0)  # popping reply service
            reply = msg
        else:
            reply = None

        return reply

    def close(self):
        self.client.close()
        self.ctx.destroy()
