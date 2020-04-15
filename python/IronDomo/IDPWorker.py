"""Irondomo Protocol Worker API, Python version
Implements the IDP/Worker 
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com>
"""

import logging
import time
import zmq
import os
from base64 import b64encode

from IronDomo.IDPHelpers import dump
from IronDomo.IDPHelpers import Dealer
# IronDomo protocol constants:
from IronDomo import IDP

class IronDomoWorker(object):
    """Irondomo Protocol Worker API, Python version
    """

    HEARTBEAT_LIVENESS = 3 # 3-5 is reasonable
    broker = None
    ctx = None
    service = None

    worker = None # Socket to broker
    heartbeat_at = 0 # When to send HEARTBEAT (relative to time.time(), so in seconds)
    liveness = 0 # How many attempts left
    heartbeat = 2500 # Heartbeat delay, msecs
    reconnect = 10000 # Reconnect delay, msecs

    # Internal state
    expect_reply = False # False only at start

    timeout = 2500 # poller timeout
    verbose = False # Print activity to stdout
    credentials = None
    callback = None

    # Return address, if any
    reply_to_clear = None
    reply_to_curve = None

    def __init__(self, broker, service, verbose=False, credentials=None, workload = None, idle_timeout = None, unique=False):
        self.alive = True
        self.last_message = time.time() 
        self.broker = broker
        self.service = service
        self.unique = unique
        self.identity = '{0}_{1}'.format(self.service.decode(), 0 if unique else b64encode(os.urandom(3)).decode())
        self.verbose = verbose
        self.ctx = zmq.Context()
        self.poller = zmq.Poller()
        logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                level=logging.INFO)
        self.credentials = credentials
        self.workload = workload
        self.idle_timeout = idle_timeout
        self.reconnect_to_broker()
        logging.info("Loggin: {0}".format(self.identity))


    def loop(self):
        reply = None
        while True:
            request = self.recv(reply)
            if request is None:
                break # Worker was interrupted
            reply = self.workload.do(request)

        self.worker.close()

        logging.warning('Exiting service: {0}'.format(self.service))

    def reconnect_to_broker(self):
        """Connect or reconnect to broker"""
        if self.worker:
            self.poller.unregister(self.worker.socket)
            self.worker.close()
        self.worker = Dealer(self.broker, ctx=self.ctx, identity=self.identity)
        self.worker._socket.setsockopt(zmq.RECONNECT_IVL, 250)
        self.worker._socket.setsockopt(zmq.RECONNECT_IVL_MAX, 2500)
        if (self.credentials is not None):
            self.worker.setup_curve((self.credentials[1], self.credentials[2]), self.credentials[0])
        self.worker.connect()
        logging.warning("I: After CONNECT ({0})".format(self.credentials))
        self.poller.register(self.worker.socket, zmq.POLLIN)
        if self.verbose:
            logging.info("I: connecting to broker at %s...", self.broker)

        # Register service with broker
        self.send_to_broker(IDP.W_READY, self.service, [])

        # If liveness hits zero, queue is considered disconnected
        self.liveness = self.HEARTBEAT_LIVENESS
        self.heartbeat_at = time.time() + 1e-3 * self.heartbeat
        self.last_message = time.time()

    def send_to_broker(self, command, option=None, msg=None):
        """Send message to broker.
        If no msg is provided, creates one internally
        """
        if msg is None:
            msg = []
        elif not isinstance(msg, list):
            msg = [msg]

        if option:
            msg = [option] + msg

        msg = [b'', IDP.W_WORKER, command] + msg
        if self.verbose:
            logging.info("I: sending %s to broker", command)
            dump(msg)
        self.worker.send(msg)


    def recv(self, reply=None):
        """Send reply, if any, to broker and wait for next request."""
        # Format and send the reply if we were provided one
        assert reply is not None or not self.expect_reply

        if reply is not None:
            if (self.reply_to_clear is not None):
                reply = [self.reply_to_clear, b''] + reply
                self.send_to_broker(IDP.W_REPLY, msg=reply)
            elif (self.reply_to_curve is not None):
                reply = [self.reply_to_curve, b''] + reply
                self.send_to_broker(IDP.W_REPLY_CURVE, msg=reply)
            else:
                logging.error('Missing Reply!')
            
        self.expect_reply = True

        while True:
            # Poll socket for a reply, with timeout
            try:
                items = self.poller.poll(self.timeout)
            except KeyboardInterrupt:
                break # Interrupted

            if items:
                msg = self.worker.recv()
                if self.verbose:
                    logging.info("I: received message from broker: ")
                    dump(msg)

                self.liveness = self.HEARTBEAT_LIVENESS
                # Don't try to handle errors, just assert noisily
                assert len(msg) >= 3

                empty = msg.pop(0)
                assert empty == b''

                header = msg.pop(0)
                assert header == IDP.W_WORKER

                command = msg.pop(0)
                if command == IDP.W_REQUEST:
                    # We should pop and save as many addresses as there are
                    # up to a null part, but for now, just save one...
                    self.reply_to_clear = msg.pop(0)
                    self.reply_to_curve = None 
                    # pop empty
                    empty = msg.pop(0)
                    assert empty == b''
                    
                    self.last_message = time.time()
                    
                    return msg # We have a request to process
                elif command == IDP.W_REQUEST_CURVE:
                    # We should pop and save as many addresses as there are
                    # up to a null part, but for now, just save one...
                    self.reply_to_clear = None 
                    self.reply_to_curve = msg.pop(0)
                    # pop empty
                    empty = msg.pop(0)
                    assert empty == b''

                    self.last_message = time.time()

                    return msg # We have a request to process
                elif command == IDP.W_HEARTBEAT:
                    logging.info('Received W_HEARTBEAT: {0}'.format(self.service))
                    # Do nothing for heartbeats
                    pass
                elif command == IDP.W_DISCONNECT:
                    logging.warning('Received W_DISCONNECT!')
                    self.reconnect_to_broker()
                else :
                    logging.error("E: invalid input message: ")
                    dump(msg)

            else:
                self.liveness -= 1
                if self.liveness == 0:
                    logging.warn("Disconnected from broker - retrying...")
                    try:
                        if self.worker:
                            self.poller.unregister(self.worker.socket)
                            self.worker.close()
                            self.worker = None
                        time.sleep(1e-3*self.reconnect)
                    except KeyboardInterrupt:
                        break
                    self.reconnect_to_broker()

            if self.idle_timeout:
                now = time.time()
                if now - self.last_message >= self.idle_timeout:
                    self.alive = False
                    logging.warning('Worker has been idle for too much time')
                    break

            # Send HEARTBEAT if it's time
            if time.time() > self.heartbeat_at:
                self.send_to_broker(IDP.W_HEARTBEAT)
                self.heartbeat_at = time.time() + 1e-3*self.heartbeat

        logging.warn("W: interrupt received, killing worker...")
        return None


    def destroy(self):
        # context.destroy depends on pyzmq >= 2.1.10
        self.ctx.destroy(0)
