# encoding: utf-8
"""
Helper module for example applications. Mimics ZeroMQ Guide's zhelpers.h.
"""
from __future__ import print_function

import binascii
import os
from random import randint

import zmq

from zmq.utils.monitor import recv_monitor_message
from zmq.auth.thread import ThreadAuthenticator
import logging
import threading 


def socket_set_hwm(socket, hwm=-1):
    """libzmq 2/3/4 compatible sethwm"""
    try:
        socket.sndhwm = socket.rcvhwm = hwm
    except AttributeError:
        socket.hwm = hwm


def dump(msg_or_socket):
    """Receives all message parts from socket, printing each frame neatly"""
    if isinstance(msg_or_socket, zmq.Socket):
        # it's a socket, call on current message
        msg = msg_or_socket.recv_multipart()
    else:
        msg = msg_or_socket
    print("----------------------------------------")
    for part in msg:
        print("[%03d]" % len(part), end=' ')
        part = part.buffer.tobytes() if isinstance(part, zmq.sugar.frame.Frame) else part
        try:
            print(part.decode('ascii'))
        except UnicodeDecodeError:
            print(r"0x%s" % (binascii.hexlify(part).decode('ascii')))


def set_id(zsocket):
    """Set simple random printable identity on socket"""
    identity = u"%04x-%04x" % (randint(0, 0x10000), randint(0, 0x10000))
    zsocket.setsockopt_string(zmq.IDENTITY, identity)


def zpipe(ctx):
    """build inproc pipe for talking to threads
    mimic pipe used in czmq zthread_fork.
    Returns a pair of PAIRs connected via inproc
    """
    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a,b


class ZSocket:
    def __init__(self, connection_string, ctx=None, keys=None, server_key=None):

        (self._ctx, self._own) = (zmq.Context.instance(), True) if ctx is None else (ctx, False)

        self._curve_keys = None
        self._server_key = None
 
        self._monitor = None
        self._monitor_thread = None

        self._connection_string = connection_string
        self._socket = None

        self.curve_keys = keys
        self.server_key = server_key

        self._EVENT_MAP = {}
        for name in dir(zmq):
            if name.startswith('EVENT_'):
                value = getattr(zmq, name)
                self._EVENT_MAP[value] = name

    def __del__(self):
        if (self._socket != None):
            self._socket.disable_monitor()

    def close(self):
        logging.warning('Disabling Monitor')
        self._monitor_active = False
        self._socket.disable_monitor()
        self._monitor_thread.join()
        self._socket.close()
        self._socket = None
        
        logging.warning('Closing Monitor')

        if(self._own):
            self._ctx.term()
            self._ctx = None

    @property
    def ctx(self):
        return self._ctx

    @property
    def curve_keys(self):
        return self._curve_keys

    @curve_keys.setter
    def curve_keys(self, keys):
        logging.warning('KEYS: {0}'.format(keys))
        if keys is not None:
            public, secret = keys
            if isinstance(public, str):
                public = public.encode()
            if isinstance(secret, str):
                secret = secret.encode()
            self._curve_keys = public, secret
            logging.info('HOST CURVE keys added')
        else:
            logging.info('No HOST CURVE keys added.')

    @property
    def server_key(self):
        return self._server_key

    @server_key.setter
    def server_key(self, key):
        if key is not None:
            if isinstance(key, str):
                key = key.encode()
            self._server_key = key
            logging.info('SERVER CURVE key added')
        else:
            logging.info('No SERVER CURVE key added.')

    @property
    def socket(self):
        return self._socket


    @property
    def connection_string(self):
        return self._connection_string

    def setup_curve(self, curve_keys, server_key):
        self.curve_keys = curve_keys
        self.server_key = server_key
        self._attach_curve_keys()

    def _attach_curve_keys(self):
        if self.curve_keys is not None:
            self.socket.curve_publickey, self.socket.curve_secretkey = self.curve_keys
            logging.warning('Attaching HOST CURVE keys.')
        if self.server_key is not None:
            self.socket.curve_serverkey = self.server_key
            logging.warning('Attaching SERVER CURVE key.')

    def create(self, zocket_type):
        self._socket = self.ctx.socket(zocket_type)
        self._socket.linger = 1
        self._attach_curve_keys()
        self._monitor = self._socket.get_monitor_socket()
        self._monitor_poller =  zmq.Poller()
        self._monitor_poller.register(self._monitor, zmq.POLLIN)

        self._monitor_thread = threading.Thread(target=self.event_monitor, daemon=True)
        self._monitor_active = True
        self._monitor_thread.start()

    def event_monitor(self):
        while self._monitor_active:
            socks = dict(self._monitor_poller.poll(timeout=100))
            if self._monitor in socks and socks[self._monitor] == zmq.POLLIN: 
                evt = recv_monitor_message(self._monitor)
                try:
                    evt.update({'description': self._EVENT_MAP[evt['event']]})
                except Exception as e:
                    evt.update({'description': 'Unknown Error'}) 
                logging.warning("Event Monitor(Host: {0}): {1}".format(self._connection_string, evt))
                if evt['event'] == zmq.EVENT_MONITOR_STOPPED:
                    break
        self._monitor.close()
        logging.warning("Event Monitor thread done!")


    def connect(self):
        try:
            self.socket.connect(self.connection_string)
        except zmq.error.ZMQError as e:
            print('Error: {} - Connection String: {}'.format(e, self.connection_string))

    def bind(self):
        if self.curve_keys is not None:
            self.socket.curve_server = True
        self.socket.bind(self.connection_string)

    def publish(self, channel, data):
        key = channel
        message = data
        logging.info('publish: {0}'.format(channel, data))
        if not isinstance(key, bytes):
            key = key.encode('utf-8')
        #if not isinstance(message, bytes):
        #    message = message.encode('utf-8')

        self.socket.send_multipart([key]+message)

    def subscribe(self, channel):
         self.socket.setsockopt(zmq.SUBSCRIBE, channel.encode())

    def unsubscribe(self, channel):
         self.socket.setsockopt(zmq.UNSUBSCRIBE, channel.encode())

    def send(self, data, copy=False):
        self.socket.send_multipart(data, copy=copy)

    def recv(self, copy=True):
        message = self.socket.recv_multipart(copy=copy)   

        return message
    
    def put(self, data):
        message = data
        if not isinstance(message, bytes):
            message = message.encode('utf-8')

        self.socket.send(message)

    def get(self):
        message = self.socket.recv()

        return message


class Dealer(ZSocket):
    def __init__(self, connection_string, ctx=None, keys=None, server_key=None, identity=None):

        super().__init__(connection_string=connection_string , ctx=ctx, keys=keys, server_key=server_key)

        self.create(zmq.DEALER)
        
        if identity is not None:
           self.socket.setsockopt(zmq.IDENTITY, identity.encode())

        logging.warning('Dealer on "{}"'.format(self.connection_string))

class Router(ZSocket):
    def __init__(self, connection_string, ctx=None, keys=None, server_key=None):

        super().__init__(connection_string=connection_string, ctx=ctx, keys=keys, server_key=server_key)

        self.create(zmq.ROUTER)

        logging.warning('Router on "{}"'.format(self.connection_string))

class Req(ZSocket):
    def __init__(self, connection_string, ctx=None, keys=None, server_key=None, identity=None):

        super().__init__(connection_string=connection_string , ctx=ctx, keys=keys, server_key=server_key)

        self.create(zmq.REQ)

        if identity is not None:
           self.socket.setsockopt(zmq.IDENTITY, identity.encode())

        logging.warning('Req on "{}"'.format(self.connection_string))

class Publisher(ZSocket):
    def __init__(self, connection_string, ctx=None, keys=None, server_key=None):

        super().__init__(connection_string=connection_string, ctx=ctx, keys=keys, server_key=server_key)

        self.create(zmq.PUB)

        logging.warning('Publisher on "{}"'.format(self.connection_string))

class Subscriber(ZSocket):
    def __init__(self, connection_string, ctx=None, keys=None, server_key=None):

        super().__init__(connection_string=connection_string, ctx=ctx, keys=keys, server_key=server_key)

        self.create(zmq.SUB)

        logging.warning('Subscriber on "{}"'.format(self.connection_string))

class CurveAuthenticator(object):
    def __init__(self, ctx, domain='*', location=zmq.auth.CURVE_ALLOW_ANY, callback = None):
        
        self._domain = domain
        self._location = location
        self._callback = callback
        self._ctx = ctx
        self._atx = ThreadAuthenticator(self.ctx)
        self._atx.start()
        if (self._callback is not None):
            logging.info('Callback: {0}'.format(self._callback))
            self._atx.configure_curve_callback('*', credentials_provider=self._callback)
        elif (self._location == zmq.auth.CURVE_ALLOW_ANY or self._location is None):
            self._atx.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)
        else:
            self.load_certs()
        

    @property
    def atx(self):
        return self._atx

    @property
    def location(self):
        return self._location

    @property
    def domain(self):
        return self._domain

    @property
    def ctx(self):
        return self._ctx

    def load_certs(self):
        self.atx.configure_curve(domain=self._domain, location=self._location)
