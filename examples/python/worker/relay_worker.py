"""Irondomo Protocol worker example.
Uses the IDPWorker API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""

import os
import sys
import time
from IronDomo import IDPWorker, IDPClient
from IronDomo.IDPHelpers import Subscriber
import logging
import begin
import threading
import zmq

class Workload(object):
    """Relay Workload class, managin CLI commands
    
    """

    def __init__(self,  publisher_connection_url):
        self.ctx = zmq.Context()
        self.control_socket = self.ctx.socket(zmq.PAIR)
        self.control_socket.bind('inproc://control')
        self.sub_thread = SubscriberThread(publisher_connection_url=publisher_connection_url, ctx=self.ctx)
        self.sub_thread.daemon = True
        self.sub_thread.start()

    def do(self, request):
        """IronDomo callback
        It checks CLI commands syntax and forwards commands to the Subscriber on the control_socket
        
        Arguments:
            request {list of bytesobjects} -- the message body parts
        
        Returns:
            list of bytesobjects -- the raply body parts
        """
        
        if request[0] in [b'Start',  b'Stop', b'Pause', b'Control']:
            self.control_socket.send(request[0])
            res = self.control_socket.recv_multipart()
            reply = res 
        elif request[0] == b'Enqueue' and request[1].decode().isdigit() and len(request) == 3:
            self.control_socket.send_multipart(request)
            res = self.control_socket.recv_multipart()
            reply = res 
        elif request[0] in [b'Subscribe', b'Unsubscribe']  and len(request) == 2:
            self.control_socket.send_multipart(request)
            res = self.control_socket.recv_multipart()
            reply = res
        else:
            logging.error('Wrong Command!')
            reply = ['ERROR: Wrong Command: {0}'.format(request).encode()] 

        return reply


class RelayRoundRobin():
    """An helper class to hold a Round Robin thread and sockets pool
    
    """
    def __init__(self):
        self.relay_list = []
        self.len = 0
        self.pos = 0

    def add(self, relay_thread, relay_queue_scoket, relay_control_scoket):
        """Adds a thread, queue socket and control socket to the pool
        
        Arguments:
            relay_thread {[type]} -- [description]
            relay_queue_scoket {[type]} -- [description]
            relay_control_scoket {[type]} -- [description]
        """
        self.relay_list.append((relay_thread, relay_queue_scoket, relay_control_scoket))
        self.pos = 0
        self.len = len(self.relay_list)

    def pop(self):
        """Pops an element from the pool list
        
        Returns:
            tuple -- a tuple containing  popped thread, queue and control sockets
        """
        ret = self.relay_list.pop()
        self.pos = 0
        self.len = len(self.relay_list)  
        return ret

    def next(self):
        """Gets next thread and sockets in the pool on a Round Robin scheme
        
        Returns:
            tuple -- a tuple containing thread, queue and control sockets
        """
        ret = (None, None, None)
        if self.len != 0:
            ret = self.relay_list[self.pos]
            self.pos = (self.pos + 1)   
            self.pos = int(int(self.pos)%int(self.len))   
        return ret


class RelayThread(threading.Thread):
    """Thread Managing queues and communication with the target broker
    

    """
    def __init__(self, target_connection_url, label, ctx=None):
        """Relay Thread contructor
        
        Arguments:
            target_connection_url {string} -- Target broker URL
            label {string} -- label ID of the thread
        
        Keyword Arguments:
            ctx {zmq context} -- The zeromq context (default: {None})
        """
        threading.Thread.__init__(self)
        logging.warning('coso: '+target_connection_url+label)
        self.target_connection_url = target_connection_url
        self.label = label
        self.ctx = ctx#zmq.Context()
        self.client = IDPClient.IronDomoClient(self.target_connection_url,  False, identity='RelayThread_{0}'.format(self.label), ctx=self.ctx)
        self.relay_queue_socket = self.ctx.socket(zmq.PAIR)
        self.relay_queue_socket.connect('inproc://relay_queue_{0}'.format(self.label))
        self.relay_control_socket = self.ctx.socket(zmq.PAIR)
        self.relay_control_socket.connect('inproc://relay_control_{0}'.format(self.label))
        self.poller = zmq.Poller()
        self.poller.register(self.relay_queue_socket, zmq.POLLIN)
        self.poller.register(self.relay_control_socket, zmq.POLLIN)
        self.queue = []
        self.polltime = 1000
        self.active = False
        self.cont = True



    def realy_enqueue(self):
        """Appends messages to relay queue
        """
        msg = self.relay_queue_socket.recv_multipart()
        logging.info('realy_enqueue :{0}'.format(msg) )
        self.queue.append(msg)

    def relay_control(self):
        """Manages commands from CLI
        """
        msg = self.relay_control_socket.recv_multipart()
        logging.info('relay_control :{0}'.format(msg))
        cmd = msg[0].decode()

        if cmd == 'Control':
            res = len(self.queue)
            self.relay_control_socket.send_multipart([str(res).encode(), b'OK!'])
        elif cmd == 'Stop':
            self.cont = False
            self.relay_control_socket.send_multipart([b'Stopping', b'OK!'])
        elif cmd == 'Start':
            self.active = True
            self.relay_control_socket.send_multipart([b'Starting', b'OK!'])
        elif cmd == 'Pause':
            self.active = False
            self.relay_control_socket.send_multipart([b'Pausing', b'OK!'])

    def run(self):
        """Main Relay Thread callback. Pollimg sockets for messages nad commands.
        Polling time is 1000 ms by default, reduced to 100 as soon as Messages need to be sent to target broker
        """
        while self.cont:
            logging.info('running')
            try:
                socks = dict(self.poller.poll(self.polltime))
            except KeyboardInterrupt:
                break # Interrupted
            if (self.relay_queue_socket in socks):
                self.realy_enqueue()
            if (self.relay_control_socket in socks):
                self.relay_control()
            else:
                logging.info('No New MSG in queue {0}. Size: {1}'.format(self.label, len(self.queue)))
            self.polltime = 1000    
            if self.active:
                if len(self.queue) > 0:
                    self.polltime = 100
                    el = self.queue[0]
                    service = el[0]
                    msg = el[3:]
                    try: 
                        rep = self.client.send(service, msg) 
                        self.queue.pop(0)
                    except Exception as e:
                        logging.error('Error on Relay Broker: {0}'.format(e))

            
        self.client.client.close()
            
        return

class SubscriberThread(threading.Thread):
    """Subscriber thread responsible of retrieving desired messages from the source broker.
    Spawns relay threads, and sends them CLI commands as well messages, according to a round robin strategy
    
    """
    def __init__(self, publisher_connection_url='tcp://localhost:5557', ctx=None):
        """Subscriber thread constructor
        
        Keyword Arguments:
            publisher_connection_url {str} -- the source broker publisher URL (default: {'tcp://localhost:5557'})
            ctx {ZeroMQ context} -- the zeroMQ context (default: {None})
        """
        threading.Thread.__init__(self)
        self.ctx = ctx
        self.subscriber_socket = Subscriber(publisher_connection_url, self.ctx)
        self.subscriber_socket.connect()
        self.control_socket = self.ctx.socket(zmq.PAIR)
        self.control_socket.connect('inproc://control')
        self.relay_round_robin = RelayRoundRobin()
        self.poller = zmq.Poller()
        self.poller.register(self.subscriber_socket._socket, zmq.POLLIN)
        self.poller.register(self.control_socket, zmq.POLLIN)
        
        
    def control(self):
        """Manages control messages from CLI
        
        Returns:
            string -- Status of the CLI command 
        """
        msg = self.control_socket.recv_multipart()
        logging.info('Received: {0}'.format(msg))
        num = 0
        target = ''
        cmd = []
        for c in msg:
            cmd.append(c.decode())
        ret = 'Error! Unknown command in Control! {0}'.format(cmd[0])

        if cmd[0] == 'Subscribe':
            self.subscriber_socket.subscribe(cmd[1])
            ret = 'OK, Subscribing: {0}'.format(cmd[1])
        elif cmd[0] == 'Unsubscribe':
            self.subscriber_socket.unsubscribe(cmd[1])
            ret = 'OK, Unsubscribing: {0}'.format(cmd[1])
        elif cmd[0] == 'Control' or cmd[0] == 'Start' or cmd[0] == 'Pause':
            ret = []
            for i in range(self.relay_round_robin.len):
                (r, qs, cs) = self.relay_round_robin.relay_list[i]
                cs.send_multipart([cmd[0].encode()])
            for i in range(self.relay_round_robin.len):
                (r, qs, cs) = self.relay_round_robin.relay_list[i] 
                ret.append(cs.recv_multipart())  
            ret = str(ret)
        elif cmd[0] == 'Stop':
            ret = []
            for i in range(self.relay_round_robin.len):
                (r, qs, cs) = self.relay_round_robin.relay_list[i]
                cs.send_multipart([cmd[0].encode()]) 
            for i in range(self.relay_round_robin.len):
                (r, qs, cs) = self.relay_round_robin.relay_list[i]
                ret.append(cs.recv_multipart())    
                cs.close()
                qs.close()
                r.join()
            self.relay_round_robin = RelayRoundRobin()      
            ret = str(ret)
        elif cmd[0] == 'Enqueue':
            if self.relay_round_robin.len > 0:
                ret = 'Error! Round Robin already active on {0} threads!'.format(self.relay_round_robin.len)
            else:
                for i in range(int(cmd[1])):
                    qs = self.ctx.socket(zmq.PAIR)
                    qs.bind('inproc://relay_queue_{0}'.format(i))
                    cs = self.ctx.socket(zmq.PAIR)
                    cs.bind('inproc://relay_control_{0}'.format(i))
                    r = RelayThread(cmd[2], str(i), self.ctx)
                    r.daemon = True
                    r.start()
                    self.relay_round_robin.add(r, qs, cs)
                ret = 'OK, Started {0} Threads'.format(cmd[1])
        return ret

    def route(self):
        """If thread pool has been created, routes messages to the relay threads according to a round robin strategy 
        """
        msg = self.subscriber_socket.recv()
        logging.info('Routing: {0}'.format(msg))
        (relay, queue_socket, control_socket) = self.relay_round_robin.next()
        if (queue_socket != None):
            queue_socket.send_multipart(msg)


    def run(self):
        """Main thread callback. Polls sockets for messages or commands from CLI
        """
        cont = True
        while cont:
            logging.info('running')
            try:
                socks = dict(self.poller.poll(1000))
            except KeyboardInterrupt:
                break # Interrupted
            if (self.subscriber_socket._socket in socks):
                self.route()
            elif (self.control_socket in socks):
                res = self.control().encode()
                self.control_socket.send_multipart([res])
            else:
                logging.info('No MSG on sockets')
            
        return


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

@begin.start
def main(source_url='tcp://localhost:5555',  publisher_connection_url='tcp://localhost:5557'):
    verbose = '-v' in sys.argv
    workload = Workload(publisher_connection_url=publisher_connection_url)
    worker = IDPWorker.IronDomoWorker(source_url, b'relay', verbose, workload=workload)

    worker.loop()

