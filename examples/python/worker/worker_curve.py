"""Irondomo Protocol worker example.
Uses the IDPWorker API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""

import os 
import sys
import zmq.auth
from IronDomo import IDPWorker

class Workload(object):
    pre = None

    def __init__(self):
        self.pre = 'ECHOXXX'

    def do(self, request):
        reply = [] 
        for part in request:
            reply.append(self.pre.encode() + b" " + part) # Echo is complex... :-)
        return reply


def main():
    verbose = '-v' in sys.argv
    base_dir = os.path.dirname(__file__)
    keys_dir = os.path.join(base_dir, 'certificates')
    public_keys_dir = os.path.join(base_dir, 'public_keys')
    secret_keys_dir = os.path.join(base_dir, 'private_keys')
    client_secret_file = os.path.join(secret_keys_dir, "worker.key_secret")
    client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
    server_public_file = os.path.join(public_keys_dir, "server.key")
    print('Server Secret File: {0}'.format(server_public_file))
    server_public, dummy  = zmq.auth.load_certificate(server_public_file)

    print('Server Key: {0}'.format(server_public))
 
    workload = Workload()

    worker = IDPWorker.IronDomoWorker("tcp://localhost:5556", b"echo", verbose, (server_public, client_public, client_secret), workload=workload)

    worker.loop()


if __name__ == '__main__':
    main()
