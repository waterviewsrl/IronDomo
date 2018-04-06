"""Irondomo Protocol worker example.
Uses the IDPWorker API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""

import os 
import sys
import zmq.auth
from IronDomo import IDPWorker

def main():
    verbose = '-v' in sys.argv
    base_dir = os.path.dirname(__file__)
    keys_dir = os.path.join(base_dir, 'certificates')
    public_keys_dir = os.path.join(base_dir, 'public_keys')
    secret_keys_dir = os.path.join(base_dir, 'private_keys')
    client_secret_file = os.path.join(secret_keys_dir, "worker.key_secret")
    client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
    server_secret_file = os.path.join(secret_keys_dir, "server.key_secret")
    print('Server Secret File: {0}'.format(server_secret_file))
    server_public, server_secret = zmq.auth.load_certificate(server_secret_file)

    print('Server Keys: {0} ||| {1}'.format(server_public, server_secret))

    worker = IDPWorker.IronDomoWorker("tcp://localhost:5556", b"echo", verbose, (server_public, client_public, client_secret))
    reply = None
    while True:
        request = worker.recv(reply)
        #print('Request: {}'.format(request))
        if request is None:
            break # Worker was interrupted
        reply = [b'ECHOOOO: ' + request[0]] # Echo is complex... :-)


if __name__ == '__main__':
    main()
