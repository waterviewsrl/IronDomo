"""
Majordomo Protocol broker
A minimal implementation of http:#rfc.zeromq.org/spec:7 and spec:8

Author: Min RK <benjaminrk@gmail.com>
Based on Java example by Arkadiusz Orzechowski
"""

import os
import sys
import logging
import zmq.auth

from IronDomo import IDPBroker

def main():
    """create and start new broker"""
    verbose = '-v' in sys.argv
    # These directories are generated by the generate_certificates script
    base_dir = os.path.dirname(__file__)
    keys_dir = os.path.join(base_dir, 'certificates')
    public_keys_dir = os.path.join(base_dir, 'public_keys')
    secret_keys_dir = os.path.join(base_dir, 'private_keys')


    server_secret_file = os.path.join(secret_keys_dir, "server.key_secret")
    print('Server Secret File: {0}'.format(server_secret_file))
    server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
    print('Server Keys: {0} ||| {1}'.format(server_public, server_secret))

    print('public_keys_dir: {0}'.format(public_keys_dir))
    broker = IDPBroker.IronDomoBroker(verbose, (server_public, server_secret), public_keys_dir)
    #broker = IDPBroker.IronDomoBroker(verbose, (server_public, server_secret))
    broker.bind("tcp://*:5555", "tcp://*:5556")
    broker.mediate()

if __name__ == '__main__':
    main()
