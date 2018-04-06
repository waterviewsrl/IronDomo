"""Irondomo Protocol worker example.
Uses the IDPWorker API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""

import sys
from IronDomo import IDPWorker

def main():
    verbose = '-v' in sys.argv
    worker = IDPWorker.IronDomoWorker("tcp://localhost:5555", b"echo", verbose)
    reply = None
    while True:
        request = worker.recv(reply)
        #print('Request: {}'.format(request))
        if request is None:
            break # Worker was interrupted
        reply = [b'ECHOOOO: ' + request[0]] # Echo is complex... :-)


if __name__ == '__main__':
    main()
