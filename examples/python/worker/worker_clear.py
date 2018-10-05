"""Irondomo Protocol worker example.
Uses the IDPWorker API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""

import sys
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
    workload = Workload()
    worker = IDPWorker.IronDomoWorker("tcp://localhost:5555", b"echo", verbose, workload= workload, idle_timeout=10)

    worker.loop()


if __name__ == '__main__':
    main()
