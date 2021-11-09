"""Irondomo Protocol worker example.
Uses the IDPWorker API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""

import sys
import time
from IronDomo import IDPWorker
import logging
import begin

class Workload(object):
    pre = None

    def __init__(self):
        self.pre = 'ECHOXXX'
        self.count = 0

    def do(self, request):
        logging.warning('Message: {0}'.format(len(request[0])))
        #time.sleep(1)
        reply = [] 
        #for part in request:
        #    reply.append(part) # Echo is complex... :-)
        return [b'OK']

@begin.start
def main(clear_url='tcp://localhost:6555', service='echo'):
    verbose = '-v' in sys.argv
    workload = Workload()
    worker = IDPWorker.IronDomoWorker(clear_url, service.encode(), verbose, workload= workload)

    worker.loop()

