"""
Irondomo Protocol client example. Uses the IDPClient API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""
import os
import sys
from IronDomo  import IDPClient
import time
import begin
import uuid

@begin.start
def main(service='echo', identity='echo'):
    verbose = '-v' in sys.argv
    client = IDPClient.IronDomoClient("tcp://localhost:5000", verbose, identity=identity+uuid.uuid4().hex)
    count = 0
    loop = True
    while loop:
        request = "Hello world {0} -> {1}".format(service, count)
        print(request)
        try:
            reply = client.send(service.encode(), [request.encode(), b'parte2'])
            print(reply)
        except KeyboardInterrupt:
            break
        else:
            # also break on failure to reply:
            if reply is None:
                break
        count += 1
        loop = False
    print("%i requests/replies processed" % count)


