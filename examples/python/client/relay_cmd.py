"""
Irondomo Protocol client example. Uses the IDPClient API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""
import os
import sys
from IronDomo  import IDPClient
import time
import begin

@begin.start
def main(source_url, command, service='relay', identity='relay-cmd'):
    verbose = '-v' in sys.argv
    client = IDPClient.IronDomoClient(source_url, verbose, identity=identity)
    count = 0
    command = command.split(';')
    request = []
    for c in command:
        request.append(c.encode())
    print(request)
    try:
        reply = client.send(service.encode(), request)
        print(reply)
    except KeyboardInterrupt:
        pass 
    else:
        # also break on failure to reply:
        if reply is None:
            pass
    count += 1
    print("%i requests/replies processed" % count)


