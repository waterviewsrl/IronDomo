"""
Irondomo Protocol client example. Uses the IDPClient API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""
import os
import sys
from IronDomo  import IDPClient

def main():
    verbose = '-v' in sys.argv
    client = IDPClient.IronDomoClient("tcp://localhost:5555", verbose)
    count = 0
    while count < 10000:
        request = "Hello world 1 -> {0}".format(count)
        try:
            reply = client.send(b"echo", request.encode())
            print(reply)
        except KeyboardInterrupt:
            break
        else:
            # also break on failure to reply:
            if reply is None:
                break
        count += 1
    print("%i requests/replies processed" % count)

if __name__ == '__main__':
    main()

