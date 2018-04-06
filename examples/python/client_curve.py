"""
Irondomo Protocol client example. Uses the IDPClient API to hide all IDP aspects
Author: Matteo Ferrabone <matteo.ferrabone@gmail.com> 
"""
import os
import sys
import zmq.auth
from IronDomo  import IDPClient

def main():
    verbose = '-v' in sys.argv
    base_dir = os.path.dirname(__file__)
    keys_dir = os.path.join(base_dir, 'certificates')
    public_keys_dir = os.path.join(base_dir, 'public_keys')
    secret_keys_dir = os.path.join(base_dir, 'private_keys')
    client_secret_file = os.path.join(secret_keys_dir, "client.key_secret")
    client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
    server_secret_file = os.path.join(secret_keys_dir, "server.key_secret")
    server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
    client = IDPClient.IronDomoClient("tcp://localhost:5556", verbose, (server_public, client_public, client_secret))
    count = 0
    while count < 100000:
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

