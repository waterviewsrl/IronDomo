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
    server_public_file = os.path.join(public_keys_dir, "server.key")
    print('Server Secret File: {0}'.format(server_public_file))
    server_public, dummy  = zmq.auth.load_certificate(server_public_file)

    print('Server Key: {0}'.format(server_public))

    client = IDPClient.IronDomoClient("tcp://127.0.0.1:5556", verbose, ('P+S690P{iVPfx<aFJwxfSY^ugFzjuWOnaIh!o7J<', client_public, client_secret))
    count = 0
    while count < 100:
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
    client.close()
    print("%i requests/replies processed" % count)

if __name__ == '__main__':
    main()

