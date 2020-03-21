"""

   Pubsub envelope subscriber

   Author: Guillaume Aubert (gaubert) <guillaume(dot)aubert(at)gmail(dot)com>

"""
import zmq

def main():
    """ main method """

    # Prepare our context and publisher
    context    = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://localhost:5557")
    subscriber.setsockopt(zmq.SUBSCRIBE, b"echo1")
    subscriber.setsockopt(zmq.SUBSCRIBE, b"relay")

    while True:
        # Read envelope with address
        l= subscriber.recv_multipart()
        print("[%s] %s" % (l[0], l[1:]))

    # We never get here but clean up anyhow
    subscriber.close()
    context.term()

if __name__ == "__main__":
    main()
