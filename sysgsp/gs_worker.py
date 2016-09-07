#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Sysgsp Worker.

One Worker will spawn off n amount of tasks to do the actuall work.
"""


import sys
import time


import zmq


# Constants
GS_WORKERS = 3
ENCODING = 'ascii'


def gs_task(ident):
    """Worker task, using a REQ socket to do load-balancing."""
    context = zmq.Context()
    work_socket = context.socket(zmq.REQ)
    work_socket.identity = 'gs_worker{}'.format(ident).encode(ENCODING)
    work_socket.connect('tcp://localhost:5559')

    try:
        # Tell broker we're ready for work
        work_socket.send(b'READY')

        while True:
            address, empty, request = work_socket.recv_multipart()
            print("{}: {}".format(work_socket.identity.decode(ENCODING),
                              request.decode(ENCODING)))
            work_socket.send_multipart([address, b'', b'OKAY'])
            time.sleep(int(ident))
    except KeyboardInterrupt:
        print('\nGot ctrl-c')
        print('Shutting down')

        # Clean up
        work_socket.close()
        context.term()


def main():
    """Start worker."""
    gs_task(sys.argv[1])

if __name__ == "__main__":
    main()
