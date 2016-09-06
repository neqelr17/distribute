#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Sysgsp client.

Replacement for old sys_remote.pl method of distributing work.
"""


import zmq


# Constants
TESTS = 10

def main():
    """Basic request-reply client using REQ socket."""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect('tcp://localhost:5558')

    for i in range(TESTS):
        # Send request, get reply
        socket.send(b'HELLO')
        reply = socket.recv()
        print("{}: {}: {}".format(
            i, socket.identity.decode("ascii"), reply.decode("ascii")))

    # Clean up
    socket.close()
    context.term()

if __name__ == "__main__":
    main()
