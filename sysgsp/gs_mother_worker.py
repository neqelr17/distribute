#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Sysgsp Worker.

One Worker will spawn off n amount of tasks to do the actuall work.
"""


import multiprocessing


import zmq


# Constants
GS_WORKERS = 3
ENCODING = 'ascii'


def gs_task(ident):
    """Worker task, using a REQ socket to do load-balancing.

    Also connects to mother with an ipc to recive shutdown commands.
    """
    mother_socket = zmq.Context().socket(zmq.REP)
    mother_socket.identity = u'gs_ipcworker{}'.format(ident).encode(ENCODING)
    mother_socket.connect('ipc://mother.ipc')

    work_socket = zmq.Context().socket(zmq.REQ)
    work_socket.identity = u'gs_worker{}'.format(ident).encode(ENCODING)
    work_socket.connect('tcp://localhost:5559')

    # Tell broker we're ready for work
    work_socket.send(b'READY')

    while True:
        address, empty, request = work_socket.recv_multipart()
        print("{}: {}".format(work_socket.identity.decode(ENCODING),
                              request.decode(ENCODING)))
        work_socket.send_multipart([address, b'', b'OKAY'])


def main():
    """Load mother main loop."""
    # Prepare context and sockets
    context = zmq.Context.instance()
    mother = context.socket(zmq.REP)
    mother.bind("ipc://mother.ipc")

    # Start background tasks
    def start(task, *args):
        process = multiprocessing.Process(target=task, args=args)
        process.daemon = True
        process.start()
    for i in range(GS_WORKERS):
        start(gs_task, i)

    # Initialize main loop state
    count = NBR_CLIENTS
    workers = []
    poller = zmq.Poller()
    # Only poll for requests from backend until workers are available
    poller.register(backend, zmq.POLLIN)

    while True:
        sockets = dict(poller.poll())

        if backend in sockets:
            # Handle worker activity on the backend
            request = backend.recv_multipart()
            worker, empty, client = request[:3]
            if not workers:
                # Poll for clients now that a worker is available
                poller.register(frontend, zmq.POLLIN)
            workers.append(worker)
            if client != b"READY" and len(request) > 3:
                # If client reply, send rest back to frontend
                empty, reply = request[3:]
                frontend.send_multipart([client, b"", reply])
                count -= 1
                if not count:
                    break

        if frontend in sockets:
            # Get next client request, route to last-used worker
            client, empty, request = frontend.recv_multipart()
            worker = workers.pop(0)
            backend.send_multipart([worker, b"", client, b"", request])
            if not workers:
                # Don't poll clients if no workers are available
                poller.unregister(frontend)

    # Clean up
    backend.close()
    frontend.close()
    context.term()

if __name__ == "__main__":
    main()
