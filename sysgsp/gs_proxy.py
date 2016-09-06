#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Sysgsp load-balancing broker.

Receives n clients and distributes to n workers based off of availability and
performance.
"""


import zmq


__author__ = 'Brett R. Ward'



def main():
    """Load balancer main loop."""
    # Prepare context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind('tcp://localhost:5558')
    backend = context.socket(zmq.ROUTER)
    backend.bind('tcp://localhost:5559')

    # Initialize main loop state
    workers = []
    poller = zmq.Poller()
    # Only poll for requests from backend until workers are available
    poller.register(backend, zmq.POLLIN)

    try:
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
                    frontend.send_multipart([client, b'', reply])

            if frontend in sockets:
                # Get next client request, route to last-used worker
                client, empty, request = frontend.recv_multipart()
                worker = workers.pop(0)
                backend.send_multipart([worker, b'', client, b'', request])
                if not workers:
                    # Don't poll clients if no workers are available
                    poller.unregister(frontend)
    except KeyboardInterrupt:
        print('\nGot ctrl-c')
        print('Shutting down')

    # Clean up
    backend.close()
    frontend.close()
    context.term()

if __name__ == "__main__":
    main()
