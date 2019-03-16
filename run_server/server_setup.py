""" sets up server
1) bind to a port a start listening for incoming connections
2) when a new connection is made with a client (from one of my web servers), keep it open
    2a) SSL or something to make it secure?
3) maintain a record of all current connections (self.clients)
4) run(): every time realtime.json.gz changes, send it to all the connected clients
    4a) make sure files are sent and received fully & correctly
"""
import sys
import socket
import selectors
import types
import time

import misc

server_logger = misc.server_logger

_HOST = misc.SERVER_IP
_PORT = misc.SERVER_PORT
_IPv4 = socket.AF_INET
_TCP = socket.SOCK_STREAM


class FileServer():

    def accept_wrapper(self, sock):
        if sock not in self.clients:
            print(f'adding {sock} to clients')
            clients.append(sock)
            conn, addr = sock.accept()  # Should be ready to read
            server_logger.info("accepted connection from %s", addr)
            conn.setblocking(False)
            sel.register(conn, selectors.EVENT_READ | selectors.EVENT_WRITE, data='connected')
        else:
            print(f'{sock} already in clients')

    def service_connection(self, sock, data):
        #recv_data = sock.recv(1024)  # Should be ready to read
        #if recv_data:
        print('sending data to client')
        #else:
        #    print('closing connection to', data.addr)
        #    sel.unregister(sock)
        #    sock.close()

    def event_loop(self):
        print('running event loop')
        try:
            while True:
                events = self.sel.select(timeout=None)
                if events:
                    print('event!')

                for key, mask in events:
                    if key.data is None:
                        self.accept_wrapper(key.fileobj)

                if self.realtime_feed_is_new:
                    server_logger.info('Pushing new realtime.json.gz')
                    data = b'test'
                    for client in self.clients:
                        #self.service_connection(client, data)
                        print(f'sending {str(data)} to {client}')
                    self.realtime_feed_is_new = False
                time.sleep(0.5)

        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
        finally:
            self.sel.close()

    def __init__(self):
        server_logger.info('\n~~~~~~~~~~~~ BEGINNING SERVER ~~~~~~~~~~~~\n')
        self.realtime_feed_is_new = False
        self.clients = []
        self.sel = selectors.DefaultSelector()

        lsock = socket.socket(_IPv4, _TCP)
        lsock.bind((_HOST, _PORT))
        lsock.listen()
        lsock.setblocking(False)
        server_logger.info("listening on %s:%s", _HOST, str(_PORT))

        self.sel = selectors.DefaultSelector()
        self.sel.register(lsock, selectors.EVENT_READ | selectors.EVENT_WRITE, data=None)






#
