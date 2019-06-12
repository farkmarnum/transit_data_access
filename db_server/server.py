""" Gets the database (websocket) server running
"""
from typing import List, NewType
from dataclasses import dataclass
import socketio   # type: ignore
import eventlet
import util as u  # type: ignore

eventlet.monkey_patch()


class DatabaseServer:
    """ Initializes a server using socketio and eventlet. Use start() and stop() to... start and stop it.
        push() pushes new data to all clients
    """
    def __init__(self):
        self.sio = socketio.Server(async_mode='eventlet', namespace='/socketio')
        self.sio.on('connect', self.connect)
        self.sio.on('disconnect', self.disconnect)
        self.sio.on('client_response', self.client_response)
        self.app = socketio.WSGIApp(self.sio)
        self.keep_server_running = True

    def connect(self, sid, environ):
        u.server_logger.info('Client connected: %s', sid)

    def client_response(self, sid, data):
        u.server_logger.info('Client %s sent: %s', sid, data)

    def disconnect(self, sid):
        u.server_logger.info('Client disconnected: %s', sid)

    def push(self):
        u.server_logger.info('Pushing the realime data to web_server')
        data_full = b''
        data_update = b''
        with open(u.REALTIME_PARSED_PATH + 'data_full.protobuf.bz2', 'rb') as infile:
            data_full = infile.read()
        with open(u.REALTIME_PARSED_PATH + 'data_update.protobuf.bz2', 'rb') as infile:
            data_update = infile.read()

        self.sio.emit('data_full', data_full)
        self.sio.emit('data_update', data_update)

    def server_process(self):
        eventlet.wsgi.server(eventlet.listen((u.IP, u.PORT)), self.app, log=u.server_logger)

    def start(self):
        u.server_logger.info('Starting eventlet server @ %s:%s', u.IP, u.PORT)
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self):
        u.server_logger.info('Stopping eventlet server')
        self.server_thread.kill()


if __name__ == "__main__":
    # print('Server.py is not intended to be run as a script on its own.')
    ds = DatabaseServer()
    ds.start()
    ds.push()
    ds.stop()
