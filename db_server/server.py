"""gets the database (websocket) server running
"""
import socketio
import eventlet
from eventlet.support import greenlets as greenlet
import time
import util as ut

eventlet.monkey_patch()

KILL_SERVER = False


class DatabaseServer():
    def connect(self, sid, environ):
        ut.server_logger.info('Client connected: %s', sid)

    def client_response(self, sid, data):
        ut.server_logger.info('Client %s sent: %s', sid, data)

    def disconnect(self, sid):
        ut.server_logger.info('Client disconnected: %s', sid)

    def __init__(self):
        self.sio = socketio.Server(async_mode='eventlet', namespace='/socketio')
        self.sio.on('connect', self.connect)
        self.sio.on('disconnect', self.disconnect)
        self.sio.on('client_response', self.client_response)
        self.app = socketio.WSGIApp(self.sio)
        self.keep_server_running = True

    def push(self, json):
        ut.server_logger.info('Pushing new data to clients')
        self.sio.emit('db_server_push', json)

    def server_process(self):
        eventlet.wsgi.server(eventlet.listen((ut.IP, ut.PORT)), self.app, log=ut.server_logger)

    def start(self):
        ut.server_logger.info('Starting eventlet server @ %s:%s', ut.IP, ut.PORT)
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self):
        ut.server_logger.info('Stopping eventlet server')
        self.server_thread.kill()

def main():
    ut.server_logger.info('~~~~~~~~~~ server.py beginning! ~~~~~~~~~~')
    db_server = DatabaseServer()
    db_server.start()
    return db_server


if __name__ == "__main__":
    main()