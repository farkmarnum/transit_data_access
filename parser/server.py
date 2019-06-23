""" Gets the database (websocket) server running
"""
from typing import Dict
import socketio   # type: ignore
import eventlet
import util as u  # type: ignore

eventlet.monkey_patch()

WSGI_LOG_FORMAT = '%(client_ip)s %(request_line)s %(status_code)s %(body_length)s %(wall_seconds).6f'


class DatabaseServer:
    """ Initializes a server using socketio and eventlet. Use start() and stop() to... start and stop it.
        push() pushes new data to all clients
    """
    def __init__(self):
        self.server = socketio.Server(async_mode='eventlet', namespace='/socket.io')
        self.server.on('connect', self.connect, namespace='/socket.io')
        self.server.on('disconnect', self.disconnect, namespace='/socket.io')
        self.server.on('client_response', self.client_response, namespace='/socket.io')
        self.app = socketio.WSGIApp(self.server)

    def connect(self, sid, environ):
        u.log.info('socketio_server: Client connected: %s', sid)

    def client_response(self, sid, data):
        u.log.info('socketio_server: Client %s sent: %s', sid, data)

    def disconnect(self, sid):
        u.log.info('socketio_server: Client disconnected: %s', sid)

    def push(self, current_timestamp: int, data_full: bytes, data_diffs: Dict[int, bytes]) -> None:
        u.log.info('socketio_server: Pushing the realime data to web_server')
        """
        with open(u.REALTIME_PARSED_PATH + 'data_full.protobuf.bz2', 'rb') as full_infile, \
                open(u.REALTIME_PARSED_PATH + 'data_update.protobuf.bz2', 'rb') as update_infile:
            self.data_full = full_infile.read()
            self.data_update = update_infile.read()
        """

        self.server.emit('new_data', {
            'current_timestamp': current_timestamp,
            'data_full': data_full,
            'data_diffs': data_diffs
        }, namespace='/socket.io')

    def server_process(self):
        eventlet.wsgi.server(eventlet.listen((u.IP, u.PORT)), self.app, log=u.server_logger, log_format=WSGI_LOG_FORMAT)

    def start(self):
        u.log.info('socketio_server: Starting eventlet server @ %s:%s', u.IP, u.PORT)
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self):
        u.log.info('socketio_server: Stopping eventlet server')
        self.server_thread.kill()


if __name__ == "__main__":
    print('Server.py is not intended to be run as a script on its own.')
