""" Gets the database (websocket) server running
"""
import socketio   # type: ignore
import eventlet
import util as u  # type: ignore

eventlet.monkey_patch()

WSGI_LOG_FORMAT = '%(client_ip)s "%(request_line)s" %(status_code)s %(body_length)s %(wall_seconds).6f'


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
        u.server_logger.info('Client connected: %s', sid)

    def client_response(self, sid, data):
        u.server_logger.info('Client %s sent: %s', sid, data)

    def disconnect(self, sid):
        u.server_logger.info('Client disconnected: %s', sid)

    def push(self, timestamp):
        u.server_logger.info('Pushing the realime data to web_server')
        with open(u.REALTIME_PARSED_PATH + 'data_full.protobuf.bz2', 'rb') as full_infile, \
                open(u.REALTIME_PARSED_PATH + 'data_update.protobuf.bz2', 'rb') as update_infile:
            self.data_full = full_infile.read()
            self.data_update = update_infile.read()

        self.server.emit('new_data', {
            'data_full': self.data_full,
            'data_update': self.data_update,
            'timestamp': timestamp
        }, namespace='/socket.io')
        # self.server.emit('new_data_full', self.data_full, namespace='/socket.io')
        # self.server.emit('new_data_update', self.data_update, namespace='/socket.io')

    def server_process(self):
        eventlet.wsgi.server(eventlet.listen((u.IP, u.PORT)), self.app, log=u.server_logger, log_format=WSGI_LOG_FORMAT)

    def start(self):
        u.server_logger.info('Starting eventlet server @ %s:%s', u.IP, u.PORT)
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self):
        u.server_logger.info('Stopping eventlet server')
        self.server_thread.kill()


if __name__ == "__main__":
    print('Server.py is not intended to be run as a script on its own.')
