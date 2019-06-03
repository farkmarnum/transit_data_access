""" Gets the database (websocket) server running
"""
import socketio
import eventlet
import util as ut

eventlet.monkey_patch()


class DatabaseServer():
    """ Initializes a server using socketio and eventlet. Use start() and stop() to... start and stop it.
        push() pushes new data to all clients
    """
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

    def push(self, data):
        ut.server_logger.info('Pushing new data to clients')
        self.sio.emit('db_server_push', data)

    def server_process(self):
        eventlet.wsgi.server(eventlet.listen((ut.IP, ut.PORT)), self.app, log=ut.server_logger)

    def start(self):
        ut.server_logger.info('Starting eventlet server @ %s:%s', ut.IP, ut.PORT)
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self):
        ut.server_logger.info('Stopping eventlet server')
        self.server_thread.kill()


if __name__ == "__main__":
    print('Server.py is not intended to be run as a script on its own.')