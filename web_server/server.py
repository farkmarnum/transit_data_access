import socketio  # type: ignore
import eventlet
import util as u  # type: ignore

eventlet.monkey_patch()

DB_SERVER_URL = f'http://{u.DB_IP}:{u.DB_PORT}'

class DatabaseClient:
    """ doc
    """
    def __init__(self):
        self.client = socketio.Client()
        self.client.on('connect', self.on_connect)
        self.client.on('disconnect', self.on_disconnect)
        self.client.on('new_data_full', self.on_new_data_full)
        self.client.on('new_data_update', self.on_new_data_update)

    def start(self):
        u.server_logger.info('~~~~~~~~~~~~~~~ Starting Database Client ~~~~~~~~~~~~~~~')
        attempt = 0
        max_attempts = 5
        while True:
            try:
                self.client.connect(DB_SERVER_URL, namespaces='/socketio')
                break
            except socketio.exceptions.ConnectionError as err:
                if attempt < max_attempts:
                    attempt += 1
                    eventlet.sleep(1)
                else:
                    u.server_logger.error('Unable to connect to %s, %s', DB_SERVER_URL, err)
                    return
        self.client.wait()

    def stop(self):
        self.client.disconnect()

    def on_connect(self):
        u.server_logger.info('Connected to Database Server at %s', DB_SERVER_URL)

    def on_disconnect(self):
        u.server_logger.info('Disconnected from Database Server at %s', DB_SERVER_URL)
        # TODO: handle reconnection!

    def on_new_data_full(self, data):
        u.server_logger.info('Received new data_full')
        self.client.emit('client_response', 'Received data_full')

    def on_new_data_update(self, data):
        u.server_logger.info('Received new data_update')
        self.client.emit('client_response', 'Received data_update')

"""
class WebServer:
    def __init__(self):
        self.server = socketio.Server(async_mode='eventlet')
        self.server.on('connect', self.on_connect)
        self.server.on('disconnect', self.on_disconnect)
        self.server.on('client_response', self.client_response)
        self.app = socketio.WSGIApp(self.server)


    def on_connect(self, sid, environ):
        u.server_logger.info('Client connected: %s', sid)

    def client_response(self, sid, data):
        u.server_logger.info('Client %s sent: %s', sid, data)

    def on_disconnect(self, sid):
        u.server_logger.info('Client disconnected: %s', sid)


    def server_process(self):
        eventlet.wsgi.server(eventlet.listen((u.WEB_IP, u.WEB_PORT)), self.app, log=u.server_logger)

    def start(self):
        u.server_logger.info('Starting eventlet server @ %s:%s', u.WEB_IP, u.WEB_PORT)
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self):
        u.server_logger.info('Stopping eventlet server')
        self.server_thread.kill()
"""

client = DatabaseClient()
client.start()

"""
while True:
    try:
        eventlet.sleep(1)
    except KeyboardInterrupt:
        print('\nKeyboardInterrupt, exiting')
        break
ws.stop()
"""
