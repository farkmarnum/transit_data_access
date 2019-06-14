from typing import Dict, NewType, NamedTuple
from bisect import bisect_left
import socketio  # type: ignore
import eventlet
from flask import Flask
# from google.transit.gtfs_realtime_pb2 import FeedMessage  # tpye: ignore
# import bz2
import util as u  # type: ignore


eventlet.monkey_patch()

Timestamp = NewType('Timestamp', int)
class DataUpdateWithTimestamp(NamedTuple):
    timestamp: Timestamp
    data_update: bytes


DB_SERVER_URL = f'http://{u.DB_IP}:{u.DB_PORT}'

class DatabaseClient:
    """ doc
    """
    def __init__(self):
        self.db_client = socketio.Client()
        self.db_client.on('on_connect', self.on_connect, namespace='/socket.io')
        self.db_client.on('on_disconnect', self.on_disconnect, namespace='/socket.io')
        self.db_client.on('new_data', self.on_new_data, namespace='/socket.io')
        self.web_server = None

    def start(self):
        u.server_logger.info('~~~~~~~~~~~~~~~ Starting Database Client ~~~~~~~~~~~~~~~')
        attempt = 0
        max_attempts = 5
        success = False
        while not success:
            try:
                self.db_client.connect(DB_SERVER_URL, namespaces=['/socket.io'])
                success = True
            except socketio.exceptions.ConnectionError as err:
                if attempt < max_attempts:
                    attempt += 1
                    eventlet.sleep(1)
                else:
                    u.server_logger.error('Unable to connect to %s, %s', DB_SERVER_URL, err)
                    return

        eventlet.spawn(self.db_client.wait)

    def stop(self):
        self.db_client.disconnect()

    def on_connect(self):
        u.server_logger.info('Connected to Database Server at %s', DB_SERVER_URL)

    def on_disconnect(self):
        u.server_logger.info('Disconnected from Database Server at %s', DB_SERVER_URL)
        # TODO: handle reconnection!

    def add_web_server(self, web_server):
        self.web_server = web_server

    def on_new_data(self, data):
        u.server_logger.info('Received new data.')
        self.db_client.emit('client_response', 'Received new data. Thanks!', namespace='/socket.io')
        if self.web_server:
            self.web_server.new_data(data['data_full'], data['data_update'], Timestamp(data['timestamp']))


class WebServer:
    def __init__(self):
        self.flask_app = Flask(__name__)
        self.flask_app.config['TEMPLATES_AUTO_RELOAD'] = True
        self.flask_app.add_url_rule(rule='/', view_func=(lambda: "hello world"))

        self.web_server = socketio.Server(async_mode='eventlet', namespace='/socket.io')
        self.web_server.on('connect', self.on_connect, namespace='/socket.io')
        self.web_server.on('disconnect', self.on_disconnect, namespace='/socket.io')
        self.web_server.on('data_request', self.on_data_request, namespace='/socket.io')

        self.data_full: bytes = b''
        self.data_update: bytes = b''
        self.latest_timestamp: Timestamp = 0
        self.previous_timestamp: Timestamp = 0
        self.data_updates: Dict[Timestamp, DataUpdateWithTimestamp] = {}


    def server_process(self):
        eventlet.wsgi.server(
            eventlet.listen((u.WEB_IP, u.WEB_PORT)),
            socketio.WSGIApp(self.web_server, self.flask_app),
            log=u.server_logger
        )

    def start(self):
        u.server_logger.info('Starting eventlet server @ %s:%s', u.WEB_IP, u.WEB_PORT)
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self):
        u.server_logger.info('Stopping eventlet server')
        self.server_thread.kill()

    def on_connect(self, sid, environ):
        u.server_logger.info('Client connected: %s', sid)

    def on_disconnect(self, sid):
        u.server_logger.info('Client disconnected: %s', sid)



    def new_data(self, data_full: bytes, data_update: bytes, timestamp: Timestamp) -> None:
        u.server_logger.info('Pushing new data to clients')
        self.web_server.emit('new_data', {'timestamp': timestamp}, namespace='/socket.io')

        self.previous_timestamp, self.latest_timestamp = self.latest_timestamp, timestamp
        self.data_full = data_full
        self.data_update = data_update

        if self.previous_timestamp:
            self.data_updates[self.previous_timestamp] = DataUpdateWithTimestamp(timestamp=self.latest_timestamp, data_update=data_update)
        if len(self.data_updates) > 20:
            del(self.data_updates[min(self.data_updates)])


    def on_data_request(self, sid, data):
        if not self.data_full:
            self.web_server.emit('no_data', namespace='/socket.io', room=sid)
            return

        client_latest_timestamp = data['client_latest_timestamp']
        if client_latest_timestamp == self.previous_timestamp:
            self.web_server.emit('data_update', {"timestamp": self.latest_timestamp, "data_update": self.data_update}, namespace='/socket.io', room=sid)

        elif client_latest_timestamp in self.data_updates:
            update_timestamps = sorted(self.data_updates)
            needed_timestamps = update_timestamps[bisect_left(update_timestamps, client_latest_timestamp):]
            sequential_updates = {
                self.data_updates[timestamp].timestamp: self.data_updates[timestamp].data_update for timestamp in needed_timestamps
            }

            self.web_server.emit('multiple_data_updates', sequential_updates, namespace='/socket.io', room=sid)
        else:
            self.web_server.emit('data_full', {"timestamp": self.latest_timestamp, "data_full": self.data_full}, namespace='/socket.io', room=sid)



if __name__ == "__main__":
    web_server = WebServer()
    web_server.start()

    eventlet.sleep(2)

    db_client = DatabaseClient()
    db_client.add_web_server(web_server)
    db_client.start()


    while True:
        try:
            eventlet.greenthread.sleep(1)
        except KeyboardInterrupt:
            print('KeyboardInterrupt, exiting')
            exit()
