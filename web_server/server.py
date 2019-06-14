from typing import Dict, NewType, NamedTuple, Optional, Any
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
        u.server_logger.info('~~~~~~~~~~~~~~~ Stopping Database Client ~~~~~~~~~~~~~~~')
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
            self.web_server.new_data(
                current_timestamp=Timestamp(data['current_timestamp']),
                data_full=data['data_full'],
                data_diffs=data['data_diffs'])


SID = NewType('SID', str)

class WebServer:
    def __init__(self) -> None:
        self.flask_app = Flask(__name__)
        self.flask_app.config['TEMPLATES_AUTO_RELOAD'] = True
        self.flask_app.add_url_rule(rule='/', view_func=(lambda: "hello world"))

        self.web_server = socketio.Server(async_mode='eventlet', namespace='/socket.io')
        self.web_server.on('connect', self.on_connect, namespace='/socket.io')
        self.web_server.on('disconnect', self.on_disconnect, namespace='/socket.io')
        self.web_server.on('data_received', self.on_data_received, namespace='/socket.io')

        self.data_full: bytes = b''
        self.data_diffs: Dict[Timestamp, bytes] = {}
        self.current_timestamp: Timestamp = Timestamp(0)

        # self.data_update: bytes = b''
        # self.latest_timestamp: Optional[Timestamp] = None
        # self.previous_timestamp: Optional[Timestamp] = None
        # self.data_updates: Dict[Timestamp, DataUpdateWithTimestamp] = {}

        self.client_managers: Dict[SID, ClientManager] = {}
        self.pool = eventlet.GreenPool(size=10000)


    def server_process(self) -> None:
        eventlet.wsgi.server(
            eventlet.listen((u.WEB_IP, u.WEB_PORT)),
            socketio.WSGIApp(self.web_server, self.flask_app),
            log=u.server_logger)

    def start(self) -> None:
        u.server_logger.info('~~~~~~~ Starting eventlet server @ %s:%s ~~~~~~~', u.WEB_IP, u.WEB_PORT)
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self) -> None:
        u.server_logger.info('~~~~~~~ Stopping eventlet server ~~~~~~~')
        self.server_thread.kill()


    def on_connect(self, sid: SID, environ: Any) -> None:
        u.server_logger.info('Client connected: %s', sid)
        self.client_managers[sid] = ClientManager(sid=sid, server=self)

    def on_disconnect(self, sid: SID) -> None:
        u.server_logger.info('Client disconnected: %s', sid)
        try:
            del self.client_managers[sid]
        except KeyError:
            u.server_logger.error('%s disconnected but was not found in self.client_managers', sid)

    def on_data_received(self, sid: SID, data):
        # print(data)
        u.server_logger.info('Data received by %s! timestamp: %s', sid, data['client_latest_timestamp'])
        self.client_managers[sid].last_successful_timestamp = data['client_latest_timestamp']
        # print(self.client_managers[sid].last_successful_timestamp)

    def new_data(self, current_timestamp: Timestamp, data_full: bytes, data_diffs: Dict[Timestamp, bytes]) -> None:
        u.server_logger.info('Loading new data.')

        self.current_timestamp = current_timestamp
        self.data_full = data_full
        self.data_diffs = {Timestamp(k): v for k, v in data_diffs.items()}
        """
        self.previous_timestamp, self.latest_timestamp = self.latest_timestamp, timestamp
        self.data_full = data_full
        self.data_update = data_update

        if self.previous_timestamp:
            self.data_updates[self.previous_timestamp] = DataUpdateWithTimestamp(timestamp=self.latest_timestamp, data_update=data_update)
        if len(self.data_updates) > 20:
            del self.data_updates[min(self.data_updates)]
        """

        for cm in self.client_managers.values():
            self.pool.spawn(cm.send_necessary_data)



class ClientManager:
    def __init__(self, sid: SID, server: WebServer) -> None:
        self.sid = sid
        self.server = server
        self.last_successful_timestamp: Timestamp = Timestamp(0)
        self.currently_sending_data: bool = False

    def send_full(self) -> None:
        self.server.web_server.emit(
            'data_full',
            {
                "timestamp": self.server.current_timestamp,
                "data_full": self.server.data_full
            },
            namespace='/socket.io',
            room=self.sid)
        u.server_logger.info('Sent full to %s', self.sid)

    def send_update(self) -> None:
        data_update = self.server.data_diffs[self.last_successful_timestamp]

        self.server.web_server.emit(
            'data_update',
            {
                "timestamp": self.server.current_timestamp,
                "data_update": data_update
            },
            namespace='/socket.io',
            room=self.sid)
        u.server_logger.info('Sent update to %s', self.sid)

    """
    def send_multiple_updates(self) -> None:
        updates = self.server.data_updates
        timestamps_of_updates = sorted(updates)

        _index = bisect_left(timestamps_of_updates, self.last_successful_timestamp)
        timestamps_of_necessary_updates = timestamps_of_updates[_index:]

        necessary_updates_dict = {updates[t].timestamp: updates[t].data_update for t in timestamps_of_necessary_updates}
        self.server.web_server.emit(
            'multiple_data_updates',
            necessary_updates_dict,
            namespace='/socket.io',
            room=self.sid)
        u.server_logger.info('Sent multiple updates to %s', self.sid)
    """

    def send_necessary_data(self) -> None:
        if self.currently_sending_data:
            return
        self.currently_sending_data = True
        u.server_logger.info('called! sid=%s', self.sid)

        # print('cm: last_succ_ts:', self.last_successful_timestamp)
        # print('cm: web_server.data_diffs:', {k: int(len(v) / 1024) for k, v in self.server.data_diffs.items()})


        if (not self.last_successful_timestamp) or (self.last_successful_timestamp not in self.server.data_diffs):
            self.send_full()
        else:
            self.send_update()

        self.currently_sending_data = False



if __name__ == "__main__":
    db_client = DatabaseClient()
    db_client.start()

    web_server = WebServer()
    web_server.start()

    eventlet.sleep(2)
    db_client.add_web_server(web_server)

    while True:
        try:
            eventlet.greenthread.sleep(1)
        except KeyboardInterrupt:
            print('KeyboardInterrupt, exiting')
            web_server.stop()
            db_client.stop()
            exit()
