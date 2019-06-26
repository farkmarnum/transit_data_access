from dataclasses import dataclass
from typing import Dict, Any
import eventlet
import socketio   # type: ignore
from flask import Flask      # type: ignore
import util as u  # type: ignore


eventlet.monkey_patch()

WSGI_LOG_FORMAT = '%(client_ip)s %(request_line)s %(status_code)s %(body_length)s %(wall_seconds).6f'
SOCKETIO_URL = f'http://{u.PARSER_SOCKETIO_HOST}:{u.PARSER_SOCKETIO_PORT}'


class SocketToParserNamespace(socketio.ClientNamespace):
    def on_connect(self):
        u.log.info('Connected!')

    def on_disconnect(self):
        u.log.info('Disconnected!')

    def on_new_data(self, data):
        self.emit('client_response', 'Received new data. Thanks!')
        if not data:
            u.log.error('new_data event, but no data attached')
            return
        u.log.info('Received new data!')
        self.web_server.load_new_data(
            current_timestamp=int(data['current_timestamp']),
            data_full=data['data_full'],
            data_diffs=data['data_diffs'])

    def __init__(self, namespace, web_server):
        self.namespace = namespace
        self.web_server = web_server


class WebServerSocketNamespace(socketio.Namespace):
    def on_connect(self, sid, environ: Any) -> None:
        if sid in self.web_server.clients:
            u.log.info('EXISTING client %s RECONNECTED to server', sid)
            self.web_server.clients[sid].connected = True
        else:
            u.log.info('NEW client %s CONNECTED to server', sid)
            self.web_server.clients[sid] = WebClient(
                connected=True,
                last_successful_timestamp=0,
                waiting_for_confirmation=True
            )

    def on_disconnect(self, sid) -> None:
        u.log.info('Client %s disconnected from server', sid)
        try:
            self.web_server.clients[sid].connected = False
        except KeyError:
            u.log.error('%s disconnected but was not found in WebServer.clients', sid)

    def on_connection_confirmed(self, sid) -> None:
        u.log.info('Client confirmed connection: %s', sid)
        self.web_server.clients[sid].waiting_for_confirmation = False

    def on_data_received(self, sid, data):
        u.log.info('Data received by %s! timestamp: %s', sid, data['client_latest_timestamp'])
        self.web_server.clients[sid].last_successful_timestamp = int(data['client_latest_timestamp'])
        self.web_server.clients[sid].waiting_for_confirmation = False

    def __init__(self, namespace, web_server):
        self.namespace = namespace
        self.web_server = web_server


@dataclass
class WebClient:
    connected: bool
    last_successful_timestamp: int
    waiting_for_confirmation: bool

class WebServer:
    def __init__(self) -> None:
        self.parser_socketio_client = socketio.Client(logger=False, engineio_logger=False)
        self.parser_socketio_client.register_namespace(
            SocketToParserNamespace(
                namespace='/socket.io',
                web_server=self))

        self.flask_app = Flask(__name__)
        self.flask_app.config['TEMPLATES_AUTO_RELOAD'] = True
        self.flask_app.add_url_rule(rule='/', view_func=(lambda: f'Hello world!'))

        self.web_server_socket = socketio.Server(async_mode='eventlet')
        self.web_server_socket.register_namespace(
            WebServerSocketNamespace(
                namespace='/socket.io',
                web_server=self))
        self.clients: Dict[int, WebClient] = {}

        self.data_full: bytes = b''
        self.data_diffs: Dict[int, bytes] = {}
        self.current_timestamp: int = 0

        self.pool = eventlet.GreenPool(size=10000)

    def connect_to_parser(self) -> None:
        u.log.info('attempting to connect to %s', SOCKETIO_URL)

        attempt = 0
        while attempt < u.SOCKETIO_CONECTION_MAX_ATTEMPTS:
            try:
                self.parser_socketio_client.connect(SOCKETIO_URL, namespaces=['/socket.io'])
                break
            except socketio.exceptions.ConnectionError:
                attempt += 1
                eventlet.sleep(2)
        else:
            raise socketio.exceptions.ConnectionError

    def server_process(self) -> None:
        eventlet.wsgi.server(
            eventlet.listen((u.WEB_SERVER_HOST, u.WEB_SERVER_PORT)),
            socketio.WSGIApp(self.web_server_socket, self.flask_app),
            log=u.log,
            log_format=WSGI_LOG_FORMAT)

    def start(self) -> None:
        u.log.info('Starting WebServer and connecting to parser')
        self.connect_to_parser()
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self) -> None:
        self.parser_socketio_client.disconnect()
        self.server_thread.kill()

    def load_new_data(self, current_timestamp: int, data_full: bytes, data_diffs: Dict[int, bytes]) -> None:
        u.log.info('Loading new data.')
        self.current_timestamp = current_timestamp
        self.data_full = data_full
        self.data_diffs = {int(k): v for k, v in data_diffs.items()}
        for sid, client in self.clients.items():
            if client.connected:
                self.pool.spawn(self.send_necessary_data, sid)

    def send_full(self, sid) -> None:
        self.web_server_socket.emit(
            'data_full',
            {
                "timestamp": self.current_timestamp,
                "data_full": self.data_full
            },
            namespace='/socket.io',
            room=sid)
        u.log.info('Sent full to %s', sid)

    def send_update(self, sid, last_successful_timestamp: int) -> None:
        self.web_server_socket.emit(
            'data_update', {
                "timestamp": self.current_timestamp,
                "data_update": self.data_diffs[last_successful_timestamp]
            },
            namespace='/socket.io',
            room=sid)
        u.log.info('Sent update to %s', sid)

    def send_necessary_data(self, sid) -> None:
        u.log.info('send_necessary_data for %s', sid)
        print(self.clients[sid])
        if self.clients[sid].waiting_for_confirmation:
            self.web_server_socket.emit(
                'connection_check',
                namespace='/socket.io',
                room=sid)
            return

        u.log.info('confirmation not needed')
        timestamp = self.clients[sid].last_successful_timestamp
        if (not timestamp) or (timestamp not in self.data_diffs):
            self.send_full(sid)
        else:
            self.send_update(sid, timestamp)

        self.clients[sid].waiting_for_confirmation = True
