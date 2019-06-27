from dataclasses import dataclass
from typing import Dict, Any, NewType
import eventlet
import socketio   # type: ignore
from flask import Flask      # type: ignore
import util as u  # type: ignore


eventlet.monkey_patch()


WSGI_LOG_FORMAT = '%(client_ip)s %(request_line)s %(status_code)s %(body_length)s %(wall_seconds).6f'

SID = NewType('SID', str)
ClientID = NewType('ClientID', str)

@dataclass
class WebClient:
    sid: SID
    connected: bool = True
    last_successful_timestamp: int = 0


class SocketToParserNamespace(socketio.ClientNamespace):
    def on_connect(self):
        u.log.debug('Connected to parser')

    def on_disconnect(self):
        u.log.debug('Disconnected from parser')

    def on_new_data(self, data):
        self.emit('client_response', 'Received new data. Thanks!')
        if not data:
            u.log.error('new_data event, but no data attached')
            return
        u.log.debug('Received new data from parser')
        self.web_server.load_new_data(
            current_timestamp=int(data['current_timestamp']),
            data_full=data['data_full'],
            data_diffs=data['data_diffs'])

    def __init__(self, namespace, web_server):
        self.namespace = namespace
        self.web_server = web_server


class WebServerSocketNamespace(socketio.Namespace):
    def on_connect(self, sid, environ: Any) -> None:
        u.log.info('New connection with sid %s from %s. Waiting for client_id', sid, environ['REMOTE_ADDR'])

    def on_set_client_id(self, sid: SID, data) -> None:
        try:
            client_id = data['client_id']
        except KeyError:
            u.log.error('set_client_id message lacks client_id')

        try:
            old_sid = self.web_server.clients[client_id].sid

            self.web_server.clients[client_id].connected = True
            self.web_server.clients[client_id].sid = sid
            del self.web_server.client_id_for_sid[old_sid]
            u.log.debug('OLD CLIENT REJOINED')
        except KeyError:
            u.log.debug('NEW CLIENT')
            self.web_server.clients[client_id] = WebClient(sid=sid)

        self.web_server.client_id_for_sid[sid] = client_id
        u.log.info('set_client for client_id %s and sid %s', client_id, sid)

    def on_disconnect(self, sid) -> None:
        u.log.info('Client %s disconnected from server', sid)
        try:
            client_id = self.web_server.client_id_for_sid[sid]
        except KeyError:
            u.log.error('can\'t find sid %s in client_id_for_sid', sid)
            return

        self.web_server.clients[client_id].connected = False

    def on_data_received(self, sid, data) -> None:
        client_id = ClientID(data['client_id'])
        client = self.web_server.clients[client_id]

        client.last_successful_timestamp = int(data['client_latest_timestamp'])
        u.log.debug('Data received by sid %s, client_id %s! timestamp: %s', sid, client_id, data['client_latest_timestamp'])

    def on_request_full(self, sid, data) -> None:
        client_id = ClientID(data['client_id'])
        try:
            client = self.web_server.clients[client_id]
        except KeyError:
            u.log.error('client %s sent request_full, but that id is not in clients. Adding an entry', client_id)
            self.web_server.clients[client_id] = WebClient(sid=sid)
            self.web_server.client_id_for_sid[sid] = client_id

        client.last_successful_timestamp = 0  # << not really necessary? (TODO)
        self.web_server.send_full(client.sid)


    def __init__(self, namespace, web_server):
        self.namespace = namespace
        self.web_server = web_server


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

        self.clients: Dict[ClientID, WebClient] = {}
        self.client_id_for_sid: Dict[SID, ClientID] = {}

        self.data_full: bytes = b''
        self.data_diffs: Dict[int, bytes] = {}
        self.current_timestamp: int = 0

        self.pool = eventlet.GreenPool(size=10000)


    def connect_to_parser(self) -> None:
        _socketio_url = f'http://{u.PARSER_SOCKETIO_HOST}:{u.PARSER_SOCKETIO_PORT}'
        u.log.info('attempting to connect to %s', _socketio_url)

        attempt = 0
        while attempt < u.SOCKETIO_CONECTION_MAX_ATTEMPTS:
            try:
                self.parser_socketio_client.connect(_socketio_url, namespaces=['/socket.io'])
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
            log_output=False,
            log=u.log,
            log_format=WSGI_LOG_FORMAT
        )

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
        for client_id, client in self.clients.items():
            if client.connected:
                self.pool.spawn(self.send_necessary_data, client_id)

    def send_full(self, sid: SID) -> None:
        self.web_server_socket.emit(
            'data_full',
            {
                "timestamp": self.current_timestamp,
                "data_full": self.data_full
            },
            namespace='/socket.io',
            room=sid)
        u.log.debug('Sent full to %s', sid)

    def send_update(self, sid: SID, last_successful_timestamp: int) -> None:
        self.web_server_socket.emit(
            'data_update', {
                "timestamp_to": self.current_timestamp,
                "timestamp_from": last_successful_timestamp,
                "data_update": self.data_diffs[last_successful_timestamp]
            },
            namespace='/socket.io',
            room=sid)
        u.log.debug('Sent update to %s', sid)

    def send_necessary_data(self, client_id: ClientID) -> None:
        u.log.debug('send_necessary_data for client_id %s', client_id)

        client = self.clients[client_id]
        timestamp = client.last_successful_timestamp
        if (not timestamp) or (timestamp not in self.data_diffs):
            self.send_full(client.sid)
        else:
            self.send_update(client.sid, timestamp)
