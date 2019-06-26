from dataclasses import dataclass
from typing import Dict, Any, NewType
import eventlet
import socketio   # type: ignore
from flask import Flask      # type: ignore
import util as u  # type: ignore

DELIMITER = chr(30)

eventlet.monkey_patch()

WSGI_LOG_FORMAT = '%(client_ip)s %(request_line)s %(status_code)s %(body_length)s %(wall_seconds).6f'


SID = NewType('SID', str)
ClientID = NewType('ClientID', str)


@dataclass
class WebClient:
    sid: SID = SID('')
    connected: bool = True
    last_successful_timestamp: int = 0

    @property
    def serialized(self) -> str:
        return f'{self.sid}{DELIMITER}{int(self.connected)}{DELIMITER}{self.last_successful_timestamp}'

class WebClientFromStr(WebClient):
    def __init__(self, in_str: str):
        _arg_list = in_str.split(DELIMITER)
        sid = SID(_arg_list[0])
        connected = (_arg_list[1] == '1')
        last_successful_timestamp = int(_arg_list[2])

        super(WebClientFromStr, self).__init__(sid, connected, last_successful_timestamp)


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
        u.log.info('New connection with sid %s from %s. Waiting for client_id', sid, environ['REMOTE_ADDR'])

    def on_set_client_id(self, sid: SID, data) -> None:
        if not data['client_id']:
            u.log.error('set_client_id message lacks client_id')
            return
        client_id = ClientID(data['client_id'])
        client = self.web_server.get_client(client_id)
        client.sid = sid

        self.web_server.store_client(client_id, client)
        self.web_server.set_client_id_for_sid(sid, client_id)
        u.log.info('set_client for client_id %s and sid %s', client_id, sid)

    def on_disconnect(self, sid) -> None:
        u.log.info('Client %s disconnected from server', sid)
        client_id = self.web_server.get_client_id_from_sid(sid)
        client = self.web_server.get_client(client_id)
        client.connected = False
        self.web_server.store_client(client_id, client)

    def on_data_received(self, sid, data):
        # print()
        # u.log.info(u.redis_server.keys())
        client_id = ClientID(data['client_id'])
        if not client_id:
            u.log.error('on_data_received message lacks client_id')
            return

        client = self.web_server.get_client(client_id)
        if not client.sid:
            u.log.error('on_data_received message client_id has no record')
            return

        client.last_successful_timestamp = int(data['client_latest_timestamp'])
        self.web_server.store_client(client_id, client)

        u.log.info('Data received by sid %s, client_id %s! timestamp: %s', sid, client_id, data['client_latest_timestamp'])

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

        self.web_server_socket = socketio.Server(
            async_mode='eventlet',
            # client_manager=socketio.RedisManager(f'redis://{u.REDIS_HOST}:{u.REDIS_PORT}'),  # TODO
            # logger=u.log,
            # engineio_logger=u.log
        )
        self.web_server_socket.register_namespace(
            WebServerSocketNamespace(
                namespace='/socket.io',
                web_server=self))
        self.sid_to_client_id: Dict[SID, ClientID] = {}

        self.data_full: bytes = b''
        self.data_diffs: Dict[int, bytes] = {}
        self.current_timestamp: int = 0

        self.pool = eventlet.GreenPool(size=10000)

    def get_client_id_from_sid(self, sid: SID) -> ClientID:
        _bytes = u.redis_server.hget('web_server-client_id_from_sid', sid)
        if _bytes:
            return ClientID(_bytes.decode('utf-8'))
        else:
            raise KeyError

    def set_client_id_for_sid(self, sid: SID, client_id: ClientID) -> None:
        return u.redis_server.hset('web_server-client_id_from_sid', sid, client_id)

    def get_all_clients(self) -> dict:
        _raw_client_dict = u.redis_server.hgetall('web_server-clients')
        return {client_id: WebClientFromStr(_bytes.decode('utf-8')) for client_id, _bytes in _raw_client_dict.items()}

    # def get_client_id_list(self) -> list:
    #    return u.redis_server.hkeys('web_server-clients')

    def get_client(self, client_id) -> WebClient:
        _bytes = u.redis_server.hget('web_server-clients', client_id)
        if _bytes:
            client = WebClientFromStr(_bytes.decode('utf-8'))
            print('got client:', client)
            return client
        else:
            return WebClient()

    def store_client(self, client_id: ClientID, client: WebClient) -> None:
        client_str = client.serialized
        print('setting client_str:', client_str)
        u.redis_server.hset(f'web_server-clients', client_id, client_str)

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
            # log=u.log,
            # log_format=WSGI_LOG_FORMAT
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
        for client_id, client in self.get_all_clients().items():
            print()
            print(client_id, client)
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
        u.log.info('Sent full to %s', sid)

    def send_update(self, sid: SID, last_successful_timestamp: int) -> None:
        self.web_server_socket.emit(
            'data_update', {
                "timestamp": self.current_timestamp,
                "data_update": self.data_diffs[last_successful_timestamp]
            },
            namespace='/socket.io',
            room=sid)
        u.log.info('Sent update to %s', sid)

    def send_necessary_data(self, client_id: ClientID) -> None:
        u.log.info('send_necessary_data for client_id %s', client_id)

        client = self.get_client(client_id)
        timestamp = client.last_successful_timestamp
        if (not timestamp) or (timestamp not in self.data_diffs):
            self.send_full(client.sid)
        else:
            self.send_update(client.sid, timestamp)
