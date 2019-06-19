from multiprocessing import Pool
from typing import Dict, Any, List
import socketio  # type: ignore
import eventlet
from flask import Flask
import util as u  # type: ignore

eventlet.monkey_patch()

DB_SERVER_URL = f'http://{u.DB_IP}:{u.DB_PORT}'
WSGI_LOG_FORMAT = '%(client_ip)s %(request_line)s %(status_code)s %(body_length)s %(wall_seconds).6f'


class DatabaseClient:
    """ doc
    """
    def __init__(self):
        self.socket_to_db_server = socketio.Client()
        self.namespace = self.SocketToDbServerNamespace('/socket.io')
        self.socket_to_db_server.register_namespace(self.namespace)
        self.web_servers: List = []

    class SocketToDbServerNamespace(socketio.ClientNamespace):
        def on_connect(self):
            u.server_logger.info('Connected to Database Server at %s', DB_SERVER_URL)

        def on_disconnect(self):
            u.server_logger.info('Disconnected from Database Server at %s', DB_SERVER_URL)
            # TODO: handle reconnection!

        def on_new_data(self, data):
            u.server_logger.info('Received new data.')
            self.emit('client_response', 'Received new data. Thanks!')

            if not len(self.web_servers):
                u.server_logger.warning('No web server connected yet.')

            for server in self.web_servers:
                server.load_new_data(
                    current_timestamp=int(data['current_timestamp']),
                    data_full=data['data_full'],
                    data_diffs=data['data_diffs'])

        def __init__(self, namespace):
            self.namespace = namespace
            self.web_servers: List = []

    def start(self):
        u.server_logger.info('Starting DatabaseClient')
        attempt = 0
        max_attempts = 5
        success = False
        while not success:
            try:
                self.socket_to_db_server.connect(DB_SERVER_URL, namespaces=['/socket.io'])
                success = True
            except socketio.exceptions.ConnectionError as err:
                if attempt < max_attempts:
                    attempt += 1
                    eventlet.sleep(1)
                else:
                    u.server_logger.error('Unable to connect to %s, %s', DB_SERVER_URL, err)
                    return

        eventlet.spawn(self.socket_to_db_server.wait)

    def stop(self):
        u.server_logger.info('Stopping DatabaseClient')
        self.socket_to_db_server.disconnect()

    def add_web_server(self, web_server):
        self.web_servers.append(web_server)
        self.namespace.web_servers.append(web_server)



class WebServer:
    def __init__(self, port) -> None:  # , message_queue) -> None:
        self.port = port
        self.sessions: Dict[int, dict] = {}
        # self.message_queue = message_queue
        self.flask_app = Flask(__name__)
        self.flask_app.config['TEMPLATES_AUTO_RELOAD'] = True
        self.flask_app.add_url_rule(rule='/', view_func=(lambda: f'hello world, my internal port is {port}'))

        self.web_server_socket = socketio.Server(async_mode='eventlet')  # , client_manager=self.message_queue)
        self.web_server_socket.register_namespace(
            self.WebServerSocketNamespace(
                namespace='/socket.io',
                web_server=self))
        self.clients: List = []

        self.data_full: bytes = b''
        self.data_diffs: Dict[int, bytes] = {}
        self.current_timestamp: int = 0

        self.pool = eventlet.GreenPool(size=10000)

    class WebServerSocketNamespace(socketio.Namespace):
        def on_connect(self, sid, environ: Any) -> None:
            u.server_logger.info('Client %s connected to server on port %s', sid, self.web_server.port)
            with self.web_server.web_server_socket.session(sid) as session:
                session['last_successful_timestamp'] = 0
                session['waiting_for_confirmation'] = True
                self.web_server.clients.append(sid)

        def on_disconnect(self, sid) -> None:
            u.server_logger.info('Client %s disconnected from server on port %s', sid, self.web_server.port)
            try:
                self.web_server.clients.remove(sid)
            except ValueError:
                u.server_logger.error('%s (%s server) disconnected but was not found in WebServer.clients', sid, self.web_server.port)

        def on_connection_confirmed(self, sid) -> None:
            u.server_logger.info('Client confirmed connection: %s', sid)
            with self.web_server.web_server_socket.session(sid) as session:
                session['waiting_for_confirmation'] = False

        def on_data_received(self, sid, data):
            u.server_logger.info('Data received by %s! timestamp: %s', sid, data['client_latest_timestamp'])
            with self.web_server.web_server_socket.session(sid) as session:
                session['last_successful_timestamp'] = int(data['client_latest_timestamp'])
                session['waiting_for_confirmation'] = False

        def __init__(self, namespace, web_server):
            self.namespace = namespace
            self.web_server = web_server

    def server_process(self) -> None:
        eventlet.wsgi.server(
            eventlet.listen((u.WEB_IP, self.port)),
            socketio.WSGIApp(self.web_server_socket, self.flask_app),
            log=u.server_logger,
            log_format=WSGI_LOG_FORMAT)

    def start(self) -> None:
        self.server_thread = eventlet.spawn(self.server_process)

    def stop(self) -> None:
        # self.web_server_socket.emit('server_stopping')
        self.server_thread.kill()

    def load_new_data(self, current_timestamp: int, data_full: bytes, data_diffs: Dict[int, bytes]) -> None:
        u.server_logger.info('Loading new data.')
        self.current_timestamp = current_timestamp
        self.data_full = data_full
        self.data_diffs = {int(k): v for k, v in data_diffs.items()}
        for sid in self.clients:
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
        u.server_logger.info('Sent full to %s', sid)

    def send_update(self, sid, last_successful_timestamp: int) -> None:
        self.web_server_socket.emit(
            'data_update', {
                "timestamp": self.current_timestamp,
                "data_update": self.data_diffs[last_successful_timestamp]
            },
            namespace='/socket.io',
            room=sid)
        u.server_logger.info('Sent update to %s', sid)

    def send_necessary_data(self, sid) -> None:
        u.server_logger.info('send_necessary_data for %s', sid)
        with self.web_server_socket.session(sid) as session:
            print(session)
            if session['waiting_for_confirmation']:
                self.web_server_socket.emit(
                    'connection_check',
                    namespace='/socket.io',
                    room=sid)
                return

            last_successful_timestamp = session['last_successful_timestamp']
            if (not last_successful_timestamp) or (last_successful_timestamp not in self.data_diffs):
                self.send_full(sid)
            else:
                self.send_update(sid, last_successful_timestamp)

            session['waiting_for_confirmation'] = True


"""
class WebServerManager(object):
    def __init__(self):
        # self.message_queue = socketio.RedisManager('redis://127.0.0.1:6379')
        self.db_client = DatabaseClient()
        ws_ports = list(range(u.WEB_SERVER_FIRST_PORT, u.WEB_SERVER_FIRST_PORT + u.NUMBER_OF_WEB_SERVERS))

        p = Pool(u.NUMBER_OF_WEB_SERVERS)
        self.web_servers = []
        # self.web_servers = p.map(self.create_web_server, ws_ports)

    def create_web_server(self, port: int) -> WebServer:
        ws = WebServer(port=port)
        self.db_client.add_web_server(ws)
        return ws

    def start(self):
        self.db_client.start()
        for server in self.web_servers:
            server.start()

    def stop(self):
        self.db_client.stop()
        for server in self.web_servers:
            server.stop()
"""
