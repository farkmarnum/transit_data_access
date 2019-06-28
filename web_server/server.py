import time
from dataclasses import dataclass
from typing import Dict, Any, NewType
import asyncio
from aiohttp import web
import websockets

import util as u  # type: ignore




# eventlet.monkey_patch()


# WSGI_LOG_FORMAT = '%(client_ip)s %(request_line)s %(status_code)s %(body_length)s %(wall_seconds).6f'

SID = NewType('SID', str)
ClientID = NewType('ClientID', str)


@dataclass
class WebClient:
    sid: SID
    connected: bool = True
    last_successful_timestamp: int = 0

"""
class SocketToParserNamespace(socketio.AsyncClientNamespace):
    async def on_connect(self):
        u.log.debug('Connected to parser')

    async def on_disconnect(self):
        u.log.debug('Disconnected from parser')

    async def on_new_data(self, data):
        await self.emit('client_response', 'Received new data. Thanks!')
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
"""

async def web_server_ws_handler(websocket, path):
    name = await websocket.recv()
    print(f"< {name}")

    greeting = f"Hello {name}!"

    await websocket.send(greeting)
    print(f"> {greeting}")

start_server = websockets.serve(web_server_ws_handler, u.WEB_SERVER_HOST, u.WEB_SERVER_PORT)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()


class WebServerSocketNamespace(socketio.AsyncNamespace):
    async def on_connect(self, sid, environ: Any) -> None:
        u.log.info('New connection with sid %s from %s. Waiting for client_id', sid, environ['REMOTE_ADDR'])

    async def on_set_client_id(self, sid: SID, data) -> None:
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

    async def on_disconnect(self, sid) -> None:
        u.log.info('Client %s disconnected from server', sid)
        try:
            client_id = self.web_server.client_id_for_sid[sid]
        except KeyError:
            u.log.error('can\'t find sid %s in client_id_for_sid', sid)
            return

        self.web_server.clients[client_id].connected = False

    async def on_data_received(self, sid, data) -> None:
        client_id = ClientID(data['client_id'])
        client = self.web_server.clients[client_id]

        client.last_successful_timestamp = int(data['client_latest_timestamp'])
        u.log.debug('Data received by sid %s, client_id %s! timestamp: %s', sid, client_id, data['client_latest_timestamp'])

    async def on_request_full(self, sid, data) -> None:
        client_id = ClientID(data['client_id'])
        try:
            client = self.web_server.clients[client_id]
        except KeyError:
            u.log.error('client %s sent request_full, but that id is not in clients. Adding an entry', client_id)
            self.web_server.clients[client_id] = WebClient(sid=sid)
            self.web_server.client_id_for_sid[sid] = client_id

        client.last_successful_timestamp = 0  # << not really necessary? (TODO)
        await self.web_server.send_full(client.sid)


    def __init__(self, namespace, web_server):
        self.namespace = namespace
        self.web_server = web_server


class WebServer:
    def __init__(self):
        self.parser_socketio_client = socketio.AsyncClient(logger=False, engineio_logger=False)
        socket_to_parser_namespace = SocketToParserNamespace(namespace='/socket.io', web_server=self)
        self.parser_socketio_client.register_namespace(socket_to_parser_namespace)

        self.app = web.Application()
        self.app.add_routes([web.get('/', self.index)])

        self.web_server_socket = socketio.AsyncServer(async_mode='aiohttp', async_handlers=True)
        web_server_socket_namespace = WebServerSocketNamespace(namespace='/socket.io', web_server=self)
        self.web_server_socket.register_namespace(web_server_socket_namespace)

        self.web_server_socket.attach(self.app)

        self.clients: Dict[ClientID, WebClient] = {}
        self.client_id_for_sid: Dict[SID, ClientID] = {}

        self.data_full: bytes = b''
        self.data_diffs: Dict[int, bytes] = {}
        self.current_timestamp: int = 0

        self.time_refs_for_sid: Dict[SID, int] = {}

        # self.loop = asyncio.get_event_loop()

    async def index(self, request):
        text = "Hello world!"
        return web.Response(text=text)

    """
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
                asyncio.sleep(2)
        else:
            raise socketio.exceptions.ConnectionError
    """

    def start(self) -> None:
        u.log.info('Starting WebServer and connecting to parser')
        # self.connect_to_parser()
        web.run_app(self.app, host=u.WEB_SERVER_HOST, port=u.WEB_SERVER_PORT, backlog=1000)

    def stop(self) -> None:
        # self.server_thread.kill()
        # self.parser_socketio_client.disconnect()
        pass

    async def load_new_data(self, current_timestamp: int, data_full: bytes, data_diffs: Dict[int, bytes]) -> None:
        u.log.info('Loading new data.')
        self.current_timestamp = current_timestamp
        self.data_full = data_full
        self.data_diffs = {int(k): v for k, v in data_diffs.items()}

        connecteds, disconnecteds = 0, 0
        with u.TimeLogger() as _t:
            for client_id, client in self.clients.items():
                if client.connected:
                    connecteds += 1
                    # self.pool.spawn(self.send_necessary_data, client_id)
                    await self.send_necessary_data(client_id)
                else:
                    disconnecteds += 1

            u.log.info('\n connected clients = %d\ndisconnected clients = %d', connecteds, disconnecteds)
            _t.tlog('LOAD NEW DATA for loop')

            # self.pool.waitall()
            # _t.tlog('LOAD NEW DATA waitall')

    async def send_full(self, sid: SID) -> None:
        _t = time.time()
        await self.web_server_socket.emit(
            'data_full',
            {
                "timestamp": self.current_timestamp,
                "data_full": self.data_full
                # "data_full": "placeholder"
            },
            namespace='/socket.io',
            room=sid)
        u.log.info('send_full time = %f', time.time() - _t)
        u.log.debug('Sent full to %s', sid)

    async def send_update(self, sid: SID, last_successful_timestamp: int) -> None:
        # _t = time.time()
        await self.web_server_socket.emit(
            'data_update', {
                "timestamp_to": self.current_timestamp,
                "timestamp_from": last_successful_timestamp,
                "data_update": self.data_diffs[last_successful_timestamp]
                # "data_update": "placeholder"
            },
            namespace='/socket.io',
            room=sid)
        # u.log.info('send_update time = %f', time.time() - _t)
        u.log.debug('Sent update to %s', sid)

    async def send_necessary_data(self, client_id: ClientID) -> None:
        u.log.debug('send_necessary_data for client_id %s', client_id)

        client = self.clients[client_id]
        timestamp = client.last_successful_timestamp
        if (not timestamp) or (timestamp not in self.data_diffs):
            await self.send_full(client.sid)
        else:
            await self.send_update(client.sid, timestamp)
