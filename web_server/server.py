import sys
# import time
from dataclasses import dataclass
from typing import Dict, NewType
import asyncio
import uvloop  # type: ignore
from concurrent.futures import ThreadPoolExecutor
import websockets  # type: ignore
import util as u  # type: ignore
import ujson
import aioredis  # type: ignore
import logging

logger = logging.getLogger('websockets.server')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class RedisClient:
    def __init__(self, web_server) -> None:
        self.web_server = web_server
        self.redis_server: aioredis.Redis = None
        self.current_timestamp: int = 0
        self.data_full = None
        self.data_diffs: Dict = {}

    async def get_timestamp(self) -> None:
        _timestamp = await self.redis_server.get('realtime:current_timestamp')
        if _timestamp:
            self.current_timestamp = int(_timestamp.decode('utf-8'))

    async def get_data_full(self) -> None:
        self.data_full = await self.redis_server.get('realtime:data_full')

    async def get_data_diffs(self) -> None:
        self.data_diffs = await self.redis_server.hgetall('realtime:data_diffs')

    async def run(self) -> None:
        pubsub = await aioredis.create_redis(f'redis://{u.REDIS_HOST}:{u.REDIS_PORT}')
        await pubsub.subscribe('realtime_updates')
        try:
            async for msg in pubsub.channels['realtime_updates'].iter():
                if msg.decode('utf-8') == 'new_data':
                    await asyncio.gather(
                        self.get_timestamp(),
                        self.get_data_full(),
                        self.get_data_diffs())

                    await self.web_server.load_new_data(
                        current_timestamp=self.current_timestamp,
                        data_full=self.data_full,
                        data_updates=self.data_diffs)
        except Exception as e:
            print('ERROR IN REDISCLIENT RUN', e)

    async def init_async(self) -> None:
        self.redis_server = await aioredis.create_redis(f'redis://{u.REDIS_HOST}:{u.REDIS_PORT}')

        await asyncio.gather(
            self.get_timestamp(),
            self.get_data_full(),
            self.get_data_diffs())

SID = NewType('SID', str)

ClientID = NewType('ClientID', str)

@dataclass
class WebClient:
    socket: websockets.WebSocketServerProtocol
    connected: bool = True
    last_successful_timestamp: int = 0


class WebServer:
    def __init__(self):
        self.redis_client = RedisClient(self)

        self.clients: Dict[ClientID, WebClient] = {}
        self.server = None

        self.data_full: bytes = b''
        self.data_updates: Dict[int, bytes] = {}
        self.current_timestamp: int = 0
        self.data_full_size: int = 0
        self.data_updates_size: Dict[int, int] = {}

        self.executor = ThreadPoolExecutor(max_workers=1000)


    def run(self):
        loop = asyncio.get_event_loop()
        loop.set_debug(True)

        loop.run_until_complete(self.redis_client.init_async())

        self.current_timestamp = self.redis_client.current_timestamp
        self.data_full = self.redis_client.data_full
        self.data_updates = self.redis_client.data_diffs

        start_server = websockets.serve(
            ws_handler=self.websocket_server_handler,
            host=u.WEB_SERVER_HOST,
            port=u.WEB_SERVER_PORT,
            # write_limit=2 ** 17,
            # read_limit=2 ** 17,
            # max_size=2 ** 14,
            # max_queue=10000,
            compression=None
        )
        self.server = loop.run_until_complete(start_server)

        loop.create_task(self.redis_client.run())

        try:
            u.log.info('running forever')
            loop.run_forever()
        except KeyboardInterrupt:
            u.log.error('Keyboard Interrupt, closing server.')
            loop.run_until_complete(self.server.wait_closed())
        finally:
            u.log.info('Exiting')
            loop.close()



    async def websocket_server_handler(self, websocket, path):
        u.log.debug('new connection! path: %s', path)
        async for msg in websocket:
            # u.log.debug(msg)
            if isinstance(msg, str):
                msg_dict = await asyncio.get_event_loop().run_in_executor(self.executor, ujson.loads, msg)
                client_id = msg_dict['client_id']

                if msg_dict['type'] == 'set_client_id':
                    self.clients[client_id] = WebClient(socket=websocket)
                    u.log.debug('set_client_id for %s', client_id)

                elif msg_dict['type'] == 'data_received':
                    self.clients[client_id].last_successful_timestamp = int(msg_dict['last_successful_timestamp'])
                    u.log.debug('data_received by %s', client_id)

                elif msg_dict['type'] == 'request_full':
                    await self.send_necessary_data(client_id, 0)
                    u.log.debug('request_full from %s', client_id)

            elif isinstance(msg, bytes):
                u.log.warning('unexpected: received a bytes message from client')
            else:
                u.log.warning('unexpected: received a %s message from client', type(msg))


    async def send_full(self, client_id: ClientID) -> None:
        socket = self.clients[client_id].socket
        msg_str = f'''{{"type": "data_full", "timestamp": "{self.current_timestamp}", "data_size": "{self.data_full_size}"}}'''

        await socket.send(msg_str)
        # await socket.send(self.data_full)
        u.log.debug('Sent full to %s', client_id)

    async def send_update(self, client_id: ClientID, last_successful_timestamp: int) -> None:
        socket = self.clients[client_id].socket
        data_update = self.data_updates[last_successful_timestamp]
        data_size = self.data_updates_size[last_successful_timestamp]
        msg_str = f'''{{"type": "data_update", "timestamp_to": "{self.current_timestamp}", "timestamp_from": "{last_successful_timestamp}", "data_size": "{data_size}"}}'''

        await socket.send(msg_str)
        # await socket.send(data_update)
        u.log.debug('Sent update to %s', client_id)

    async def send_necessary_data(self, client_id, timestamp):
        if (not timestamp) or (timestamp not in self.data_updates):
            await self.send_full(client_id)
        else:
            await self.send_update(client_id, timestamp)


    async def load_new_data(self, current_timestamp: int, data_full: bytes, data_updates: Dict[int, bytes]) -> None:
        with u.TimeLogger() as _tl:
            try:
                self.current_timestamp = current_timestamp
                self.data_full = data_full
                self.data_updates = {int(k): v for k, v in data_updates.items()}
                self.data_full_size = sys.getsizeof(self.data_full) - 33
                self.data_updates_size = {_timestamp: sys.getsizeof(_update) - 33 for _timestamp, _update in self.data_updates.items()}

                if self.clients:
                    client_ids = list(self.clients)
                    u.log.info('sending out the messages!')
                    tasks = []
                    for client_id in client_ids:
                        client = self.clients[client_id]
                        if client.connected:
                            if client.socket.closed:
                                u.log.debug('client %s disconnected', client_id)
                                client.connected = False
                            else:
                                tasks.append(asyncio.create_task(
                                    self.send_necessary_data(
                                        client_id,
                                        client.last_successful_timestamp)))
                        await asyncio.sleep(0)

                    if tasks:
                        await asyncio.wait(tasks)

                    connected_clients_count = 0
                    for client_id in client_ids:
                        client = self.clients[client_id]
                        if client.connected:
                            connected_clients_count += 1

                    u.log.info('clients: there are %d connected & %d total', connected_clients_count, len(self.clients))

                else:
                    u.log.info('no clients to send messages to')
            except Exception as e:
                print('ERROR IN LOAD_NEW_DATA', e)

            _tl.tlog('>>>>>>>>>>>>>>>> load_new_data()')


if __name__ == "__main__":
    ws = WebServer()
    ws.run()
