import sys
# import time
from dataclasses import dataclass
from typing import Dict, NewType
import asyncio
import websockets  # type: ignore
import util as u  # type: ignore
import ujson

import logging
logger = logging.getLogger('websockets.server')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


SID = NewType('SID', str)
ClientID = NewType('ClientID', str)

@dataclass
class WebClient:
    socket: websockets.WebSocketServerProtocol
    connected: bool = True
    last_successful_timestamp: int = 0


class ParserClient:
    def __init__(self, web_server):
        self.web_server = web_server
        self.current_timestamp = 0
        self.upcoming_data_full_size = 0
        self.upcoming_data_diffs_size = 0
        self.data_full = None
        self.data_diffs = None

    async def listen(self):
        async with websockets.connect(f'ws://{u.PARSER_SOCKETIO_HOST}:{u.PARSER_SOCKETIO_PORT}') as websocket:
            await websocket.send("hello")

            async for msg in websocket:
                if isinstance(msg, str):
                    msg_dict = ujson.loads(msg)
                    self.current_timestamp = msg_dict['current_timestamp']
                    self.upcoming_data_full_size = msg_dict['upcoming_data_full_size']
                    self.upcoming_data_diffs_size = msg_dict['upcoming_data_diffs_size']

                elif isinstance(msg, bytes):
                    _size = sys.getsizeof(msg)
                    if _size == self.upcoming_data_full_size:
                        self.data_full = msg
                    elif _size == self.upcoming_data_diffs_size:
                        self.data_diffs = msg

                    if self.data_full and self.data_diffs and self.current_timestamp:
                        self.web_server.load_new_data(
                            self.current_timestamp,
                            self.data_full,
                            self.data_diffs)

    async def run(self):
        await self.listen()


class WebServer:
    def __init__(self):
        self.clients: Dict[ClientID, WebClient] = {}
        self.parser_client = ParserClient(self)

    def run(self):
        asyncio.ensure_future(self.websocket_server())
        asyncio.ensure_future(self.parser_client.run())

        loop = asyncio.get_event_loop()
        loop.run_forever()

    async def websocket_server_handler(self, websocket, path):
        u.log.debug('new connection! path: %s', path)

        async for msg in websocket:
            if isinstance(msg, str):
                msg_dict = ujson.loads(msg)
                client_id = msg_dict['client_id']

                if msg_dict['type'] == 'set_client_id':
                    self.clients[client_id] = WebClient(socket=websocket)
                    await websocket.send('{"type": "hello"}')
                    # u.log.info('set_client_id for %s', client_id)

                elif msg_dict['type'] == 'data_received':
                    self.clients[client_id].last_successful_timestamp = msg_dict['last_successful_timestamp']
                    # u.log.info('data_received by %s', client_id)

                elif msg_dict['type'] == 'request_full':
                    self.clients[client_id].last_successful_timestamp = 0
                    await self.send_to_client(client_id)
                    u.log.info('request_full from %s', client_id)

            elif isinstance(msg, bytes):
                u.log.warning('unexpected: received a bytes message from client')
            else:
                u.log.warning('unexpected: received a %s message from client', type(msg))

    async def websocket_server(self):
        u.log.info('starting websocket_server')
        await websockets.serve(self.websocket_server_handler, u.WEB_SERVER_HOST, u.WEB_SERVER_PORT)

    async def send_necessary_data(self, client_id):
        client = self.clients[client_id]
        if client.socket.closed:
            u.log.info('client %s disconnected', client_id)
            client.connected = False
            return
        await client.socket.send(ujson.dumps({
            'type': 'data_full',
            'timestamp': 12345000,
            'data_size': 20
        }))
        await client.socket.send(
            bytes(20)
        )
        # u.log.info('sent to %s', client_id)

    async def load_new_data(self, current_timestamp: int, data_full: bytes, data_diffs: Dict[int, bytes]) -> None:
        # _t = time.time()
        u.log.info('sending out the messages!')
        # total_sent = 0
        for client_id, client in self.clients.items():
            if client.connected:
                # total_sent += 1
                asyncio.ensure_future(self.send_necessary_data(client_id))
        # u.log.info('took %f', time.time() - _t)
        # u.log.info('sent %d messages', total_sent)


def main():
    ws = WebServer()
    ws.run()

if __name__ == "__main__":
    main()
