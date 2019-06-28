import time
from dataclasses import dataclass
from typing import Dict, NewType
import asyncio
# import signal
# from aiohttp import web
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

clients: Dict[ClientID, WebClient] = {}

async def websocket_server_handler(websocket, path):
    # u.log.info('new connection! path: %s', path)

    async for msg in websocket:
        if isinstance(msg, str):
            msg_dict = ujson.loads(msg)
            client_id = msg_dict['client_id']

            if msg_dict['type'] == 'set_client_id':
                clients[client_id] = WebClient(socket=websocket)
                await websocket.send('{"type": "hello"}')
                # u.log.info('set_client_id for %s', client_id)

            elif msg_dict['type'] == 'data_received':
                clients[client_id].last_successful_timestamp = msg_dict['last_successful_timestamp']
                # u.log.info('data_received by %s', client_id)

            elif msg_dict['type'] == 'request_full':
                clients[client_id].last_successful_timestamp = 0
                await send_to_client(client_id)
                u.log.info('request_full from %s', client_id)

        elif isinstance(msg, bytes):
            u.log.info('it\'s bytes!')
            u.log.warning('unexpected: received a bytes message from client')

        else:
            u.log.info('it\'s something else!')

async def websocket_server():
    u.log.info('starting websocket_server')
    await websockets.serve(websocket_server_handler, u.WEB_SERVER_HOST, u.WEB_SERVER_PORT)

async def send_to_client(client_id):
    client = clients[client_id]
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

async def parser_client():
    u.log.info('starting parser_client')
    while True:
        await asyncio.sleep(10)
        _t = time.time()
        u.log.info('sending out the messages!')
        total_sent = 0
        for client_id, client in clients.items():
            if client.connected:
                total_sent += 1
                asyncio.ensure_future(send_to_client(client_id))

        u.log.info('took %f', time.time() - _t)
        u.log.info('sent %d messages', total_sent)


def main():
    asyncio.ensure_future(websocket_server())
    asyncio.ensure_future(parser_client())

    loop = asyncio.get_event_loop()
    loop.run_forever()

if __name__ == "__main__":
    main()
