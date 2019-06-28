""" Gets the parser websocket server running
"""
import sys
from typing import Dict
import logging
import asyncio
import websockets  # type: ignore
import util as u  # type: ignore
import ujson

logger = logging.getLogger('websockets.server')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class DatabaseServer:
    def __init__(self):
        self.server = None

    async def run(self):
        asyncio.ensure_future(self.start_websocket_server())
        loop = asyncio.get_event_loop()
        loop.run_forever()

    async def websocket_server_handler(self, websocket, path):
        u.log.debug('new connection! path: %s', path)
        async for msg in websocket:
            if isinstance(msg, str):
                u.log.info(msg)

    async def start_websocket_server(self):
        u.log.info('starting websocket_server')
        self.server = await websockets.serve(self.websocket_server_handler, u.PARSER_SOCKETIO_HOST, u.PARSER_SOCKETIO_PORT)


    def push(self, current_timestamp: int, data_full: bytes, data_diffs: Dict[int, bytes]) -> None:
        u.log.debug('socketio_server: Pushing the realime data to web_server')

        for websocket in self.server.sockets:
            if websocket.connected:
                websocket.send(ujson.dumps({
                    'current_timestamp': current_timestamp,
                    'data_full_size': sys.getsizeof(data_full),
                    'data_diffs_size': sys.getsizeof(data_diffs)
                }))
                websocket.send(data_full, data_diffs)

def main():
    ds = DatabaseServer()
    ds.run()

if __name__ == "__main__":
    main()
