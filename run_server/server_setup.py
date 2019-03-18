""" sets up server
"""
import time
from bottle import Bottle, route, run, static_file

import misc
from ts_config import MTA_SETTINGS
from misc import server_logger

server_logger = misc.server_logger
json_path = MTA_SETTINGS.realtime_json_path
json_filename = 'realtime.json.gz'

def main():
    json_server = Bottle()

    @route('/static/realtime.json')
    def serve_realtime_json():
        return static_file(json_filename, root=json_path)

    server_logger.info(f'Starting server listening @ %s:%s', misc.DB_SERVER_IP, misc.DB_SERVER_PORT)
    run(host=misc.DB_SERVER_IP, port=misc.DB_SERVER_PORT, quiet=True)

if __name__ == "__main__":
    main()
