""" sets up server
"""
import time
from bottle import Bottle, route, run, static_file

import misc
from transit_system_config import MTA_SETTINGS

server_logger = misc.server_logger
json_path = MTA_SETTINGS.realtime_json_path
json_filename = 'realtime.json.gz'

IP = SERVER_CONF['DB_SERVER_IP']
PORT = int(SERVER_CONF['DB_SERVER_PORT'])

def main():
    json_server = Bottle()

    @route('/static/realtime.json')
    def serve_realtime_json():
        return static_file(json_filename, root=json_path)

    server_logger.info(f'Starting server listening @ %s:%s', IP, PORT)
    run(host=IP, port=PORT, quiet=True)

if __name__ == "__main__":
    main()
