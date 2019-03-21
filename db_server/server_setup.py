""" sets up server
"""
import time
from flask import Flask, render_template
from flask_socketio import SocketIO

import misc
from transit_system_config import MTA_SETTINGS

server_logger = misc.server_logger
json_path = MTA_SETTINGS.realtime_json_path
json_filename = 'realtime.json.gz'

def main():
    json_server = Flask(__name__)
    socketio = SocketIO(json_server)
    socketio.run(json_server, host=misc.IP, port=misc.PORT)

if __name__ == "__main__":
    main()
