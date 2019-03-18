""" sets up server
1) bind to a port a start listening for incoming connections
2) when a new connection is made with a client (from one of my web servers), keep it open
3) maintain a record of all current connections (self.clients)
4) update_loop(): every time realtime.json.gz changes, send it to all the connected clients
    4a) make sure files are sent and received fully & correctly
"""
import socket
import time

import misc
from ts_config import MTA_SETTINGS

server_logger = misc.server_logger

class JSONServer():
    """ A simple server that accepts connections and periodically sends new JSON files
    """
    def send_json(self, conn, send_file):
        """ Sends send_file to conn
        """
        with open(send_file, 'rb') as file_:
            chunk = file_.read(1024)
            while chunk:
                conn.sendall(chunk)
                chunk = file_.read(1024)

    def listen_loop(self):
        """ Listens for new connections and adds them to self.clients
        """
        server_logger.info('Beginning listen loop')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((misc.SERVER_IP, misc.SERVER_PORT))
            sock.listen()
            while True:
                conn, addr = sock.accept()
                if conn not in self.clients:
                    print(f'adding {conn} @ {addr} to clients')
                    self.clients.append((conn, addr))
                time.sleep(2)

    def update_loop(self):
        """ Waits for self.realtime_feed_is_new to be changed to True, then iterates throgh self.clients, calling send_json() for each
        """
        server_logger.info('Beginning update loop')
        while True:
            if self.realtime_feed_is_new:
                print('time to push a new json!')
                for client in self.clients:
                    (conn, addr) = client
                    try:
                        self.send_json(conn, self.json_file)
                        print(f'sent to {addr}')
                    except ConnectionError as err:
                        print(f'ConnectionError {err} when attempting to send to {addr}, removing from clients list')
                        self.clients.remove(client)

                self.realtime_feed_is_new = False

            time.sleep(1)

    def __init__(self):
        self.clients = []
        self.json_file = f'{MTA_SETTINGS.realtime_json_path}/realtime.json.gz'
        self.realtime_feed_is_new = False
