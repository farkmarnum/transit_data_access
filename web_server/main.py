""" Serves requests as a web server and websockets for JSON
Also, sets up a connection with the db server and receives a new realtime.json periodically.
"""
import os
import time
import datetime
import time
import requests
import socketio
import eventlet
from flask import Flask, render_template

import misc
from misc import web_server_logger

eventlet.monkey_patch()

json_local_fname = misc.DATA_PATH+'/realtime.json.gz'
KILL_SERVER = False


###################################################################################################
# SET UP WEB SERVER WITH WEBSOCKETS:

web_socket_namespace = '/socketio'
web_socket = socketio.Server(async_mode='eventlet')
flask_app = Flask(__name__)
flask_app.config['TEMPLATES_AUTO_RELOAD'] = True

@flask_app.route('/')
def hello_world():
    web_server_logger.info('new http connection')
    return render_template('index.html')

@web_socket.on('connect', namespace=web_socket_namespace)
def connect(sid, environ):
    web_server_logger.info('Client connected: %s', sid)
    try:
        with open(json_local_fname, 'rb') as json_local:
            json = json_local.read()
    except FileNotFoundError:
        web_server_logger.info('%s does not exist, cannot send JSON to client', misc.DATA_PATH)
    else:
        web_socket.emit('json_push', json, namespace=web_socket_namespace, room=sid)

def web_server_init():
    """ Initializes the websocket server within a greenthread
    """
    socket_app = socketio.WSGIApp(web_socket, flask_app)
    eventlet.wsgi.server(
        eventlet.listen((misc.WEB_SERVER_IP, misc.WEB_SERVER_PORT)),
        socket_app,
        log=web_server_logger
    )


###################################################################################################
# SET UP WEBSOCKET TO DB SERVER:

db_socket_namespace = '/socketio'
db_socket = socketio.Client()

@db_socket.on('connect', namespace=db_socket_namespace)
def on_db_connect():
    web_server_logger.info('Connected to db_server')
    db_socket.emit('client_response', 'connected', namespace=db_socket_namespace)

@db_socket.on('disconnect', namespace=db_socket_namespace)
def on_db_disconnect():
    web_server_logger.warning('Disconnected from db_server')

@db_socket.on('db_server_push', namespace=db_socket_namespace)
def on_db_message(json):
    web_server_logger.info('Received new realtime.json from db_server')
    db_socket.emit('client_response', 'thanks!', namespace=db_socket_namespace)
    try:
        with open(json_local_fname, 'wb') as json_local:
            json_local.write(json)
    except FileNotFoundError:
        web_server_logger.info('%s does not exist, will create it', misc.DATA_PATH)
        os.makedirs(misc.DATA_PATH)
        with open(json_local_fname, 'wb') as json_local:
            json_local.write(json)

    web_socket.emit('json_push', json, namespace=db_socket_namespace)


def connect_to_db_server(attempt=0):
    """ sets up websocket connection to db server and listens in a greenthread
    """
    try:
        db_socket.connect(f'http://{misc.DB_SERVER_IP}:{misc.DB_SERVER_WEBSOCKET_PORT}', namespaces=[db_socket_namespace])
    except socketio.exceptions.ConnectionError:
        if attempt < 5:
            print('FAIL')
            web_server_logger.warning('failed to connect to http://%s:%s',misc.DB_SERVER_IP,misc.DB_SERVER_WEBSOCKET_PORT)
            eventlet.greenthread.sleep(2)
            connect_to_db_server(attempt=attempt+1)
        else:
            web_server_logger.error('failed to connect to db server 5 times, exiting')
            print('failed to connect to db server 5 times')
            exit()
    else:
        eventlet.greenthread.spawn(db_socket.wait)

###################################################################################################


def main():
    print(f'Logging in {misc.LOG_PATH}')

    connect_to_db_server()
    eventlet.greenthread.sleep(1)

    eventlet.greenthread.spawn(web_server_init)

    while not KILL_SERVER:
        try:
            eventlet.greenthread.sleep(1)
        except KeyboardInterrupt:
            print('KeyboardInterrupt, exiting')
            exit()


if __name__ == "__main__":
    main()
