"""Schedules static and realtime functions.
Then, starts websocket server to push realtime.json to clients
"""
import os
import logging
import schedule
import types

import socketio
import eventlet

import transit_system_config
import misc
from misc import parser_logger, server_logger
import static
import realtime

eventlet.monkey_patch()

#Stop 'schedule' from logging info every few seconds
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(logging.WARNING)

json_filename = f'{transit_system_config.MTA_SETTINGS.realtime_json_path}/realtime.json'

KILL_SERVER = False


###################################################################################################
# socket_io methods:

sio = socketio.Server(async_mode='eventlet')
socket_namespace = '/socketio'

@sio.on('connect', namespace=socket_namespace)
def connect(sid, environ):
    server_logger.info('Client connected: %s', sid)

@sio.on('client_response', namespace=socket_namespace)
def client_response(sid, data):
    server_logger.info('Client response: %s', data)

@sio.on('disconnect', namespace=socket_namespace)
def disconnect(sid):
    server_logger.info('Client disconnected: %s', sid)

def socketio_init():
    server_logger.info('Starting eventlet server @ %s:%s', misc.IP, misc.PORT)
    app = socketio.WSGIApp(sio)
    eventlet.wsgi.server(eventlet.listen((misc.IP, misc.PORT)), app, log=server_logger)

def socketio_push(json):
    server_logger.info('Pushing new data to clients')
    sio.emit('db_server_push', json, namespace=socket_namespace)

###################################################################################################
realtime_in_progress = False

def realtime_parse_and_push():
    global realtime_in_progress
    if realtime_in_progress:
        print('whoa there, realtime.main is already running!')
        exit()
    realtime_in_progress = True

    realtime_is_new = realtime.main()
    if realtime_is_new:
        with open(json_filename, 'r') as json_file:
            json = json_file.read()
            socketio_push(json)

    realtime_in_progress = False

def schedule_runner():
    parser_logger.info('Starting scheduler')
    while True:
        schedule.run_pending()
        eventlet.greenthread.sleep(1)


def main():
    print(f'Logging in {misc.LOG_PATH}')
    parser_logger.info('\n~~~~~~~~~~~~ BEGINNING transit_data_access Database Server processes... ~~~~~~~~~~~~\n')

    # Reset the timestamp file that stores the realtime feed timestamp for comparisons
    latest_timestamp_file = transit_system_config.MTA_SETTINGS.realtime_data_path+'/latest_feed_timestamp.txt'
    if os.path.exists(latest_timestamp_file):
        os.remove(latest_timestamp_file)

    # Start the server
    eventlet.greenthread.spawn(socketio_init)

    # Set the schedule for realtime and static
    schedule.every(misc.REALTIME_FREQ).seconds.do(eventlet.greenthread.spawn, realtime_parse_and_push)
    schedule.every().day.at("03:30").do(eventlet.greenthread.spawn, static.main)

    # Check if there's a new static feed
    parser_logger.info('Running static.main() once before scheduler begins')
    static.main()

    # Start the scheduler
    eventlet.greenthread.spawn(schedule_runner)
    parser_logger.info('Scheduler begun.')

    while not KILL_SERVER:
        try:
            eventlet.greenthread.sleep(1)
        except KeyboardInterrupt:
            print('KeyboardInterrupt, exiting')
            exit()

if __name__ == "__main__":
    main()
