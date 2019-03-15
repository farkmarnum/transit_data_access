#!/usr/bin/python
"""Schedules static and realtime functions.
    TODO: replace this with a cron-based approach?
"""
import os
import threading
import logging
import time
import schedule
import socket

import ts_config
import misc
import static_parse
import realtime_parse

#Stop 'schedule' from logging info every few seconds
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(logging.WARNING)

GTFS_logger = misc.GTFS_logger
server_logger = misc.server_logger

HOST = misc.SERVER_IP
PORT = misc.SERVER_PORT
_IPv4 = socket.AF_INET
_TCP = socket.SOCK_STREAM


def server_init():
    """ sets up server

    1) bind to a port a start listening for incoming connections
    2) when a new connection is made with a client (from one of my web servers), keep it open
        2a) SSL or something to make it secure?
    3) maintain a record of all current connections somewhere
    """
    server_logger.info('\n~~~~~~~~~~~~ BEGINNING SERVER ~~~~~~~~~~~~\n')
    with socket.socket(_IPv4, _TCP) as socket_:
        socket_.bind((HOST,PORT))
        socket_.listen()
        connection_, address_ = socket_.accept()
        with connection_:
            server_logger.info('Connected by %s', address_)
            out_ = f'Thanks for connecting, {address_}\n'.encode('utf-8')
            connection_.send(out_)
            data = connection_.recv(1024)


def server_push():
    """ every time realtime.json changes, send it to all the connected clients
        make sure files are sent and received fully & correctly
    """
    server_logger.info('Pushing new realtime.json.gz')


def realtime_parse_and_push_if_new():
    feed_is_new = realtime_parse.main()
    if feed_is_new:
        server_push()


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def schedule_runner():
    while True:
        schedule.run_pending()
        time.sleep(0.5)

def main():
    print(f'Logs can be found in {misc.LOG_PATH}')

    GTFS_logger.info('\n~~~~~~~~~~~~ BEGINNING GTFS parse scheduer ~~~~~~~~~~~~\n')

    latest_timestamp_file = ts_config.MTA_SETTINGS.realtime_data_path+'/latest_feed_timestamp.txt'
    if os.path.exists(latest_timestamp_file):
        os.remove(latest_timestamp_file)

    schedule.every(misc.REALTIME_FREQ).seconds.do(run_threaded, realtime_parse_and_push_if_new)
    schedule.every().day.at("03:30").do(run_threaded, static_parse.main)

    GTFS_logger.info('Running static.main() once before scheduler')
    static_parse.main()

    #GTFS_logger.info('Running realtime.main() once before scheduler')
    #realtime_parse.main()

    GTFS_logger.info('Starting scheduler')
    run_threaded(schedule_runner)

    run_threaded(server_init)

if __name__ == "__main__":
    main()
