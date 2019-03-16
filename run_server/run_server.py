#!/usr/bin/python
"""Schedules static and realtime functions.
    TODO: replace this with a cron-based approach?
"""
import os
import threading
import logging
import time
import schedule

import ts_config
import misc
import static_parse
import realtime_parse
import server_setup

#Stop 'schedule' from logging info every few seconds
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(logging.WARNING)

GTFS_logger = misc.GTFS_logger
server_logger = misc.server_logger

def realtime_parse_and_update_server(tcp_server):
    realtime_feed_is_new = realtime_parse.main()
    if realtime_feed_is_new:
        server_logger.info('Server sending new realtime.json to web servers')

def schedule_runner():
    GTFS_logger.info('Starting scheduler')
    while True:
        schedule.run_pending()
        time.sleep(1)


def main():
    print(f'Logging in {misc.LOG_PATH}')
    GTFS_logger.info('\n~~~~~~~~~~~~ BEGINNING gtfs_parser/run_server ~~~~~~~~~~~~\n')

    latest_timestamp_file = ts_config.MTA_SETTINGS.realtime_data_path+'/latest_feed_timestamp.txt'
    if os.path.exists(latest_timestamp_file):
        os.remove(latest_timestamp_file)

    #server_ = server_setup.FileServer()
    #misc.run_threaded(server_.event_loop)

    schedule.every(misc.REALTIME_FREQ).seconds.do(misc.run_threaded, realtime_parse_and_update_server, file_server=server_)
    schedule.every().day.at("03:30").do(misc.run_threaded, static_parse.main)

    GTFS_logger.info('Running static.main() once before scheduler')
    static_parse.main()

    misc.run_threaded(schedule_runner)

if __name__ == "__main__":
    main()
