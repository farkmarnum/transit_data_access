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
import static
import realtime
import server

#Stop 'schedule' from logging info every few seconds
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(logging.WARNING)

GTFS_logger = misc.GTFS_logger
server_logger = misc.server_logger

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

    schedule.every(misc.REALTIME_FREQ).seconds.do(run_threaded, realtime.main)
    schedule.every().day.at("03:30").do(run_threaded, static.main)

    GTFS_logger.info('Running static.main() once before scheduler')
    static.main()

    GTFS_logger.info('Running realtime.main() once before scheduler')
    realtime.main()

    GTFS_logger.info('Starting scheduler')
    run_threaded(schedule_runner)

    run_threaded(server.main)

if __name__ == "__main__":
    main()
