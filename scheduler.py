#!/usr/bin/python
"""Example usage of gtfs_parser methods
"""
import os
import threading
import logging
import time
import schedule

import ts_config
from misc import logger
import static
import realtime

#Stop schedule from logging every 2 seconds...
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(logging.WARNING)


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


def main():
    logger.info('\n~~~~~~~~~~~~ BEGINNING GTFS_PARSER ~~~~~~~~~~~~\n')

    latest_timestamp_file = ts_config.MTA_SETTINGS.realtime_data_path+'/latest_feed_timestamp.txt'
    if os.path.exists(latest_timestamp_file):
        os.remove(latest_timestamp_file)
    
    schedule.every(2).seconds.do(run_threaded, realtime.main)
    schedule.every().day.at("03:30").do(run_threaded, static.main)

    logger.info('starting static.main()')
    static.main()

    logger.info('starting realtime.main()')
    realtime.main()

    logger.info('starting schedule')
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
