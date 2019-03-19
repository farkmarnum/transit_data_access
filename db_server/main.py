#!/usr/bin/python
"""Schedules static and realtime functions.
    TODO: replace this with a cron-based approach?
"""
import os
import logging
import time
import schedule

import transit_system_config
import misc
import static_parse
import realtime_parse
import server_setup

#Stop 'schedule' from logging info every few seconds
schedule_logger = logging.getLogger('schedule')
schedule_logger.setLevel(logging.WARNING)

parser_logger = misc.parser_logger

def schedule_runner():
    parser_logger.info('Starting scheduler')
    while True:
        schedule.run_pending()
        time.sleep(1)

def main():
    print(f'Logging in {misc.LOG_PATH}')
    parser_logger.info('\n~~~~~~~~~~~~ BEGINNING transit_data_access Database Server processes... ~~~~~~~~~~~~\n')

    # Reset the timestamp file that stores the realtime feed timestamp for comparisons
    latest_timestamp_file = transit_system_config.MTA_SETTINGS.realtime_data_path+'/latest_feed_timestamp.txt'
    if os.path.exists(latest_timestamp_file):
        os.remove(latest_timestamp_file)

    # Set the schedule for realtime_parse and static_parse
    schedule.every(misc.REALTIME_FREQ).seconds.do(misc.run_threaded, realtime_parse.main)
    schedule.every().day.at("03:30").do(misc.run_threaded, static_parse.main)

    # Check if there's a new static feed
    parser_logger.info('Running static.main() once before scheduler begind')
    static_parse.main()

    # Start the server
    misc.run_threaded(server_setup.main)

    # Start the scheduler
    misc.run_threaded(schedule_runner)
    parser_logger.info('Scheduler begun.')

if __name__ == "__main__":
    main()
