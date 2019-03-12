#!/usr/bin/python3
"""Example usage of gtfs_parser methods
"""
import sys
import time
import requests

import static
import ts_config
import realtime

def main():
    """ testing
    """
    print(time.time())
    mta_handler = ts_config.MTASubwayLoader('MTA_subway')
    if not mta_handler.has_static_data():
        print("LOADING")
        mta_handler.update_ts()

    mta = mta_handler.build_ts()

    #with requests.Session() as session:
    feed_list = {}
    prev = ''
    while True:
        time_before = time.time()
        index = int(time_before*100)
        feed_list[index] = realtime.Feeds(mta_handler)
        time_after = time.time()
        #timestamp = feed_list[index].timestamp()
        #print(f'Got a new feed @ {time_before}, timestamp = {timestamp}. It took {time_after - time_before:2.3f} seconds ',end='')
        #print(f'{index}:{feed_list[index]}')
        print(time_after-time_before)
        #if feed_list[index].data_ == prev:
        #    print('same as last time')
        #else:
        #    print('diff')
        #prev = feed_list[index].data_
        time.sleep(1)

if __name__ == "__main__":
    main()
