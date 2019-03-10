#!/usr/bin/python3
"""Example usage of gtfs_parser methods
"""
#import time
#import os

#import static
import transit_systems as ts_list
import realtime

def main():
    """main method
    """
    mta = ts_list.MTASubway('MTA_subway')
    if not mta.is_loaded():
        mta.update_ts()

    mta.build()
    #time_before = time.time()
    realtime_feed = realtime.Feeds(mta)
    #time_after = time.time()
    #print(time_after - time_before)
    #print(realtime_feed.timestamp('1'))
    print(realtime_feed.trains_by_route('2'))

if __name__ == "__main__":
    main()
