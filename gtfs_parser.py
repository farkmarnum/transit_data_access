#!/usr/bin/python3
import time
import os

import static
import transit_systems as ts_list
import realtime


def main():
    mta = ts_list.MTASubway('MTA_subway')
    if not mta.is_loaded():
        mta.update_ts()

    mta.build()
    time_before = time.time()
    realtime_feed = realtime.Feeds(mta)
    time_after = time.time()
    print(time_after - time_before)
    
    print(realtime_feed.timestamp('1'))

if __name__ == "__main__":
    main()
