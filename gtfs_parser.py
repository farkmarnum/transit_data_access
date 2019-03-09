#!/usr/bin/python3
import time
import os
import grequests

import static
import transit_systems as ts_list
import realtime


def main():
    mta = ts_list.MTASubway('MTA_subway')
    if not mta.is_loaded():
        mta.update_ts()

    mta.build()

    with grequests.Session() as s:
        for i in range(10):
            time_before = time.time()
            realtime_feed = realtime.Feeds(mta, s)
            print(realtime_feed.timestamp('1'))
            time_after = time.time()
            print(time_after - time_before)


if __name__ == "__main__":
    main()
