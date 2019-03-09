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

    for i in range(10):
        time_before = time.time()
        realtime_feed = realtime.Feeds(mta)
        print(realtime_feed.timestamp('1'))
        time_after = time.time()
        print(time_after - time_before)


if __name__ == "__main__":
    main()
