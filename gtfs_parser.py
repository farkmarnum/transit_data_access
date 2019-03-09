#!/usr/bin/python3
import time
import os
import requests

import static
import transit_systems as ts_list
import realtime


def main():
    mta = ts_list.MTASubway('MTA_subway')
    if not mta.is_loaded():
        mta.update_ts()

    mta.build()

    with requests.Session() as s:
        for i in range(10):
            time_before = time.time()
            realtime_feeds = realtime.Feeds(mta, s)
            time_after = time.time()
            print(time_after - time_before)


if __name__ == "__main__":
    main()
