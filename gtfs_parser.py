#!/usr/bin/python3
import time
import requests

import static
import transit_systems as ts_list
import realtime

def is_loaded(ts_name):
    # check if the database is already populated for this ts
    return True

def main():
    ts_name = 'MTA_subway'
    if is_loaded(ts_name):
        mta = static.load(ts_list.MTASubway, ts_name)
    else:
        mta = static.pull(ts_list.MTASubway, ts_name)
    with requests.Session() as s:
        for i in range(10):
            time_before = time.time()
            realtime_feeds = realtime.Feeds(mta, s)
            time_after = time.time()
            print(time_after - time_before)


if __name__ == "__main__":
    main()
