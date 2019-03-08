#!/usr/bin/python3
import static
import transit_systems as ts_list
import realtime

def is_loaded(ts_name):
    # check if the database is already populated for this ts
    return False

def pull(ts, name_):
    new_ts = ts(name_)
    new_ts.update()
    new_ts.build()
    new_ts.display()
    return new_ts

def main():
    ts_name = 'MTA_subway'

    if not is_loaded('MTA_subway'):
        mta = pull(ts_list.MTASubway, 'MTA_subway')

if __name__ == "__main__":
    main()
