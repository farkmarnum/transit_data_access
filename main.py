#!/usr/bin/python3
"""Example usage of gtfs_parser methods
"""
import logging
logging.basicConfig(level=logging.DEBUG)

import os
#import time

import static
import realtime

from ts_config import mta_settings

def main():
    """ testing
    """
    '''
    static_handler = static.StaticHandler(mta_settings, name='MTA')
    static_handler.update_()
    static_handler.build(force=True)
    '''
    realtime_handler = realtime.RealtimeHandler(mta_settings, name='MTA')
    realtime_handler.get_feed()
    test_file = f'{realtime_handler.gtfs_settings.realtime_json_path}/test_file'
    with open(test_file, 'w') as test_out:
        test_out.write(str(realtime_handler.realtime_data))


if __name__ == "__main__":
    main()
