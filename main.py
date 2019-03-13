#!/usr/bin/python3
"""Example usage of gtfs_parser methods
"""
import logging
logging.basicConfig(level=logging.WARNING)

import static
import realtime

from ts_config import mta_settings

def main():
    """ Runs static.py and then realtime.py
    """
    static.main()
    realtime.main()

if __name__ == "__main__":
    main()
