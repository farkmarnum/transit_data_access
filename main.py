#!/usr/bin/python3
"""Example usage of gtfs_parser methods
"""
import logging
import static
import realtime

logging.basicConfig(level=logging.WARNING)

def main():
    """ Runs static.py and then realtime.py
    """
    static.main()
    realtime.main()

if __name__ == "__main__":
    main()
