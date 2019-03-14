"""Miscellaneous modules and classes for the gtfs_parser package
Also sets the logging level
"""
import time
import os
import logging

LOG_PATH = 'logs'
LOG_FILE_NAME = 'gtfs_parser.log'
LOG_FORMAT = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)

logging.basicConfig(
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        #logging.FileHandler(f'{LOG_PATH}/{LOG_FILE_NAME}'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('gtfs_parser')
logger.setLevel(logging.INFO)


def trip_to_shape(trip_id):
    """Takes a trip_id in form '092200_6..N03R' and returns what's after the last underscore
    This should be the shape_id ('6..N03R')
    """
    return trip_id.split('_').pop()


class NestedDict(dict):
    """A dict that automatically creates new dicts within itself as needed"""
    def __getitem__(self, key):
        if key in self:
            return self.get(key)
        return self.setdefault(key, NestedDict())


class TimeLogger():
    """ Convenient little way to log how long something takes
    Usage: with TimeLogger('process name') as _tl:
    """
    def __init__(self, message_text):
        self.message_text = message_text
        self.start_time = None

    def add_to_message(self, additional_text):
        self.message_text.append(additional_text)

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug('%s completed, took %s seconds\n', self.message_text, time.time()-self.start_time)
