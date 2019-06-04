""" This contains all utility functions and package variables.
"""
import os
import logging
from typing import NamedTuple, NewType, Union, List, Optional

# CONSTANTS
PACKAGE_NAME = 'transit_data_access'
DATA_PATH = f'/data/{PACKAGE_NAME}/db_server'

REALTIME_RAW_SUFFIX = f'realtime/raw'
REALTIME_PARSED_SUFFIX = f'realtime/parsed'
STATIC_TMP_SUFFIX = f'static/tmp'
STATIC_RAW_SUFFIX = f'static/raw'
STATIC_PARSED_SUFFIX = f'static/parsed'

LOG_PATH = f'/var/log/{PACKAGE_NAME}/db_server'
LOG_LEVEL = logging.INFO

IP = '127.0.0.1'
PORT = 3333

REALTIME_FREQ = 3
TIMEOUT = 3.2

RT_FEED_FAIL, RT_FEED_IS_OLD, RT_FEED_IS_NEW = list(range(3))

RealtimeFeed = NewType('RealtimeFeed', bytes)
StaticFeed = NewType('StaticFeed', bytes)

OptionalInt = Optional[Union[int, None]]
OptionalStr = Optional[Union[str, None]]
OptionalRealtimeFeed = Optional[Union[RealtimeFeed, None]]


# CUSTOM TYPES
class FeedStatus(NamedTuple):
    feed_fetched: bool
    feed_is_new: bool
    timestamp_diff: Optional[int]
    error: Optional[str]


class RealtimeFetchResult(NamedTuple):
    feed_fetched: bool
    error: OptionalStr = None
    feed: OptionalRealtimeFeed = None
    timestamp: OptionalInt = None


# LOG SETUP
def log_setup(loggers: list):
    """ Creates paths and files for loggers, given a list of logger objects
    """
    # create log path if possible
    if not os.path.exists(LOG_PATH):
        print(f'Creating log path: {LOG_PATH}')
        try:
            os.makedirs(LOG_PATH)
        except PermissionError:
            print(f'ERROR: Don\'t have permission to create log path: {LOG_PATH}')
            exit()

    # set the formatfor log messages
    log_format = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'
    log_formatter = logging.Formatter(fmt=log_format, datefmt=log_date_format)

    # initialize the logger objects (passed in the 'loggers' param)
    for logger_obj in loggers:
        _log_file = f'{LOG_PATH}/{logger_obj.name}.log'
        try:
            _file_handler = logging.FileHandler(_log_file)
            _file_handler.setFormatter(log_formatter)
        except PermissionError:
            print(f'ERROR: Don\'t have permission to create log file: {_log_file}')
            exit()
        logger_obj.setLevel(LOG_LEVEL)
        logger_obj.addHandler(_file_handler)


parser_logger = logging.getLogger('parser')
server_logger = logging.getLogger('server')
log_setup([parser_logger, server_logger])


class NestedDict(dict):
    """A dict that automatically creates new dicts within itself as needed"""
    def __getitem__(self, key):
        if key in self:
            return self.get(key)
        return set.setdefault(key, NestedDict())


class TimeLogger():
    """ Convenient little way to log how long something takes. Usage:

    with TimeLogger() as _tl:
        # BLOCK 1
        _tl.log_time()
        # BLOCK 2
        _tl.log_time()
        # BLOCK 3
    """
    def __init__(self):
        self.times = []

    def __enter__(self):
        self.tlog()
        return self

    def tlog(self, block_name=''):
        self.times.append((time.time(), block_name))

    def __exit__(self, exc_type, exc_val, exc_tb):
        prev_time, _ = self.times.pop(0)
        while len(self.times) > 0:
            time_, block_name = self.times.pop(0)
            block_time = time_ - prev_time
            parser_logger.info('%s took %s seconds', block_name, block_time)
            prev_time = time_
