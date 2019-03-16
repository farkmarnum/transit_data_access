"""Miscellaneous modules and classes for the package
Also sets up logging
"""
import time
import os
import logging
import threading

# CONSTANTS
PACKAGE_NAME = 'gtfs_parser'
DATA_PATH = '/data/GTFS'
LOG_PATH = f'/var/log/{PACKAGE_NAME}'
LOG_LEVEL = logging.INFO
REALTIME_FREQ = 3 # realtime GTFS feed will be checked every {REALTIME_FREQ} seconds
SERVER_IP = '127.0.0.1'
SERVER_PORT = 64299

# LOG SETUP
if not os.path.exists(LOG_PATH):
    try:
        print(f'Creating log path: {LOG_PATH}')
        os.makedirs(LOG_PATH)
    except PermissionError:
        print(f'Don\'t have permission to create log path: {LOG_PATH}')
        exit()

log_file_1 = 'GTFS.log'
log_file_2 = 'server.log'
log_format = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
log_date_format = '%Y-%m-%d %H:%M:%S'
log_formatter = logging.Formatter(fmt=log_format, datefmt=log_date_format)

GTFS_logger = logging.getLogger('GTFS')
GTFS_logger.setLevel(LOG_LEVEL)
GTFS_file_handler = logging.FileHandler(f'{LOG_PATH}/{log_file_1}')
GTFS_file_handler.setFormatter(log_formatter)
GTFS_logger.addHandler(GTFS_file_handler)

server_logger = logging.getLogger('server')
server_logger.setLevel(LOG_LEVEL)
server_file_handler = logging.FileHandler(f'{LOG_PATH}/{log_file_2}')
server_file_handler.setFormatter(log_formatter)
server_logger.addHandler(server_file_handler)

# PACKAGE METHODS AND CLASSES
def run_threaded(job_func, **kwargs):
    print(f'running {job_func} now in a thread')
    job_thread = threading.Thread(target=job_func,kwargs=kwargs)
    job_thread.start()

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
       GTFS_logger.debug('%s completed, took %s seconds\n', self.message_text, time.time()-self.start_time)
