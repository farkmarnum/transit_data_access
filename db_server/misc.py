"""Miscellaneous modules and classes for the package
Also sets up logging
"""
import time
import os
import types
import logging
import multiprocessing
from flask import Flask, render_template
from flask_socketio import SocketIO


# CONSTANTS
PACKAGE_NAME = 'transit_data_access'
DATA_PATH = f'/data/{PACKAGE_NAME}/db_server'
LOG_PATH = f'/var/log/{PACKAGE_NAME}/db_server'
LOG_LEVEL = logging.INFO
REALTIME_FREQ = 3 # realtime GTFS feed will be checked every REALTIME_FREQ seconds

IP = '127.0.0.1'
PORT = 65432

TIMEOUT = 6.2

####################################################################################
# LOG SETUP
if not os.path.exists(LOG_PATH):
    try:
        print(f'Creating log path: {LOG_PATH}')
        os.makedirs(LOG_PATH)
    except PermissionError:
        print(f'Don\'t have permission to create log path: {LOG_PATH}')
        exit()

try:
    log_file_1 = 'parser.log'
    log_file_2 = 'db-server.log'
    log_format = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'
    log_formatter = logging.Formatter(fmt=log_format, datefmt=log_date_format)

    parser_logger = logging.getLogger('parser')
    parser_logger.setLevel(LOG_LEVEL)
    parser_file_handler = logging.FileHandler(f'{LOG_PATH}/{log_file_1}')
    parser_file_handler.setFormatter(log_formatter)
    parser_logger.addHandler(parser_file_handler)

    server_logger = logging.getLogger('server')
    server_logger.setLevel(LOG_LEVEL)
    server_file_handler = logging.FileHandler(f'{LOG_PATH}/{log_file_2}')
    server_file_handler.setFormatter(log_formatter)
    server_logger.addHandler(server_file_handler)


except PermissionError:
    print(f'Don\'t have permission to write to log files in: {LOG_PATH}')
    exit()

####################################################################################

# PACKAGE METHODS AND CLASSES

def hr_min_sec(total_seconds):
    hrs = int(total_seconds / 3600)
    mins = int( (total_seconds % 3600) / 60)
    secs = int(total_seconds % 60)
    suffix = ''
    secbuff = ''
    minbuff = ''

    if hrs < 1:
        hrs_ = ''
    else:
        hrs_ = str(hrs) + ':'
        if mins < 10:
            minbuff = '0'

    if mins < 1:
        mins_ = ''
        if hrs < 1:
            suffix = ' seconds'
    else:
        mins_ = str(mins) + ':'
        if secs < 10:
            secbuff = '0'

    secs_ = str(secs)

    return hrs_ + minbuff + mins_ + secbuff + secs_ + suffix

def trip_to_shape(trip_id, trip_to_shape_long_dict=None):
    """Takes a trip_id in form '092200_6..N03R' or 'AFA18GEN-1037-Sunday-00_000600_1..S03R', and returns what's after the last underscore.
    This should be the shape_id ('6..N03R')

    UNFORTUNATELY the MTA sucks and in most of the realtime feeds the trip.trip_id is in the form '092234_6..N' (with no shape indication)
    If that's the case, the trip_to_shape_long_dict is used, which is in the form: dict[trip_shape_trunc][trip_start_time] = shape_id
    """
    if trip_id[-1] not in 'NS': # if this is a good trip_id with shape information in it

        shape_id = trip_id.split('_').pop()
        if 'X' in shape_id: # I have no idea why a small subset of the shape_ids are like this... but they are. thanks MTA
            shape_id = shape_id.split('X')[0]+'R'

        if shape_id == '1..S02R':
            return '1..S03R'
        elif shape_id == '1..N02R':
            return '1..N03R'
        return shape_id

    else: # this is a baaaaaad trip_id. bad MTA, bad!

        if trip_to_shape_long_dict:
            start_time, truncated_shape_id = trip_id.split('_')[-2:] # '092234', '6..N'
            start_time = int(start_time)

            try:
                shape_id = trip_to_shape_long_dict[truncated_shape_id][start_time]
                if 'X' in shape_id: # I have no idea why a few of the 'shape_id's are like this... but they are. thanks MTA
                    shape_id = shape_id.split('X')[0]+'R'
                return shape_id

            except KeyError:
                try:
                    adj_start_time = min(
                        trip_to_shape_long_dict[truncated_shape_id].keys(),
                        key=lambda x: min(
                            abs(x-start_time),
                            abs(x-(start_time+100*60*24))
                        )
                    )
                    parser_logger.debug('trip_to_shape(): couldn\'t find a shape_id for %s with start_time=%s, next best is %s which gives %s', truncated_shape_id, start_time, adj_start_time, trip_to_shape_long_dict[truncated_shape_id][adj_start_time])
                    return trip_to_shape_long_dict[truncated_shape_id][adj_start_time]

                except (ValueError, KeyError):
                    parser_logger.debug('couldn\'t find a shape_id for %s', truncated_shape_id)
                    return None

        else: # trip_to_shape_long_dict was not provided
            return None

class NestedDict(dict):
    """A dict that automatically creates new dicts within itself as needed"""
    def __getitem__(self, key):
        if key in self:
            return self.get(key)
        return self.setdefault(key, NestedDict())

class TimeLogger():
    """ Convenient little way to log how long something takes
    Usage:

    with TimeLogger() as _tl:
        # BLOCK 1
        _tl.log_time()
        # BLOCK 2
        _tl.log_time()
        # BLOCK 3

    # parser_logger will then
    """
    def __init__(self):
        self.times = []

    def __enter__(self):
        self.log_time()
        return self

    def log_time(self, block_name=''):
        self.times.append( (time.time(), block_name) )

    def __exit__(self, exc_type, exc_val, exc_tb):
        prev_time, _ = self.times.pop(0)
        while len(self.times) > 0:
            time_, block_name = self.times.pop(0)
            block_time = time_ - prev_time
            parser_logger.info('%s took %s seconds', block_name, block_time)
            prev_time = time_
