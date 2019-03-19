"""Miscellaneous modules and classes for the package
Also sets up logging
"""
import os
import logging
import threading

# CONSTANTS
PACKAGE_NAME = 'transit_data_access'
DATA_PATH = f'/data/{PACKAGE_NAME}/web_server'
LOG_PATH = f'/var/log/{PACKAGE_NAME}/web_server'
LOG_LEVEL = logging.INFO
REALTIME_FREQ = 3 # realtime GTFS feed will be checked every REALTIME_FREQ seconds

SERVER_CONF = []
with open('server.conf') as conf_file:
    for line in conf_file:
        line = line.split('#')[0] # removes comments
        line = line.strip().split('=') # converts a line in the form 'a = b' to [a, b]
        SERVER_CONF[line[0]] = line[1]

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
    log_file = 'web_server.log'
    log_format = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'
    log_formatter = logging.Formatter(fmt=log_format, datefmt=log_date_format)

    server_logger = logging.getLogger('web-server')
    server_logger.setLevel(LOG_LEVEL)
    server_file_handler = logging.FileHandler(f'{LOG_PATH}/{log_file}')
    server_file_handler.setFormatter(log_formatter)
    server_logger.addHandler(server_file_handler)


except PermissionError:
    print(f'Don\'t have permission to write to log files in: {LOG_PATH}')
    exit()

####################################################################################


# PACKAGE METHODS AND CLASSES
def run_threaded(job_func, **kwargs):
    job_thread = threading.Thread(target=job_func,kwargs=kwargs)
    job_thread.start()
