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

DB_IP = '127.0.0.1'
DB_PORT = 65432

WEB_IP = '127.0.0.1'
WEB_PORT = 433

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

    web_server_logger = logging.getLogger('web-server')
    web_server_logger.setLevel(LOG_LEVEL)
    web_server_file_handler = logging.FileHandler(f'{LOG_PATH}/{log_file}')
    web_server_file_handler.setFormatter(log_formatter)
    web_server_logger.addHandler(web_server_file_handler)


except PermissionError:
    print(f'Don\'t have permission to write to log files in: {LOG_PATH}')
    exit()
