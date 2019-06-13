import logging
import os

PACKAGE_NAME = 'transit_data_access'
DATA_PATH = f'/data/{PACKAGE_NAME}/web_server'

LOG_PATH = f'/var/log/{PACKAGE_NAME}/web_server'
LOG_LEVEL = logging.INFO

DB_IP = '127.0.0.1'
DB_PORT = 8000

WEB_IP = ''
WEB_PORT = 0

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

    # set the format for log messages
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


server_logger = logging.getLogger('server')
log_setup([server_logger])
