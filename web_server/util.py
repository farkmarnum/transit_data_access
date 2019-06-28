import time
import os
import logging
import logging.config
import redis

PACKAGE_NAME = 'transit_data_access'
DATA_PATH = f'/data/'

LOG_LEVEL: str = os.environ.get('LOG_LEVEL', 'INFO')

PARSER_SOCKETIO_HOST: str = os.environ.get('PARSER_SOCKETIO_HOST', 'parser')
PARSER_SOCKETIO_PORT: int = int(os.environ.get('PARSER_SOCKETIO_PORT', 45654))
SOCKETIO_CONECTION_MAX_ATTEMPTS = 5

REDIS_HOST: str = os.environ.get('REDIS_HOST', 'redis_server')
REDIS_PORT: int = int(os.environ.get('REDIS_PORT', 6379))

WEB_SERVER_HOST = '0.0.0.0'
WEB_SERVER_PORT = 9000

logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s.%(msecs)03d %(module)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'handlers': {
        'stream': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        'web': {
            'level': LOG_LEVEL,
            'handlers': ['stream']
        }
    }
})

log = logging.getLogger('web')

redis_server = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

class TimeLogger:
    """ Convenient little way to log how long something takes.
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
            log.info('%s took %s seconds', block_name, block_time)
            prev_time = time_
