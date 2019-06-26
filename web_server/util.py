import os
import logging
import logging.config

PACKAGE_NAME = 'transit_data_access'
DATA_PATH = f'/data/'

LOG_LEVEL: str = os.environ.get('LOG_LEVEL', 'INFO')

PARSER_SOCKETIO_HOST: str = os.environ.get('PARSER_SOCKETIO_HOST', 'parser')
PARSER_SOCKETIO_PORT: int = int(os.environ.get('PARSER_SOCKETIO_PORT', 45654))
SOCKETIO_CONECTION_MAX_ATTEMPTS = 5

# REDIS_HOST: str = os.environ.get('REDIS_HOST', 'redis_server')
# REDIS_PORT: int = int(os.environ.get('REDIS_PORT', 6379))

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
