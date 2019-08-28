""" This script manages the database server
"""
import time
from typing import Dict, Tuple
import redis
import static     # type: ignore
import realtime   # type: ignore
import util as u  # type: ignore


class RedisHandler:
    def __init__(self) -> None:
        self.server: redis.Redis = redis.Redis(host=u.REDIS_HOSTNAME, port=u.REDIS_PORT, db=0)

    def realtime_push(self, current_timestamp: int, data_full: bytes, data_diffs: Dict[int, bytes]) -> None:
        u.log.debug('Pushing the realime data to redis_server')

        self.server.set('realtime:current_timestamp', current_timestamp)
        self.server.set('realtime:data_full', data_full)
        if data_diffs:
            self.server.hmset('realtime:data_diffs', data_diffs)
            if self.server.hlen('realtime:data_diffs') > u.REALTIME_DATA_DICT_CAP:
                self.server.hdel('realtime:data_diffs', min(self.server.hkeys('realtime:data_diffs')))
            assert self.server.hlen('realtime:data_diffs') <= u.REALTIME_DATA_DICT_CAP

        self.server.publish('realtime_updates', 'new_data')
        u.log.debug('published \'new_data\' to realtime_updates')

def connect_to_redis() -> Tuple[realtime.RealtimeManager, redis.Redis]:
    """ establishes a connection to Redis and returns the handler and server
    Retries indefinitely upon failure
    """
    while True:
        try:
            redis_handler = RedisHandler()
            redis_server = redis_handler.server
            realtime_manager = realtime.RealtimeManager(redis_handler)
            return realtime_manager, redis_server
        except redis.exceptions.ConnectionError:
            time.sleep(2)

def main_loop() -> None:
    realtime_manager, redis_server = connect_to_redis()

    time_for_next_static_parse = time_for_next_realtime_parse = time.time()

    while True:
        try:
            if time.time() > time_for_next_static_parse:
                u.log.debug('initiating static parse')
                static_handler = static.StaticHandler(redis_server)
                static_handler.update()
                del static_handler
                time_for_next_static_parse += (60 * 60 * 24)

            if time.time() > time_for_next_realtime_parse:
                u.log.debug('initiating realtime parse')
                realtime_manager.update()
                time_for_next_realtime_parse += 15

            time.sleep(1)

        except redis.exceptions.ConnectionError:
            # if we've lost connection to Redis, reconnect.
            realtime_manager, redis_server = connect_to_redis()

if __name__ == "__main__":
    main_loop()
