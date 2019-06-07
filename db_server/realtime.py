""" Downloads realtime GTFS data, checks if it's new, parses it, and stores it
"""
import time
from typing import NewType
import warnings
import eventlet
from eventlet.green.urllib.error import URLError
from eventlet.green.urllib import request
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
# import matplotlib.pyplot as plt
# import matplotlib.animation as animation
# import numpy as np
import util as ut
from gtfs_conf import GTFS_CONF

REALTIME_RAW_PATH = f'{ut.DATA_PATH}/{GTFS_CONF.name}/{ut.REALTIME_RAW_SUFFIX}'
REALTIME_PARSED_PATH = f'{ut.DATA_PATH}/{GTFS_CONF.name}/{ut.REALTIME_PARSED_SUFFIX}'

FeedStatus = NewType('FeedStatus', int)
NONE, NEW_FEED, OLD_FEED, FETCH_FAILED, DECODE_FAILED, RUNTIME_WARNING = list(range(6))
feed_status_messages = {
    NONE: 'NONE',
    NEW_FEED: 'NEW_FEED',
    OLD_FEED: 'OLD_FEED',
    FETCH_FAILED: 'FETCH_FAILED',
    DECODE_FAILED: 'DECODE_FAILED',
    RUNTIME_WARNING: 'RUNTIME_WARNING'
}

TIME_DIFF_THRESHOLD = 3


class RealtimeFeedHandler():
    def fetch(self, attempt=0):
        """ Fetches url, updates class attributes with feed info. Returns true if feed is new
        """
        with warnings.catch_warnings(), eventlet.Timeout(ut.TIMEOUT):
            try:
                warnings.filterwarnings(action='error', category=RuntimeWarning)
                with request.urlopen(self.url) as response:
                    feed_message = gtfs_realtime_pb2.FeedMessage()
                    feed_message.ParseFromString(response.read())
                    self.timestamp = feed_message.header.timestamp
                    if self.timestamp >= self.latest_timestamp + TIME_DIFF_THRESHOLD:
                        self.prev_feed, self.feed = self.feed, feed_message
                        self.latest_timestamp = self.timestamp
                        self.status = NEW_FEED
                        return True
                    else:
                        self.status = OLD_FEED
                        ut.parser_logger.info('Old feed: %s', self.id_)
                        return False
            except (URLError, OSError) as err:
                self.status = FETCH_FAILED
                ut.parser_logger.error('%s fetching %s', err, self.id_)
            except (DecodeError, SystemError) as err:
                self.status = DECODE_FAILED
                ut.parser_logger.error('%s decoding feed %s', err, self.id_)
            except RuntimeWarning:
                self.status = RUNTIME_WARNING
                ut.parser_logger.warning('RuntimeWarning converting %s', self.id_)
            except eventlet.Timeout:
                self.status = FETCH_FAILED
                ut.parser_logger.error('Timeout fetching %s', self.id_)


        if attempt + 1 < self.max_attempts:
            return self.fetch(attempt=attempt + 1)
        return False

    def __init__(self, url, id_):
        self.url = url
        self.id_ = id_
        self.feed = None
        self.prev_feed = None
        self.timestamp = 0
        self.latest_timestamp = 0
        self.status = FeedStatus(NONE)
        self.max_attempts = 3


class RealtimeManager():
    """docstring for RealtimeManager"""
    def check(self):
        """get all new feeds, check each, and combine
        """
        ut.parser_logger.info('Checking feeds!')
        for feed in self.feeds:
            self.request_pool.spawn(feed.fetch)

        self.request_pool.waitall()
        # feed_ages = {feed.id_: time.time() - feed.latest_timestamp for feed in self.feeds}
        print(list(map(lambda a: int(time.time() - a.timestamp), self.feeds)), end=' ')
        print(sum(map(lambda a: int(a.status == NEW_FEED), self.feeds)), end=' ')
        print(int(100 * sum([time.time() - feed.timestamp for feed in self.feeds]) / len(self.feeds)) / 100)

    def parse_feed(self):
        """ Parses the feed into the smallest possible representation of the necessary realtime data.
        """
        pass

    def __init__(self):
        self.request_pool = eventlet.GreenPool(len(GTFS_CONF.realtime_urls))
        self.feeds = [RealtimeFeedHandler(url, id_) for id_, url in GTFS_CONF.realtime_urls.items()]
        print(list(map(lambda a: a.id_, self.feeds)))
        self.merged_feeds = None
        self.merged_feeds_prev = None


if __name__ == "__main__":
    ut.parser_logger.info('Running realtime.py')
    rm = RealtimeManager()

    while True:
        _t = time.time()
        rm.check()
        _t_diff = time.time() - _t
        print(_t_diff)
        eventlet.sleep(ut.REALTIME_FREQ - _t_diff)
