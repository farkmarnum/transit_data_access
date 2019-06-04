""" Downloads realtime GTFS data, checks if it's new, parses it, and stores it
"""
from typing import NewType
import warnings
import eventlet
from eventlet.green.urllib.error import URLError
from eventlet.green.urllib import request
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
import util as ut
from gtfs_conf import GTFS_CONF

REALTIME_RAW_PATH = f'{ut.DATA_PATH}/{GTFS_CONF.name}/{ut.REALTIME_RAW_SUFFIX}'
REALTIME_PARSED_PATH = f'{ut.DATA_PATH}/{GTFS_CONF.name}/{ut.REALTIME_PARSED_SUFFIX}'


FeedStatus = NewType('FeedStatus', int)
NONE, NEW_FEED, OLD_FEED, FETCH_FAILED, DECODE_FAILED, RUNTIME_WARNING = list(range(6))
feed_status_messages = {
    NONE: 'NONE',
    NEW_FEED: 'NEW_FEED',
    OLD_FEED: 'old_feed',
    FETCH_FAILED: 'FETCH_FAILED',
    DECODE_FAILED: 'DECODE_FAILED',
    RUNTIME_WARNING: 'RUNTIME_WARNING'
}

TIME_DIFF_THRESHOLD = 2


class RealtimeFeedHandler():
    def fetch(self):
        """ Fetches url, updates class attributes with feed info. Returns true if feed is new
        TODO: add logging
        """
        with request.urlopen(self.url) as response:
            feed_message = gtfs_realtime_pb2.FeedMessage()
            with warnings.catch_warnings() and eventlet.Timeout(ut.TIMEOUT):
                try:
                    feed_message.ParseFromString(response.read())
                    self.timestamp = feed_message.header.timestamp
                    if self.timestamp > self.latest_timestamp + TIME_DIFF_THRESHOLD:
                        self.prev_feed, self.feed = self.feed, feed_message
                        self.latest_timestamp = self.timestamp
                        self.status = NEW_FEED
                        return True
                    else:
                        self.status = OLD_FEED
                except (URLError, eventlet.Timeout) as err:
                    self.status = FETCH_FAILED
                except DecodeError:
                    self.status = DECODE_FAILED
                except RuntimeWarning:
                    self.status = RUNTIME_WARNING
                finally:
                    return False


    def __init__(self, url, id_):
        self.url = url
        self.id_ = id_
        self.feed = None
        self.prev_feed = None
        self.timestamp = 0
        self.latest_timestamp = 0
        self.status = FeedStatus(NONE)


class RealtimeManager():
    """docstring for RealtimeManager"""
    def check(self):
        """get all new feeds, check each, and combine
        """
        # feed_statuses = self.request_pool.imap(lambda handler: handler.fetch(), self.urls.values())
        for feed in self.feeds:
            self.request_pool.spawn(feed.fetch)

        self.request_pool.waitall()

        for feed in self.feeds:
            print(feed.id_, feed_status_messages[feed.status], sep=' ', end=', ')
        print()

    def parse_feed(self):
        """ Parses the feed into the smallest possible representation of the necessary realtime data.
        """
        pass

    def __init__(self):
        self.request_pool = eventlet.GreenPool(len(GTFS_CONF.realtime_urls))
        self.feeds = [RealtimeFeedHandler(url, id_) for id_, url in GTFS_CONF.realtime_urls.items()]
        self.merged_feeds = None
        self.merged_feeds_prev = None


rm = RealtimeManager()
for _ in range(100):
    rm.check()
    eventlet.sleep(1)
