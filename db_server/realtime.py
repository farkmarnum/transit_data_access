""" Downloads realtime GTFS data, checks if it's new, parses it, and stores it
"""

import os
import eventlet
from typing import Union
from eventlet.green.urllib import error as urllib_error
from eventlet.green.urllib import request
from google.transit import gtfs_realtime_pb2
import util as ut
from gtfs_conf import GTFS_CONF

REALTIME_RAW_PATH = f'{ut.DATA_PATH}/{GTFS_CONF.name}/{ut.REALTIME_RAW_SUFFIX}'
REALTIME_PARSED_PATH = f'{ut.DATA_PATH}/{GTFS_CONF.name}/{ut.REALTIME_PARSED_SUFFIX}'

class RealtimeFeedHandler():
    def eventlet_fetch(self, url, attempt=0) -> ut.RealtimeFetchResult:
        """ async fetch url
        """
        max_attempts = 2
        with eventlet.Timeout(ut.TIMEOUT):
            try:
                with request.urlopen(url) as response:
                    feed = response.read()
                    feed_message = gtfs_realtime_pb2.FeedMessage()
                    feed_message.ParseFromString(feed)
                    print(feed_message.header.timestamp, end=' ')
                    return ut.RealtimeFetchResult(feed_fetched=True)

            except (OSError, urllib_error.URLError, eventlet.Timeout) as err:
                if attempt < max_attempts:
                    ut.parser_logger.info('%s: unable to connect to %s, RETRYING', err, url)
                    return self.eventlet_fetch(url, attempt + 1)
                ut.parser_logger.error('%s: unable to connect to %s, FAILED after %s attempts',
                                        err, url, attempt + 1)
                return ut.RealtimeFetchResult(feed_fetched=False)

    def run(self):
        """get all new feeds, check each, and combine
        """
        request_pool = eventlet.GreenPool(20)
        response_list = request_pool.imap(self.eventlet_fetch, self.urls)

        all_feeds = []
        for response in response_list:
            if response.feed_fetched:
                all_feeds.append(response.feed)
            else:
                ut.parser_logger.info('Unable to receive all feeds, exiting get_realtime_feed')
                return ut.FeedStatus(feed_fetched=False)
        
        ut.parser_logger.debug('realtime.py: received all feeds')

    def parse_feed(self):
        """ Parses the feed into the smallest possible representation of the necessary realtime data.
        """
        pass

    def load_latest_timestamp(self) -> Union[float, None]:
        latest_timestamp_file = f'{REALTIME_RAW_PATH}/latest_timestamp.txt'
        try:
            with open(latest_timestamp_file, 'r+') as latest_timestamp_infile:
                return float(latest_timestamp_infile.read())

        except FileNotFoundError:
            ut.parser_logger.info('%s/latest_timestamp.txt does not exist, will create it', REALTIME_RAW_PATH)
            try:
                os.makedirs(REALTIME_RAW_PATH, exist_ok=True)
                return None
            except PermissionError:
                ut.parser_logger.error('Don\'t have permission to write to %s', REALTIME_RAW_PATH)
                raise PermissionError

    def __init__(self):
        self.latest_timestamp = self.load_latest_timestamp()
        self.urls = GTFS_CONF.realtime_urls
        pass




class RealtimeSupervisor():
    """docstring for RealtimeSupervisor"""
    def __init__(self, arg):
        self.arg = arg
        

def get_feed():
    feed_handler = RealtimeFeedHandler()
    feed_handler.run()
