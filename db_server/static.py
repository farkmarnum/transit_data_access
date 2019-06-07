""" downloads static GTFS data, checks if it's new, parses it, and stores it
"""

import util as ut
from gtfs_conf import GTFS_CONF


def get_feed() -> ut.FeedStatus:
    """ Fetches a new feed and checks if it's new. If so, stores it. Returns a FeedStatus.
    """
    return ut.FeedStatus(feed_fetched=False, feed_is_new=False, error="TODO lol")


def parse_feed():
    """ parses
    """
    pass


def main():
    pass
