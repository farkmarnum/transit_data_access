""" Downloads realtime GTFS data, checks if it's new, parses it, and stores it
"""
import time
import os
from typing import NamedTuple, NewType, Union, Any, Optional  # noqa
import warnings
import json
import eventlet
from eventlet.green.urllib.error import URLError
from eventlet.green.urllib import request
# from google.transit import gtfs_realtime_pb2              # type: ignore
from google.transit.gtfs_realtime_pb2 import FeedMessage    # type: ignore
from google.protobuf.message import DecodeError
# import matplotlib.pyplot as plt
# import matplotlib.animation as animation
# import numpy as np
import util as u
from gtfs_conf import GTFS_CONF


TIME_DIFF_THRESHOLD = 3


FetchStatus = NewType('FetchStatus', int)
NONE, NEW_FEED, OLD_FEED, FETCH_FAILED, DECODE_FAILED, RUNTIME_WARNING = list(map(FetchStatus, range(6)))


class FetchResult(NamedTuple):
    status: FetchStatus
    timestamp: int = 0
    error: str = ''


class RealtimeFeedHandler():
    """ TODO: docstring
    """

    def fetch(self, attempt=0):
        """ Fetches url, updates class attributes with feed info.
        """
        with warnings.catch_warnings(), eventlet.Timeout(u.REALTIME_TIMEOUT):
            warnings.filterwarnings(action='error', category=RuntimeWarning)
            try:
                with request.urlopen(self.url) as response:
                    feed_message = FeedMessage()
                    feed_message.ParseFromString(response.read())
                    timestamp = feed_message.header.timestamp
                    if timestamp >= self.latest_timestamp + TIME_DIFF_THRESHOLD:
                        self.result = FetchResult(NEW_FEED, timestamp=timestamp)
                        self.prev_feed, self.latest_feed, self.latest_timestamp = \
                            self.latest_feed, feed_message, timestamp
                    else:
                        self.result = FetchResult(OLD_FEED)
                    return
            except (URLError, OSError) as err:
                self.result = FetchResult(FETCH_FAILED, error=err)
            except (DecodeError, SystemError) as err:
                self.result = FetchResult(DECODE_FAILED, error=err)
            except RuntimeWarning as err:
                self.result = FetchResult(RUNTIME_WARNING, error=err)
            except eventlet.Timeout as err:
                self.result = FetchResult(FETCH_FAILED, error=f'TIMEOUT of {err}')

        if attempt + 1 < u.MAX_ATTEMPTS:
            print('retry!', self.result)
            self.fetch(attempt=attempt + 1)

    def __init__(self, url, id_):
        self.url: str = url
        self.id_: str = id_
        self.result: FetchResult = FetchResult(NONE)
        self.latest_timestamp: int = 0
        self.latest_feed: FeedMessage = None
        self.prev_feed: FeedMessage = None


class RealtimeManager():
    """docstring for RealtimeManager
    """
    def __init__(self) -> None:
        self.request_pool = eventlet.GreenPool(len(GTFS_CONF.realtime_urls))
        self.feed_handlers = [RealtimeFeedHandler(url, id_) for id_, url in GTFS_CONF.realtime_urls.items()]

        self.feed: FeedMessage = None
        self.data: u.RealtimeData = None        # type: ignore
        self.prev_data: u.RealtimeData = None   # type: ignore
        self.average_timestamp: int

    def fetch_all(self) -> None:
        """get all new feeds, check each, and combine
        """
        u.parser_logger.info('Checking feeds!')
        for fh in self.feed_handlers:
            self.request_pool.spawn(fh.fetch)
        self.request_pool.waitall()

        for fh in self.feed_handlers:
            if fh.result.status not in [NEW_FEED, OLD_FEED]:
                u.parser_logger.error('Encountered %s when fetching feed %s', fh.result.error, fh.id_)

        self.average_timestamp = int(sum([fh.latest_timestamp for fh in self.feed_handlers]) / len(self.feed_handlers))
        new_feeds = sum([int(fh.result.status == NEW_FEED) for fh in self.feed_handlers])
        u.parser_logger.info('%s new feeds', new_feeds)
        if new_feeds < 1:
            raise u.UpdateFailed('No new feeds.')

    def merge_feeds(self) -> None:
        """ Parses the feed into the smallest possible representation of the necessary realtime data.
        """
        full_feed = FeedMessage()
        for fh in self.feed_handlers:
            try:
                full_feed.MergeFrom(fh.latest_feed)
            except (ValueError, TypeError):
                try:
                    full_feed.MergeFrom(fh.prev_feed)
                except (ValueError, TypeError) as err:
                    raise u.UpdateFailed('Could not merge feed', fh.id_, err)
            self.feed = full_feed

    def load_static(self) -> None:
        """Loads the static.json file into self.data
        """
        static_json = u.STATIC_PARSED_PATH + '/static.json'
        with open(static_json, mode='r') as static_json_file:
            static_data = json.loads(static_json_file.read(), cls=u.StaticJSONDecoder)
            self.data = u.RealtimeData(
                name=static_data.name,
                static_timestamp=static_data.static_timestamp,
                routes=static_data.routes,
                stations=static_data.stations,
                routehash_lookup=static_data.routehash_lookup,
                stationhash_lookup=static_data.stationhash_lookup,
                transfers=static_data.transfers,
                average_realtime_timestamp=self.average_timestamp,
                trips={}
            )

    def parse(self) -> None:
        for elem in self.feed.entity:
            if elem.HasField('trip_update'):
                trip_hash = u.short_hash(elem.trip_update.trip.trip_id, u.TripHash)
                route_hash = u.short_hash(elem.trip_update.trip.route_id, u.RouteHash)
                try:
                    last_stop_id = elem.trip_update.stop_time_update[-1].stop_id
                except IndexError:
                    pass
                final_station = self.data.stationhash_lookup[last_stop_id]
                branch = u.Branch(route_hash, final_station)
                self.data.trips[trip_hash] = u.Trip(
                    id_=trip_hash,
                    branch=branch,
                    arrivals={}
                )

                for stop_time_update in elem.trip_update.stop_time_update:
                    try:
                        station_hash = self.data.stationhash_lookup[stop_time_update.stop_id]
                    except KeyError:
                        # print(stop_time_update.stop_id)
                        continue
                    arrival_time = u.ArrivalTime(stop_time_update.arrival.time)
                    if arrival_time < time.time():
                        continue
                    self.data.stations[station_hash].add_arrival(branch, arrival_time, trip_hash)
                    self.data.trips[trip_hash].add_arrival(station_hash, arrival_time)

    def serialize(self, attempt=0):
        """ Stores self.data in u.STATIC_PARSED_PATH+'/static.json'
        """
        json_path = u.REALTIME_PARSED_PATH

        try:
            with open(json_path + '/realtime.json', 'w') as out_file:
                json.dump(self.data, out_file, cls=u.RealtimeJSONEncoder)
            u.parser_logger.info('Wrote parsed static data to %s/realtime.json', json_path)

        except OSError as err:
            if attempt != 0:
                u.parser_logger.error('Unable to write to %s/realtime.json', json_path)
                raise u.UpdateFailed(err)

            u.parser_logger.info('%s/realtime.json does not exist, attempting to create it', json_path)

            try:
                os.makedirs(json_path)
            except PermissionError as err:
                u.parser_logger.error('Don\'t have permission to create %s', json_path)
                raise u.UpdateFailed(err)
            except FileExistsError as err:
                u.parser_logger.error('The file %s/realtime.json exists, no permission to overwrite', json_path)
                raise u.UpdateFailed(err)

            self.serialize(attempt=attempt + 1)


    def update(self) -> bool:
        try:
            self.prev_data = self.data
            self.fetch_all()
            self.merge_feeds()
            self.load_static()
            self.parse()
            self.serialize()
        except u.UpdateFailed as err:
            self.data = self.prev_data
            print(err)
            u.parser_logger.error(err.__str__)
            return False
        return True



if __name__ == "__main__":
    u.parser_logger.info('~~~~~~~~~~ Running realtime.py ~~~~~~~~~~')
    rm = RealtimeManager()

    while True:
        _t = time.time()
        rm.update()
        _t_diff = time.time() - _t
        eventlet.sleep(u.REALTIME_FREQ - _t_diff)

"""
        # with open('tmp.txt', 'w') as outfile:
        #    outfile.write(str(self.full_feed.entity))
"""
