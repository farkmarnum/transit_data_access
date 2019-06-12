""" Downloads realtime GTFS data, checks if it's new, parses it, and stores it
"""
import time
import os
from typing import List, Dict, NamedTuple, NewType, Union, Any, Optional  # noqa
import warnings
import json
import eventlet
import gzip
import bz2
from eventlet.green.urllib.error import URLError
from eventlet.green.urllib import request
# from google.transit import gtfs_realtime_pb2
from google.transit.gtfs_realtime_pb2 import FeedMessage    # type: ignore
import transit_data_access_pb2
from google.protobuf.message import DecodeError
# import matplotlib.pyplot as plt
# import matplotlib.animation as animation
# import numpy as np
import util as u
from gtfs_conf import GTFS_CONF
from server import DatabaseServer

TIME_DIFF_THRESHOLD = 3


FetchStatus = NewType('FetchStatus', int)
NONE, NEW_FEED, OLD_FEED, FETCH_FAILED, DECODE_FAILED, RUNTIME_WARNING = list(map(FetchStatus, range(6)))


class FetchResult(NamedTuple):
    status: FetchStatus
    timestamp: int = 0
    error: str = ''


class RealtimeFeedHandler:
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
    def __init__(self, db_server: DatabaseServer = None) -> None:
        self.request_pool = eventlet.GreenPool(len(GTFS_CONF.realtime_urls))
        self.feed_handlers = [RealtimeFeedHandler(url, id_) for id_, url in GTFS_CONF.realtime_urls.items()]

        self.feed: FeedMessage = None
        self.data: u.RealtimeData = None        # type: ignore
        self.data_diff: u.DataDiff = None
        self.prev_data: u.RealtimeData = None   # type: ignore
        self.average_timestamp: int
        self.db_server = db_server

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
        static_json = u.STATIC_PARSED_PATH + 'static.json'
        with open(static_json, mode='r') as static_json_file:
            static_data = json.loads(static_json_file.read(), cls=u.StaticJSONDecoder)
            self.data = u.RealtimeData(
                name=static_data.name,
                static_timestamp=static_data.static_timestamp,
                routes=static_data.routes,
                stations=static_data.stations,
                routehash_lookup={str(k): v for k, v in static_data.routehash_lookup.items()},
                stationhash_lookup={str(k): v for k, v in static_data.stationhash_lookup.items()},
                transfers={int(k): {int(_k): _v for _k, _v in v.items()} for k, v in static_data.transfers.items()},
                realtime_timestamp=int(time.time()),
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
                    branch=branch
                )

                for stop_time_update in elem.trip_update.stop_time_update:
                    try:
                        station_hash = self.data.stationhash_lookup[stop_time_update.stop_id]
                    except KeyError:
                        continue
                    arrival_time = u.ArrivalTime(stop_time_update.arrival.time)
                    if arrival_time < time.time():
                        continue
                    self.data.trips[trip_hash].add_arrival(station_hash, arrival_time)

            elif elem.HasField('vehicle'):
                timestamp = elem.vehicle.timestamp
                self.data.trips[trip_hash].timestamp = timestamp

                if time.time() - timestamp > 90:
                    trip_hash = u.short_hash(elem.vehicle.trip.trip_id, u.TripHash)
                    self.data.trips[trip_hash].status = u.STOPPED

        self.serialize_to_JSON(self.data, 'realtime.json')


    def serialize_to_JSON(self, data, outfile, attempt=0):
        """ Stores data in outfile with custom JSON encoder u.RealtimeJSONEncoder
        """
        json_path = u.REALTIME_PARSED_PATH

        data_str = json.dumps(data, cls=u.RealtimeJSONEncoder)
        try:
            with open(json_path + outfile, 'w') as out_file:
                out_file.write(data_str)
            u.parser_logger.info('Wrote parsed static data to %s', json_path + outfile)

            # with gzip.open(json_path + outfile + '.gz', 'wb') as f:
            #    b = bytes(data_str, 'utf-8')
            #    f.write(gzip.compress(b, compresslevel=9))

            with bz2.open(json_path + outfile + '.bz2', 'wb') as f:
                b = bytes(data_str, 'utf-8')
                f.write(bz2.compress(b, compresslevel=9))

        except (OSError, FileNotFoundError) as err:
            if attempt != 0:
                u.parser_logger.error('Unable to write to %s', json_path + outfile)
                raise u.UpdateFailed(err)

            u.parser_logger.info('%s does not exist, attempting to create it', json_path + outfile)

            try:
                os.makedirs(json_path)
            except PermissionError as err:
                u.parser_logger.error('Don\'t have permission to create %s', json_path)
                raise u.UpdateFailed(err)
            except FileExistsError as err:
                u.parser_logger.error('The file %s exists, no permission to overwrite', json_path + outfile)
                raise u.UpdateFailed(err)

            self.serialize_to_JSON(attempt=attempt + 1)

    def diff(self) -> None:
        """ docstring for diff()
        """
        if not self.prev_data:
            return None

        new_trips = self.data.trips
        old_trips = self.prev_data.trips

        trip_diff = u.TripDiff(
            deleted=list(set(old_trips) - set(new_trips)),
            added=[new_trips[trip_hash] for trip_hash in (set(new_trips) - set(old_trips))]
        )
        arrivals_diff = u.ArrivalsDiff()
        status_diff   = u.StatusDiff()  # noqa
        branch_diff   = u.BranchDiff()  # noqa

        for trip_hash in (set(new_trips) & set(old_trips)):
            new_trip = self.data.trips[trip_hash]
            old_trip = self.prev_data.trips[trip_hash]

            # Since arrivals is a dict, set(arrivals) is a set of the keys, which are station_hashes:
            new_arrivals = set(new_trip.arrivals)
            old_arrivals = set(old_trip.arrivals)

            # Now we can find the deleted & added arrivals:
            if old_arrivals - new_arrivals:
                arrivals_diff.deleted[trip_hash] = list(old_arrivals - new_arrivals)
            if new_arrivals - old_arrivals:
                arrivals_diff.added[trip_hash] = {station_hash: new_trip.arrivals[station_hash] for station_hash in (new_arrivals - old_arrivals)}

            # Finding the modified arrivals takes a little more work, and we organize them by time_diff, then station, then trip
            #    This is for storage efficiency: most arrivals in a trip
            _intersection = list(old_arrivals & new_arrivals)
            _modified_arrivals = list(filter(lambda station: new_trip.arrivals[station] != old_trip.arrivals[station], _intersection))
            for station_hash in _modified_arrivals:
                time_diff = u.TimeDiff(new_trip.arrivals[station_hash] - old_trip.arrivals[station_hash])
                arrivals_diff.modified[time_diff][trip_hash].append(station_hash)

            # Then, find status & branch changes:
            if new_trip.status != old_trip.status:
                status_diff.modified[trip_hash] = new_trip.status
            if new_trip.branch != old_trip.branch:
                branch_diff.modified[trip_hash] = new_trip.branch

        data_diff = u.DataDiff(
            realtime_timestamp=self.data.realtime_timestamp,
            trips=trip_diff,
            arrivals=arrivals_diff,
            status=status_diff,
            branch=branch_diff
        )
        # print(arrivals_diff)
        self.serialize_to_JSON(data_diff, 'realtime_diff.json')
        # u.parser_logger.info('realtime JSON size:', os.path.getsize(u.REALTIME_PARSED_PATH + 'realtime.json'))
        # u.parser_logger.info('realtime_diff JSON size:', os.path.getsize(u.REALTIME_PARSED_PATH + 'realtime_diff.json'))
        self.data_diff = data_diff

    def full_to_protobuf(self):
        """ doc
        """
        data_full = self.data
        proto_full = transit_data_access_pb2.DataFull()

        proto_full.name = data_full.name
        proto_full.static_timestamp = data_full.static_timestamp
        proto_full.realtime_timestamp = data_full.realtime_timestamp

        for route_hash, route_info in data_full.routes.items():
            proto_full.routes[route_hash].desc = route_info.desc
            proto_full.routes[route_hash].color = route_info.color
            proto_full.routes[route_hash].text_color = route_info.text_color
            proto_full.routes[route_hash].stations[:] = list(route_info.stations)

        for station_hash, station in data_full.stations.items():
            proto_full.stations[station_hash].name = station.name
            proto_full.stations[station_hash].lat = station.lat
            proto_full.stations[station_hash].lon = station.lon
            for other_station_hash, travel_time in station.travel_times.items():
                proto_full.stations[station_hash].travel_times[other_station_hash] = travel_time

        for route_str, route_hash in data_full.routehash_lookup.items():
            proto_full.routehash_lookup[route_str] = route_hash

        for station_str, station_hash in data_full.stationhash_lookup.items():
            proto_full.stationhash_lookup[station_str] = station_hash

        for station_hash, transfers_for_station in data_full.transfers.items():
            for other_station_hash, transfer_time in transfers_for_station.items():
                proto_full.transfers[station_hash].transfer_times[other_station_hash] = transfer_time

        for trip_hash, trip in data_full.trips.items():
            proto_full.trips[trip_hash].branch.route_hash = trip.branch.route
            proto_full.trips[trip_hash].branch.final_station = trip.branch.final_station
            proto_full.trips[trip_hash].status = trip.status
            proto_full.trips[trip_hash].timestamp = trip.timestamp if trip.timestamp else 0
            for station_hash, arrival_time in trip.arrivals.items():
                proto_full.trips[trip_hash].arrivals[station_hash] = arrival_time

        data_out = proto_full.SerializeToString()
        with open(u.REALTIME_PARSED_PATH + 'data_full.protobuf', 'wb') as outfile:
            outfile.write(data_out)

        # with gzip.open(u.REALTIME_PARSED_PATH + 'data_full.protobuf' + '.gz', 'wb') as f:
        #        f.write(gzip.compress(data_out, compresslevel=9))

        with bz2.open(u.REALTIME_PARSED_PATH + 'data_full.protobuf' + '.bz2', 'wb') as f:
                f.write(bz2.compress(data_out, compresslevel=9))
        u.parser_logger.info('Serialized full_data to protobuf')


    def diff_to_protobuf(self):
        """ doc
        """
        if not self.data_diff:
            return

        data_update = self.data_diff
        proto_update = transit_data_access_pb2.DataUpdate()

        proto_update.realtime_timestamp = data_update.realtime_timestamp

        proto_update.trips.deleted[:] = data_update.trips.deleted
        for trip in data_update.trips.added:
            proto_trip = proto_update.trips.added.add()
            proto_trip.trip_hash = trip.id_
            proto_trip.info.status = trip.status
            proto_trip.info.timestamp = trip.timestamp if trip.timestamp else 0
            proto_trip.info.branch.route_hash = trip.branch.route
            proto_trip.info.branch.final_station = trip.branch.final_station
            for station_hash, arrival_time in trip.arrivals.items():
                proto_trip.info.arrivals[station_hash] = arrival_time

        for trip_hash, stations_list in data_update.arrivals.deleted.items():
            proto_update.arrivals.deleted.trip_station_dict[trip_hash].station_hash[:] = stations_list

        for trip_hash, station_arrival_dict in data_update.arrivals.added.items():
            for station_hash, arrival_time in station_arrival_dict.items():
                station_arrival = proto_update.arrivals.added[trip_hash].arrival.add()
                station_arrival.station_hash = station_hash
                station_arrival.arrival_time = arrival_time

        for time_diff, trip_stationlist_dict in data_update.arrivals.modified.items():
            for trip_hash, stations_list in trip_stationlist_dict.items():
                proto_update.arrivals.modified[time_diff].trip_station_dict[trip_hash].station_hash[:] = stations_list

        for trip_hash, trip_status in data_update.status.modified.items():
            proto_update.status[trip_hash] = trip_status

        for trip_hash, branch in data_update.branch.modified.items():
            proto_update.branch[trip_hash].route_hash = branch.route
            proto_update.branch[trip_hash].final_station = branch.final_station

        data_out = proto_update.SerializeToString()
        with open(u.REALTIME_PARSED_PATH + 'data_update.protobuf', 'wb') as outfile:
            outfile.write(data_out)

        # with gzip.open(u.REALTIME_PARSED_PATH + 'data_update.protobuf' + '.gz', 'wb') as f:
        #        f.write(gzip.compress(data_out, compresslevel=9))

        with bz2.open(u.REALTIME_PARSED_PATH + 'data_update.protobuf' + '.bz2', 'wb') as f:
                f.write(bz2.compress(data_out, compresslevel=9))

        print(proto_update)
        u.parser_logger.info('Serialized update_data to protobuf')

    def update(self) -> None:
        with u.TimeLogger() as _tl:
            try:
                self.prev_data = self.data
                self.fetch_all()
                self.merge_feeds()
                self.load_static()
                self.parse()
                self.diff()
                self.full_to_protobuf()
                self.diff_to_protobuf()
                if self.db_server:
                    self.db_server.push()
                _tl.tlog('realtime update')
            except u.UpdateFailed as err:
                self.data = self.prev_data
                u.parser_logger.error(err)
                if not self.data:
                    eventlet.sleep(1)
                    self.update()

    def run(self) -> None:
        u.parser_logger.info('~~~~~~~~~~ Running realtime.py ~~~~~~~~~~')
        while True:
            _t = time.time()
            self.update()
            _t_diff = time.time() - _t
            eventlet.sleep(u.REALTIME_FREQ - _t_diff)

if __name__ == "__main__":
    rm = RealtimeManager()
    rm.run()
