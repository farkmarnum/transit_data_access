""" Downloads realtime GTFS data, checks if it's new, parses it, and stores it
"""
import sys
import time
from typing import List, Dict, Tuple, NamedTuple, NewType, Union, Any, Optional  # noqa
import warnings
import json
import eventlet
import bz2
from eventlet.green.urllib.error import URLError
from eventlet.green.urllib import request
# from google.transit import gtfs_realtime_pb2
from google.transit.gtfs_realtime_pb2 import FeedMessage    # type: ignore
import transit_data_access_pb2    # type: ignore
from google.protobuf.message import DecodeError
import util as u    # type: ignore
from server import DatabaseServer    # type: ignore

TIME_DIFF_THRESHOLD = 3


FetchStatus = NewType('FetchStatus', int)
NONE, NEW_FEED, OLD_FEED, FETCH_FAILED, DECODE_FAILED, RUNTIME_WARNING = list(map(FetchStatus, range(6)))

Timestamp = NewType('Timestamp', int)

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
                    _raw = response.read()
                    feed_message = FeedMessage()
                    feed_message.ParseFromString(_raw)
                    timestamp = feed_message.header.timestamp
                    if timestamp >= self.latest_timestamp + TIME_DIFF_THRESHOLD:
                        self.result = FetchResult(NEW_FEED, timestamp=timestamp)
                        self.prev_feed, self.latest_feed, self.latest_timestamp = \
                            self.latest_feed, feed_message, timestamp
                        u.redis_server.hset('realtime_feeds', self.id_, _raw)
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

        if attempt + 1 < u.REALTIME_MAX_ATTEMPTS:
            u.log.debug('parser: Fetch failed for %s, trying again', self.id_)
            self.fetch(attempt=attempt + 1)

    def restore_feed_from_redis(self):
        _raw = u.redis_server.hget('realtime_feeds', self.id_)
        if not _raw:
            return

        _feed = FeedMessage()
        try:
            _feed.ParseFromString(_raw)
            self.latest_feed = _feed
        except (DecodeError, SystemError, RuntimeWarning) as err:
            u.log.error('%s: unable to parse feed %s restored from redis', err, self.id_)

    def __init__(self, url, id_):
        self.url: str = url
        self.id_: str = id_
        self.result: FetchResult = FetchResult(NONE)
        self.latest_timestamp: int = 0
        self.latest_feed: FeedMessage = None
        self.prev_feed: FeedMessage = None

        self.restore_feed_from_redis()


class RealtimeManager():
    """docstring for RealtimeManager
    """
    def __init__(self, db_server: DatabaseServer = None) -> None:
        self.db_server = db_server
        self.parser_thread = None
        self.initial_merge_attempts = 0
        self.max_initial_merge_attempts = 10
        self.request_pool = eventlet.GreenPool(len(u.GTFS_CONF.realtime_urls))
        self.feed_handlers = [RealtimeFeedHandler(url, id_) for id_, url in u.GTFS_CONF.realtime_urls.items()]

        self.feed: FeedMessage = None
        self.current_timestamp: Timestamp = Timestamp(0)
        self.current_data: u.RealtimeData = None  # type: ignore
        self.current_data_json: str = ''
        self.current_data_bz2: bytes = b''

        self.diff_dict: Dict[Timestamp, u.DataDiff] = {}
        self.diff_dict_bz2: Dict[Timestamp, bytes] = {}

        self.data_dict: Dict[Timestamp, u.RealtimeData] = {}

        _t = time.time()
        self.load_data_dict_from_redis()
        print(f'load_data_dict_from_redis took {time.time() - _t:.4f} seconds')


    def load_data_dict_from_redis(self):
        if not u.redis_server.exists('realtime_data_dict'):
            u.log.debug('No realtime_data_dict key in Redis')
            return

        _redis_data_dict_timestamps = u.redis_server.hkeys('realtime_data_dict')
        _oldest_timestamp_desired = time.time() - u.REALTIME_DATA_DICT_CAP * u.REALTIME_FREQ
        _outdated_timestamps = [t for t in _redis_data_dict_timestamps if float(t) < _oldest_timestamp_desired]
        u.log.debug('removing these timestamps from redis since they\'re too old: %s', _outdated_timestamps)
        if _outdated_timestamps:
            u.redis_server.hdel('realtime_data_dict', *_outdated_timestamps)

        data_json_dict = u.redis_server.hgetall('realtime_data_dict')
        u.log.debug('realtime_data_dict loaded from Redis, len is %s', len(data_json_dict))

        for timestamp, json_str in data_json_dict.items():
            self.data_dict[int(timestamp.decode('utf-8'))] = json.loads(json_str, cls=u.RealtimeJSONDecoder)


    def fetch_all(self) -> None:
        """get all new feeds, check each, and combine
        """
        u.log.debug('parser: Checking feeds!')
        for fh in self.feed_handlers:
            self.request_pool.spawn(fh.fetch)
        self.request_pool.waitall()

        for fh in self.feed_handlers:
            if fh.result.status not in [NEW_FEED, OLD_FEED]:
                u.log.error('parser: Encountered %s when fetching feed %s', fh.result.error, fh.id_)

        # self.average_realtime_timestamp = int(sum([fh.latest_timestamp for fh in self.feed_handlers]) / len(self.feed_handlers))
        new_feeds = sum([int(fh.result.status == NEW_FEED) for fh in self.feed_handlers])
        u.log.info('parser: %s new feeds', new_feeds)
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
        """Loads the static.json file into self.current_data
        """
        static_json = f'{u.STATIC_PATH}/parsed/static.json'
        with open(static_json, mode='r') as static_json_file:
            static_data = json.loads(static_json_file.read(), cls=u.StaticJSONDecoder)
            self.current_timestamp = Timestamp(int(time.time()))
            self.current_data = u.RealtimeData(
                name=static_data.name,
                static_timestamp=static_data.static_timestamp,
                routes=static_data.routes,
                stations=static_data.stations,
                routehash_lookup={str(k): v for k, v in static_data.routehash_lookup.items()},
                stationhash_lookup={str(k): v for k, v in static_data.stationhash_lookup.items()},
                transfers={int(k): {int(_k): _v for _k, _v in v.items()} for k, v in static_data.transfers.items()},
                realtime_timestamp=self.current_timestamp,
                trips={})

    def parse(self) -> None:
        for elem in self.feed.entity:
            trip_hash = u.short_hash(elem.trip_update.trip.trip_id, u.TripHash)
            route_hash = u.short_hash(elem.trip_update.trip.route_id, u.RouteHash)

            if trip_hash not in self.current_data.trips:
                if not len(elem.trip_update.stop_time_update):
                    continue
                last_stop_id = elem.trip_update.stop_time_update[-1].stop_id
                final_station = self.current_data.stationhash_lookup[last_stop_id]
                if not final_station:
                    continue
                branch = u.Branch(route_hash, final_station)
                self.current_data.trips[trip_hash] = u.Trip(id_=trip_hash, branch=branch)

            if elem.HasField('trip_update'):
                for stop_time_update in elem.trip_update.stop_time_update:
                    try:
                        station_hash = self.current_data.stationhash_lookup[stop_time_update.stop_id]
                    except KeyError:
                        u.log.debug('parser: KeyError for %s', stop_time_update.stop_id)
                        continue

                    arrival_time = u.ArrivalTime(stop_time_update.arrival.time)
                    if arrival_time < time.time():
                        continue
                    self.current_data.trips[trip_hash].add_arrival(station_hash, arrival_time)

            elif elem.HasField('vehicle'):
                timestamp = elem.vehicle.timestamp
                self.current_data.trips[trip_hash].timestamp = timestamp

                if time.time() - timestamp > 90:
                    trip_hash = u.short_hash(elem.vehicle.trip.trip_id, u.TripHash)
                    self.current_data.trips[trip_hash].status = u.STOPPED

        self.serialize_to_JSON()


    def load_data_and_diffs(self) -> None:
        self.data_dict[self.current_timestamp] = self.current_data
        if len(self.data_dict) > u.REALTIME_DATA_DICT_CAP:
            del self.data_dict[min(self.data_dict)]
        assert len(self.data_dict) <= u.REALTIME_DATA_DICT_CAP

        # TODO !!! optimize this with piping
        u.redis_server.hset('realtime_data_dict', self.current_timestamp, self.current_data_json)
        if u.redis_server.hlen('realtime_data_dict') > u.REALTIME_DATA_DICT_CAP:
            u.redis_server.hdel('realtime_data_dict', min(u.redis_server.hkeys('realtime_data_dict')))
        assert u.redis_server.hlen('realtime_data_dict') <= u.REALTIME_DATA_DICT_CAP

        new_diff_dict = {}
        for timestamp in (set(self.data_dict) - {self.current_timestamp}):
            new_diff_dict[timestamp] = self.diff(old_data=self.data_dict[timestamp], new_data=self.current_data)

        self.diff_dict = new_diff_dict


    def serialize_to_JSON(self):
        """ Stores data in outfile with custom JSON encoder u.RealtimeJSONEncoder
        """
        self.current_data_json = json.dumps(self.current_data, cls=u.RealtimeJSONEncoder)

        with open(u.REALTIME_PATH + '/parsed/realtime.json', 'w') as _out_stream:
            _out_stream.write(self.current_data_json)


    def diff(self, old_data: u.RealtimeData, new_data: u.RealtimeData) -> u.DataDiff:
        """ docstring for diff()
        """
        new_trips = new_data.trips
        old_trips = old_data.trips

        trip_diff = u.TripDiff(
            deleted=list(set(old_trips) - set(new_trips)),
            added=[new_trips[trip_hash] for trip_hash in (set(new_trips) - set(old_trips))]
        )
        arrivals_diff = u.ArrivalsDiff()
        status_diff   = u.StatusDiff()  # noqa
        branch_diff   = u.BranchDiff()  # noqa

        for trip_hash in (set(new_trips) & set(old_trips)):
            new_trip = new_trips[trip_hash]
            old_trip = old_trips[trip_hash]

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
            realtime_timestamp=self.current_data.realtime_timestamp,
            trips=trip_diff,
            arrivals=arrivals_diff,
            status=status_diff,
            branch=branch_diff)

        return data_diff


    def full_to_protobuf_bz2(self) -> None:
        """ doc
        """
        data_full = self.current_data
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

        self.current_data_bz2 = bz2.compress(proto_full.SerializeToString(), compresslevel=9)

        u.log.debug('full: %fKB', sys.getsizeof(self.current_data_bz2) / 1024)



    def diff_to_protobuf_bz2(self, data_diff: u.DataDiff) -> bytes:
        """ doc
        """
        proto_update = transit_data_access_pb2.DataUpdate()

        proto_update.realtime_timestamp = data_diff.realtime_timestamp

        proto_update.trips.deleted[:] = data_diff.trips.deleted
        for trip in data_diff.trips.added:
            proto_trip = proto_update.trips.added.add()
            proto_trip.trip_hash = trip.id_
            proto_trip.info.status = trip.status
            proto_trip.info.timestamp = trip.timestamp if trip.timestamp else 0
            proto_trip.info.branch.route_hash = trip.branch.route
            proto_trip.info.branch.final_station = trip.branch.final_station
            for station_hash, arrival_time in trip.arrivals.items():
                proto_trip.info.arrivals[station_hash] = arrival_time

        for trip_hash, stations_list in data_diff.arrivals.deleted.items():
            proto_update.arrivals.deleted.trip_station_dict[trip_hash].station_hash[:] = stations_list

        for trip_hash, station_arrival_dict in data_diff.arrivals.added.items():
            for station_hash, arrival_time in station_arrival_dict.items():
                station_arrival = proto_update.arrivals.added[trip_hash].arrival.add()
                station_arrival.station_hash = station_hash
                station_arrival.arrival_time = arrival_time

        for time_diff, trip_stationlist_dict in data_diff.arrivals.modified.items():
            for trip_hash, stations_list in trip_stationlist_dict.items():
                proto_update.arrivals.modified[time_diff].trip_station_dict[trip_hash].station_hash[:] = stations_list

        for trip_hash, trip_status in data_diff.status.modified.items():
            proto_update.status[trip_hash] = trip_status

        for trip_hash, branch in data_diff.branch.modified.items():
            proto_update.branch[trip_hash].route_hash = branch.route
            proto_update.branch[trip_hash].final_station = branch.final_station

        compressed_protobuf = bz2.compress(proto_update.SerializeToString(), compresslevel=9)
        return compressed_protobuf


    def all_diff_to_protobuf_bz2(self):
        for timestamp in sorted(self.diff_dict.keys()):
            diff = self.diff_dict[timestamp]
            _bz2 = self.diff_to_protobuf_bz2(diff)
            u.log.debug('update %s: %fKB', timestamp, sys.getsizeof(_bz2) / 1024)
            self.diff_dict_bz2[timestamp] = _bz2



    def update(self) -> None:
        with u.TimeLogger() as _tl:
            try:
                tmp_data_placeholder = self.current_data
                self.fetch_all()
                _tl.tlog('fetch_all')
                self.merge_feeds()
                _tl.tlog('merge_feeds')
                self.load_static()
                _tl.tlog('load_static')
                self.parse()
                _tl.tlog('parse')
                self.load_data_and_diffs()
                _tl.tlog('load_data_and_diffs')
                self.full_to_protobuf_bz2()
                _tl.tlog('full_to_protobuf_bz2')
                self.all_diff_to_protobuf_bz2()
                _tl.tlog('all_diff_to_protobuf_bz2')

                if self.db_server:
                    self.db_server.push(
                        current_timestamp=self.current_timestamp,
                        data_full=self.current_data_bz2,
                        data_diffs=self.diff_dict_bz2)
                _tl.tlog('push')

            except u.UpdateFailed as err:
                self.current_data = tmp_data_placeholder
                u.log.error(err)
                if not self.current_data:
                    if self.initial_merge_attempts < self.max_initial_merge_attempts:
                        eventlet.sleep(5)
                        self.initial_merge_attempts += 1
                        self.update()
                    else:
                        u.log.error('parser: Couldn\'t get all feeds, exiting after %s attempts.\n%s', self.max_initial_merge_attempts, err)
                        exit()

    def run(self) -> None:
        while True:
            _t = time.time()
            self.update()
            _t_diff = time.time() - _t
            eventlet.sleep(u.REALTIME_FREQ - _t_diff)

    def start(self):
        u.log.info('parser: ~~~~~~~~~~ Running realtime parser ~~~~~~~~~~')
        self.parser_thread = eventlet.spawn(self.run)

    def stop(self):
        u.log.info('parser: ~~~~~~~~~~ Stopping realtime parser ~~~~~~~~~~')
        self.parser_thread.kill()

if __name__ == "__main__":
    rm = RealtimeManager()
    rm.start()
    while True:
        eventlet.sleep(2)
