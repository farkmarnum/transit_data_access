"""Classes and methods for static GTFS data
"""
from collections import defaultdict
from contextlib import suppress
from typing import NamedTuple
import base64
import bisect
import gzip
#import heapq
import json
#import logging
import os
import pprint as pp
#import random
#import sys
import time
import warnings


from google.protobuf import message as protobuf_message
from google.transit import gtfs_realtime_pb2
#from graphviz import Digraph
import eventlet
from eventlet.green.urllib import error as urllib_error
from eventlet.green.urllib import request
import pyhash

from transit_system_config import MTA_SETTINGS
import misc

parser_logger = misc.parser_logger

eventlet.monkey_patch()

hasher = pyhash.super_fast_hash()
def small_hash(input_):
    hash_int = hasher(str(input_))
    hash_bytes = hash_int.to_bytes((hash_int.bit_length() + 7) // 8, 'big')
    hash_base85 = base64.b85encode(hash_bytes)
    hash_str = hash_base85.decode('utf-8')
    return hash_str


class TransitVertex():
    def add_neighbor(self, edge_type, neighbor_vertex, ride_time, times=None):
        if edge_type == misc.RIDE:
            if self.neighbors[misc.RIDE]:
                print('ERROR: only one RIDE neighbor possible: ', neighbor_vertex.id_)
                return
            self.neighbors[misc.RIDE] = (neighbor_vertex.id_, ride_time)
        elif edge_type == misc.TRANSFER:
            transfer_time, wait_time = times
            self.neighbors[misc.TRANSFER][neighbor_vertex.id_] = (
                transfer_time,
                wait_time,
                ride_time
            )
        elif edge_type == misc.WALK:
            exit_time, walk_time, enter_time, wait_time = times
            self.neighbors[misc.WALK][neighbor_vertex.id_] = (
                exit_time,
                walk_time,
                enter_time,
                wait_time,
                ride_time
            )
        self.degree, neighbor_vertex.degree = self.degree + (0, 1), neighbor_vertex.degree + (1, 0)


    def pack(self):
        """ TODO: make pack() serialize the data in flatbuffers or similar?
        """
        return [
            self.type_,
            self.time_,
            self.station_id,
            self.route_id,
            self.direction,
            self.neighbors,
            self.prev_station,
            self.next_station
        ]

    def __init__(self, trip_id, vertex_time, vertex_type, info):
        self.type_ = vertex_type
        self.time_ = vertex_time
        self.trip_id = trip_id

        # Generate a unique ID:
        if self.type_ == misc.STATION:
            self.station_id, self.route_id, self.direction = info
            self.id_ = small_hash((trip_id, self.station_id)) # forms a unique ID
        elif self.type_ == misc.LOC:
            self.loc = info
            self.id_ = small_hash((trip_id, self.loc)) # forms a unique ID

        self.neighbors = {
            misc.RIDE: tuple(),
            misc.TRANSFER: defaultdict(),
            misc.WALK: defaultdict()
        }
        self.degree = (0, 0)
        self.prev_station = None
        self.next_station = None

class RealtimeFeedResult(NamedTuple):
    feed_is_new: bool
    timestamp_diff: int = None
    error: str = None

class RealtimeHandler():
    """Gets a new realtime GTFS feed
    """
    def parse_stop_id(self, stop_id):
        try:
            station_id = self.station_id_lookup[stop_id]
        except KeyError:
            parser_logger.debug('stop_id %s not found in self.station_id_lookup', stop_id)
            return None, None

        try:
            direction = misc.DIRECTIONS[stop_id[-1]]
        except KeyError:
            parser_logger.debug('direction %s not found in DIRECTIONS', direction)
            return None, None

        return direction, station_id

    def add_arrival(self, station_id, vertex_id, route_id, direction, arrival_time):
        arrivals = self.parsed_data['stations'][str(station_id)]['arrivals']
        arrivals_for_route_and_direction = arrivals[route_id][direction]
        if not arrivals_for_route_and_direction:
            arrivals_for_route_and_direction = arrivals[route_id][direction] = []
        bisect.insort(arrivals_for_route_and_direction, (arrival_time, vertex_id))

    def get_static(self) -> bool:
        """Loads the static.json file into self.static_data
        Returns True if static.json is new
        """
        static_json = self.gtfs_settings.static_json_path+'/static.json'
        with open(static_json, mode='r') as static_json_file:
            self.static_data = json.loads(static_json_file.read())

        self.static_data['trains'] = misc.NestedDict()
        self.station_id_lookup = self.static_data['station_id_lookup']
        self.route_id_lookup = self.static_data['route_id_lookup']

        # check if the static_timestamp is different than the stored one
        static_timestamp = self.static_data['static_timestamp']
        static_timestamp_file = self.gtfs_settings.realtime_data_path\
                                + '/previous_static_feed_timestamp.txt'
        try:
            with open(static_timestamp_file, 'r+') as static_timestamp_infile:
                prev_static_timestamp = float(static_timestamp_infile.read())
                if prev_static_timestamp:
                    if static_timestamp == prev_static_timestamp:
                        return False
        except FileNotFoundError:
            parser_logger.info('%s does not exist, will create it', static_timestamp_file)
            try:
                os.makedirs(self.gtfs_settings.realtime_data_path, exist_ok=True)
            except PermissionError:
                parser_logger.error('Don\'t have permission to write to %s',
                                    self.gtfs_settings.realtime_data_path)
                raise PermissionError

        # store the new timestamp
        with open(static_timestamp_file, 'w') as static_timestamp_outfile:
            static_timestamp_outfile.write(str(static_timestamp))
        return True

    def eventlet_fetch(self, url, attempt=1):
        max_attempts = 2
        with eventlet.Timeout(misc.TIMEOUT):
            try:
                with request.urlopen(url) as response:
                    return response.read()
            except (OSError, urllib_error.URLError, eventlet.Timeout) as err:
                if attempt < max_attempts:
                    parser_logger.info('%s: unable to connect to %s, RETRYING', err, url)
                    self.eventlet_fetch(url, attempt + 1)
                else:
                    parser_logger.error('%s: unable to connect to %s, FAILED after %s attempts',
                                        err, url, attempt)
                    return False

    def get_realtime_feed(self) -> RealtimeFeedResult:
        """Gets a new realtime GTFS feed and checks if its timestamp is more recent
        than previous feeds' timestamps. This is done by storing the most recent timestamp seen in
        self.gtfs_settings.realtime_data_path+'/latest_feed_timestamp.txt'
        """
        request_pool = eventlet.GreenPool(20)
        response_list = request_pool.imap(self.eventlet_fetch, self.gtfs_settings.realtime_urls)

        try:
            all_bytes = b''.join(response_list)
            parser_logger.debug('realtime.py: received all feeds')
        except TypeError:
            parser_logger.info('Unable to receive all feeds, exiting get_realtime_feed')
            return RealtimeFeedResult(False, error='Unable to receive all feeds')

        with warnings.catch_warnings():
            warnings.filterwarnings(action='error', category=RuntimeWarning)
            try:
                feed_message = gtfs_realtime_pb2.FeedMessage()
                feed_message.ParseFromString(all_bytes)
                parser_logger.debug('realtime.py: joined feeds to a single FeedMessage object')
            except (RuntimeWarning, SystemError) as err:
                parser_logger.warning('%s when attempting: feed_message.ParseFromString', err)
                return RealtimeFeedResult(False, error=err)
            except protobuf_message.DecodeError:
                parser_logger.error('DecodeError when attempting: feed_message.ParseFromString')
                return RealtimeFeedResult(False, error='DecodeError')

        new_feed_timestamp = feed_message.header.timestamp
        parser_logger.debug('new_feed_timestamp: %s', new_feed_timestamp)

        latest_timestamp_file = self.gtfs_settings.realtime_data_path\
                                + '/latest_realtime_feed_timestamp.txt'
        try:
            with open(latest_timestamp_file, 'r+') as latest_timestamp_infile:
                latest_feed_timestamp = float(latest_timestamp_infile.read())
                if latest_feed_timestamp:
                    if new_feed_timestamp <= latest_feed_timestamp:
                        parser_logger.debug('We already have the most recent realtime GTFS feed')
                        parser_logger.debug('Current realtime feed\'s timestamp is %s secs old',
                                            time.time()-latest_feed_timestamp)
                        if new_feed_timestamp < latest_feed_timestamp:
                            parser_logger.debug('We\'ve received an older feed...')
                            parser_logger.debug('This timestamp is %s secs old',
                                                time.time()-new_feed_timestamp)
                        return RealtimeFeedResult(False)

            parser_logger.info('Loading new realtime GTFS. Recent by %s seconds',
                               new_feed_timestamp-latest_feed_timestamp)

        except FileNotFoundError:
            parser_logger.info('%s/latest_feed_timestamp.txt does not exist, will create it',
                               self.gtfs_settings.realtime_data_path)
            try:
                os.makedirs(self.gtfs_settings.realtime_data_path, exist_ok=True)
            except PermissionError:
                parser_logger.error('Don\'t have permission to write to %s',
                                    self.gtfs_settings.realtime_data_path)
                raise PermissionError

            parser_logger.info('Now, loading new realtime GTFS.')

        parser_logger.debug('This timestamp is %s secs old', time.time()-new_feed_timestamp)
        self.realtime_data = feed_message

        with open(latest_timestamp_file, 'w') as latest_response:
            latest_response.write(str(new_feed_timestamp))

        with open(f'{self.gtfs_settings.realtime_data_path}/realtime', 'wb') as realtime_raw_file:
            realtime_raw_file.write(all_bytes)

        timestamp_diff = new_feed_timestamp - latest_feed_timestamp
        return RealtimeFeedResult(
            feed_is_new=True,
            timestamp_diff=timestamp_diff
        )

    def parse_feed(self):
        """ Walks through self.realtime_data and creates self.parsed_data
        Uses self.static_data as a starting point

        Also creates self.realtime_graph, a directed graph of all trip stops
        """
        self.vertices = {}
        self.parsed_data = self.static_data
        self.parsed_data['realtime_timestamp'] = self.realtime_data.header.timestamp
        self.parsed_data['trains'] = defaultdict(dict)
        for station_id in self.parsed_data['stations']:
            self.parsed_data['stations'][str(station_id)]['arrivals'] = misc.NestedDict()

        for entity in self.realtime_data.entity:
            eventlet.greenthread.sleep(0) # yield time to other server processes if necessary

            if entity.HasField('trip_update'):
                trip_id = entity.trip_update.trip.trip_id
                route_id = entity.trip_update.trip.route_id
                route_id = self.route_id_lookup[route_id]

                prev_vertex_time, prev_vertex, prev_station_id = [None] * 3
                for stop_time_update in entity.trip_update.stop_time_update:
                    direction, station_id = self.parse_stop_id(stop_time_update.stop_id)
                    if not station_id:
                        continue

                    if stop_time_update.arrival.time > time.time():
                        vertex_time = stop_time_update.arrival.time

                        vertex = TransitVertex(
                            trip_id=trip_id,
                            vertex_time=vertex_time,
                            vertex_type=misc.STATION,
                            info=(station_id, route_id, direction)
                        )
                        self.vertices[vertex.id_] = vertex

                        self.add_arrival(station_id, vertex.id_, route_id, direction, vertex_time)

                        if prev_vertex:
                            prev_vertex.next_station = station_id
                            prev_vertex.add_neighbor(
                                edge_type=misc.RIDE,
                                neighbor_vertex=vertex,
                                ride_time=vertex_time - prev_vertex_time
                            )

                            vertex.prev_station = prev_station_id

                        prev_station_id, prev_vertex, prev_vertex_time = station_id, vertex, vertex_time
            """
            elif entity.HasField('vehicle'):
                trip_id = entity.vehicle.trip.trip_id
                route_id = entity.vehicle.trip.route_id
                route_id = self.route_id_lookup[route_id]
                direction, station_id = self.parse_stop_id(entity.vehicle.stop_id)
                # TODO: figure out previous station!!
                # TODO add last stop??
                self.parsed_data['trains'][(route_id, direction)][trip_id] = {
                    'current_station': station_id,
                    'previous_station': None, # TODO TODO TODO
                    'current_status': entity.vehicle.current_status,
                    'last_detected_movement': entity.vehicle.timestamp
                }
            """

    def add_edges_to_vertex_at_station(self, vertex, edge_type, station_id, times):
        try:
            min_transfer_time = sum(times)
        except TypeError:
            min_transfer_time = times
        next_allowable_time = vertex.time_ + min_transfer_time

        vertices_for_station = self.parsed_data['stations'][str(station_id)]['arrivals']
        for route_id, directions in vertices_for_station.items():
            for direction, vertices_for_direction in directions.items():
                if (route_id, direction, station_id) == (vertex.route_id, vertex.direction, vertex.station_id):
                    # don't add edges to vertices from the same route & direction @ the same station
                    continue

                split_index = bisect.bisect_right(vertices_for_direction, (next_allowable_time,))
                try:
                    nextvertex_time, nextvertex_id = vertices_for_direction[split_index]
                    wait_time = nextvertex_time - next_allowable_time
                except IndexError:
                    # there are no vertices for this branch after this vertex's time
                    continue

                transfer_vertex_tup = self.vertices[nextvertex_id].neighbors[misc.RIDE]
                if not transfer_vertex_tup:
                    continue
                transfer_vertex_id, ride_time = transfer_vertex_tup
                transfer_vertex = self.vertices[transfer_vertex_id]

                if transfer_vertex.station_id not in [vertex.prev_station, vertex.next_station]:
                    # (if the transfer_vertex is not headed to the current vertex's station
                    # or to the neighbor's next station)
                    continue

                if edge_type == misc.TRANSFER:
                    neighbor_times_arg = (min_transfer_time, wait_time)
                    vertex.add_neighbor(misc.TRANSFER, transfer_vertex, ride_time, neighbor_times_arg)
                elif edge_type == misc.WALK:
                    neighbor_times_arg = (*times, wait_time)
                    vertex.add_neighbor(misc.WALK, transfer_vertex, ride_time, neighbor_times_arg)

    def add_transfer_edges(self, vertex):
        """ This is called during the creation of the realtime graph.
        It adds transfer edges to each vertex where possible,
        as defined in self.static_data['transfers']
        """
        transfer_stations = {}
        with suppress(KeyError):
            transfer_stations = self.static_data['transfers'][str(vertex.station_id)]

        if vertex.station_id and not transfer_stations.get(str(vertex.station_id)): # TODO: this seems problematic...
            transfer_stations[str(vertex.station_id)] = 0

        for transfer_station_id, min_transfer_time in transfer_stations.items():
            self.add_edges_to_vertex_at_station(vertex, misc.TRANSFER, transfer_station_id, min_transfer_time)

    def add_walk_edges(self, vertex, walkable_stations):
        """ This is called during the creation of the realtime graph.
        It adds walk edges to each vertex where possible,
        as defined in self.static_data['walkable']
        """
        for walkable_station_id, times in walkable_stations:
            self.add_edges_to_vertex_at_station(vertex, misc.WALK, walkable_station_id, times)

    def add_all_transfer_and_walk_edges(self):
        for _, vertex in self.vertices.items():
            self.add_transfer_edges(vertex)
            """ # TODO: walkable...
            with suppress(KeyError):
                walkable_stations = self.static_data['stations'][str(vertex.station_id)]['walkable']
                self.add_walk_edges(vertex, walkable_stations)
            """

    def generate_realtime_update(self):
        # TODO
        pass




    def graph_info(self):
        vertices = edges = 0
        for _, vertex in self.vertices.items():
            vertices += 1
            for neighbor_category, neighbors in vertex.neighbors.items():
                if neighbor_category != misc.RIDE or neighbors[0]:
                    edges += len(neighbors)
        print('Generated a graph with', vertices, 'vertices, and', edges, 'edges.\n')

    def pack_graph(self):
        self.packed_vertices = {
            vertex.id_: vertex.pack() for _, vertex in self.vertices.items()
        }
        self.parsed_data['vertices'] = self.packed_vertices

    def protobuf_pack_full(self):
        pass

    def protobuf_pack_update(self):
        pass

    def to_json(self, attempt=0):
        """dumps self.parsed_data to realtime.json
        """
        #self.parsed_data.pop('shape_to_branch')
        #self.parsed_data.pop('transfers')

        json_path = self.gtfs_settings.realtime_json_path
        json_str = json.dumps(self.parsed_data)

        try:
            with open(json_path+'/realtime.json', 'w') as json_file:
                json_file.write(json_str)
                parser_logger.debug('Wrote realtime parsed data to %s/realtime.json', json_path)

            with gzip.open(json_path+'/realtime.json.gz', 'wb') as gzip_file:
                gzip_file.write(json_str.encode('utf-8'))

        except OSError:
            if attempt != 0:
                parser_logger.error('Unable to write to %s/realtime.json\n', json_path)
                raise OSError

            parser_logger.info('%s/realtime.json does not exist, attempting to create it', json_path)
            try:
                os.makedirs(json_path, exist_ok=True)

            except PermissionError:
                parser_logger.error('Do not have permission to write to %s/realtime.json\n', json_path)
                raise PermissionError

            self.to_json(attempt=attempt+1)

    def get_prev_feed(self):
        with open('/data/transit_data_access/db_server/MTA/realtime/raw/realtime', 'rb') as example_realtime_feed:
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(example_realtime_feed.read())

        self.realtime_data = feed_message

        return True

    def __init__(self, gtfs_settings):
        self.gtfs_settings = gtfs_settings
        self.name = gtfs_settings.ts_name
        self.realtime_data = None
        self.static_data = {}
        self.parsed_data = {}
        self.station_id_lookup = None
        self.route_id_lookup = None
        self.packed_vertices = None
        self.vertices = None

def main() -> int:
    """ TODO main() docstring

    explanation of return values:
        PARSE_FAIL = exception raised while parsing, no data output
        PARSE_SUCCESS = data_full & data_update generated
        REALTIME_NOT_NEW = no new realtime feed, so no data output
        PREV_REALTIME_TOO_OLD = previous realtime feed was too long ago for a meaningful update, so only data_full generated
        NEW_STATIC = static feed has changed, so to be safe, no data_update generated, just data_full
    """
    parser_logger.info('***********realtime.main() BEGINNING*************')
    with misc.TimeLogger() as _tl:
        realtime_handler = RealtimeHandler(MTA_SETTINGS)
        _tl.tlog('RealtimeHandler()')
        realtime_result = realtime_handler.get_realtime_feed()
        _tl.tlog('get_realtime_feed()')
        if not realtime_result.feed_is_new:
            if realtime_result.error:
                return misc.PARSE_FAIL
            return misc.REALTIME_NOT_NEW

        static_is_new = realtime_handler.get_static()
        _tl.tlog('get_static()')
        realtime_handler.parse_feed()
        _tl.tlog('parse_feed()')
        realtime_handler.add_all_transfer_and_walk_edges()
        _tl.tlog('add_all_transfer_edges()')
        realtime_handler.protobuf_pack_full()
        _tl.tlog('protobuf_pack_full()')
        if realtime_result.timestamp_diff > 120:
            return misc.PREV_REALTIME_TOO_OLD
        if static_is_new:
            return misc.NEW_STATIC

        realtime_handler.get_prev_feed()
        _tl.tlog('get_prev_feed()')
        realtime_handler.generate_realtime_update()
        _tl.tlog('generate_realtime_update()')
        realtime_handler.protobuf_pack_update()
        _tl.tlog('protobuf_pack_update()')
        realtime_handler.pack_graph()
        realtime_handler.to_json()
        _tl.tlog('pack_graph() & to_json()')

        return misc.PARSE_SUCCESS


if __name__ == '__main__':
    main()
