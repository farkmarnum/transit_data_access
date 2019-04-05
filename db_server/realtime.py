"""Classes and methods for static GTFS data
"""
import logging
import time
import os
import base64
from collections import defaultdict, OrderedDict
import bisect
import json
import bson
import eventlet
from eventlet.green.urllib import request
from eventlet.green.urllib import error as urllib_error
import warnings
import pprint as pp
import pyhash

import gzip
from google.transit import gtfs_realtime_pb2
from google.protobuf import message as protobuf_message

from transit_system_config import MTA_SETTINGS
import misc

parser_logger = misc.parser_logger

eventlet.monkey_patch()

DEP, ARR = 0, 1

hasher = pyhash.super_fast_hash()

def small_hash(input_):
    hash_int = hasher(str(input_))
    hash_bytes = hash_int.to_bytes((hash_int.bit_length() + 7) // 8, 'big')
    hash_base85 = base64.b85encode(hash_bytes)
    hash_str = hash_base85.decode('utf-8')
    return hash_str


def add_sorted(list_, element):
    index = bisect.bisect_left(list_, element)
    list_.insert(index, element)


class Station():
    def add_departure(self, branch_id, departure_vertex_id, time_):
        bisect.insort( self.departures_by_branch[branch_id], (time_, departure_vertex_id) )

    def add_arrival(self, branch_id, arrival_vertex_id, time_):
        bisect.insort( self.arrivals_by_branch[branch_id], (time_, arrival_vertex_id) )

    def __init__(self, station_id, parent_station=None):
        self.id_ = station_id
        self.departures_by_branch = defaultdict(list)
        self.arrivals_by_branch = defaultdict(list)
        self.parent_station = parent_station

class TransitVertex():

    def add_info(self, station_id, route_id, branch_id):
        self.station_id, self.route_id, self.branch_id = station_id, route_id, branch_id

    def add_prev_station(self, prev_station):
        self.prev_station = prev_station

    def add_next_station(self, next_station):
        self.next_station = next_station

    def add_neighbor(self, tup):
        (neighbor_id, weight) = tup
        self.neighbors[neighbor_id] = weight

    def add_transfer(self, tup):
        (transfer_id, weight) = tup
        self.transfers[transfer_id] = weight

    def pack(self):
        """ TODO: make pack() serialize the data in something like BSON
        """
        compact_list = [
            self.station_id,
            self.type_,
            #self.route_id,
            self.branch_id,
            self.time_,
            self.neighbors,
            self.transfers
        ]
        return compact_list

    def __init__(self, type_, time_, trip_id):
        self.id_ = small_hash( (type_, time_, trip_id) )
        self.type_ = type_
        self.time_ = time_
        #self.trip_id = trip_id
        self.neighbors = {}
        self.transfers = {}
        self.prev_station = self.next_station = None


class RealtimeHandler:
    """Gets a new realtime GTFS feed
    """

    def get_static(self):
        """Loads the static.json file into self.static_data"""
        static_json = self.gtfs_settings.static_json_path+'/static.json'
        with open(static_json, mode='r') as static_json_file:
            self.static_data = json.loads(static_json_file.read())
        self.static_data['trains'] = misc.NestedDict()

    def eventlet_fetch(self, url):
        with eventlet.Timeout(misc.TIMEOUT):
            try:
                response = request.urlopen(url).read()
                return response
            except (OSError, urllib_error.URLError, eventlet.Timeout) as err:
                parser_logger.error('%s: unable to connect to %s', err, url)
                return False

    def check_feed(self):
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
            parser_logger.info('Unable to receive all feeds, exiting check_feed & returning False')
            return False

        feed_message = gtfs_realtime_pb2.FeedMessage()

        with warnings.catch_warnings():
            warnings.filterwarnings(action='error',category=RuntimeWarning)

            try:
                feed_message.ParseFromString(all_bytes)
            except (RuntimeWarning, SystemError) as err:
                parser_logger.warning('%s when attempting: feed_message.ParseFromString(response.content)', err)
                return False
            except protobuf_message.DecodeError:
                parser_logger.error('DecodeError when attempting: feed_message.ParseFromString(response.content)')
                return False

        parser_logger.debug('realtime.py: joined feeds and converted to a single FeedMessage object')

        new_feed_timestamp = feed_message.header.timestamp

        parser_logger.debug('new_feed_timestamp: %s', new_feed_timestamp)

        latest_timestamp_file = self.gtfs_settings.realtime_data_path+'/latest_feed_timestamp.txt'
        try:
            with open(latest_timestamp_file, 'r+') as latest_timestamp_infile:
                latest_feed_timestamp = float(latest_timestamp_infile.read())
                if latest_feed_timestamp:
                    if new_feed_timestamp <= latest_feed_timestamp:
                        parser_logger.debug('We already have the most up-to-date realtime GTFS feed')
                        parser_logger.debug('Current realtime feed\'s timestamp is %s secs old', time.time()-latest_feed_timestamp)
                        if new_feed_timestamp < latest_feed_timestamp:
                            parser_logger.debug('We\'ve received an older feed...')
                            parser_logger.debug('This timestamp is %s secs old', time.time()-new_feed_timestamp)
                        return False

            parser_logger.info('Loading new realtime GTFS. Recent by %s seconds', new_feed_timestamp-latest_feed_timestamp)

        except FileNotFoundError:
            parser_logger.info('%s/latest_feed_timestamp.txt does not exist, will create it', self.gtfs_settings.realtime_data_path)
            try:
                os.makedirs(self.gtfs_settings.realtime_data_path, exist_ok=True)
            except PermissionError:
                parser_logger.error('Don\'t have permission to write to %s', self.gtfs_settings.realtime_data_path)
                exit()

            parser_logger.info('Now, loading new realtime GTFS.')

        parser_logger.debug('This timestamp is %s secs old', time.time()-new_feed_timestamp)
        self.realtime_data = feed_message

        with open(latest_timestamp_file, 'w') as latest_response:
            latest_response.write(str(new_feed_timestamp))

        with open(f'{self.gtfs_settings.realtime_data_path}/realtime', 'w') as realtime_raw_file:
            realtime_raw_file.write(str(feed_message))

        return True

    def entity_info(self, entity_body):
        """ pulls route_id, trip_id, and shape_id from entity
        """
        trip_id = entity_body.trip.trip_id
        route_id = entity_body.trip.route_id

        if '6..N04' in trip_id or '6..S04' in trip_id: # i don't even know anymore...
            trip_id = trip_id.split('X')[0][:-2]


        shape_id = misc.trip_to_shape(trip_id, trip_to_shape_long_dict=self.trip_to_shape_long)

        try:
            branch_id = self.static_data['shape_to_branch'][shape_id]
        except KeyError:
            if shape_id:
                parser_logger.debug('entity_info(): couldn\'t find a branch_id for shape_id %s route_id %s trip_id %s', shape_id, route_id, trip_id)
            branch_id = None

        return [trip_id, branch_id, route_id]

    def parse_feed(self):
        """ Walks through self.realtime_data and creates self.parsed_data
        Uses self.static_data as a starting point

        Also creates self.realtime_graph, a directed graph of all trip stops
        """

        #self.realtime_graph = defaultdict(list)
        self.vertices = {}
        self.stations = {}

        self.parsed_data = self.static_data
        self.parsed_data['realtime_timestamp'] = self.realtime_data.header.timestamp

        for _, route_data in self.parsed_data['routes'].items():
            route_data.pop('shapes')

        for entity in self.realtime_data.entity:
            if entity.HasField('trip_update'):
                eventlet.greenthread.sleep(0) # yield time to other server processes if necessary
                trip_id, branch_id, route_id = self.entity_info(entity.trip_update)
                if not branch_id:
                    continue

                prev_departure_time = prev_departure = prev_station = None
                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id
                    try:
                        station_id = self.static_data['stops'][stop_id]['info']['parent_station']
                    except KeyError:
                        continue

                    if stop_time_update.arrival.time > time.time():
                        arrival_time = stop_time_update.arrival.time

                        this_arrival = TransitVertex(ARR, arrival_time, trip_id)
                        this_arrival.add_info(station_id, route_id, branch_id)

                        self.vertices[this_arrival.id_] = this_arrival

                        try:
                            station = self.stations[station_id]
                            station.add_arrival(branch_id, this_arrival.id_, arrival_time)
                        except KeyError:
                            station = Station(station_id)
                            station.add_arrival(branch_id, this_arrival.id_, arrival_time)
                            self.stations[station_id] = station

                        if prev_departure:
                            prev_departure.add_next_station( station_id )
                            prev_departure.add_neighbor( (this_arrival.id_, arrival_time - prev_departure_time) )

                            this_arrival.add_prev_station( prev_station )


                        departure_time = stop_time_update.departure.time
                        if departure_time:

                            this_departure = TransitVertex(DEP, departure_time, trip_id)
                            this_departure.add_info(station_id, route_id, branch_id)

                            self.vertices[this_departure.id_] = this_departure
                            station.add_departure(branch_id, this_departure.id_, departure_time)

                            this_arrival.add_neighbor( (this_departure.id_, departure_time - arrival_time) )

                            prev_departure_time = departure_time
                            prev_departure = this_departure
                            prev_station = station_id



            elif entity.HasField('vehicle'):
                trip_id, branch_id, route_id = self.entity_info(entity.vehicle)
                if not branch_id:
                    continue

                self.parsed_data['trains'][route_id][branch_id][trip_id]['next_stop'] = entity.vehicle.stop_id
                self.parsed_data['trains'][route_id][branch_id][trip_id]['current_status'] = entity.vehicle.current_status
                self.parsed_data['trains'][route_id][branch_id][trip_id]['last_detected_movement'] = entity.vehicle.timestamp


        #self.realtime_graph_sorted = OrderedDict({
        #    k: v for k, v in sorted(self.realtime_graph.items(), key=lambda kv: kv[1])
        #})

        """
        self.packed_vertices = {
            vertex.id_: vertex.pack() for _, vertex in self.vertices.items()
        }

        pp.pprint(self.packed_vertices)
        """


    def add_transfer_edges(self):
        vertices = self.vertices

        for _, vertex in vertices.items():
            if vertex.type_ == DEP:
                continue

            station = self.stations[vertex.station_id]
            # TODO method to get next allowable time based on transfer time between branches at stop and vertex.time_

            for _, list_of_departures_for_branch in station.departures_by_branch.items():
                split_index = bisect.bisect_right( list_of_departures_for_branch, (vertex.time_, ) )

                try:
                    next_departure_time, next_departure_id = list_of_departures_for_branch[split_index]
                    next_departure = self.vertices[next_departure_id]
                    if next_departure.next_station:
                        if next_departure.next_station != vertex.prev_station:
                            vertex.add_transfer( (next_departure_id, next_departure_time - vertex.time_) )
                except IndexError:
                    pass

        self.packed_vertices = {
            vertex.id_: vertex.pack() for _, vertex in self.vertices.items()
        }
        data = bson.BSON.encode(self.packed_vertices)

        with open('tmp.txt', 'w') as outfile:
            outfile.write(data)
        #pp.pprint(self.packed_vertices)

        exit()

    """
    def hash_graph(self):
        self.realtime_graph_hashed = defaultdict(dict)
        self.realtime_vertices_info_hashed = defaultdict(dict)
        hashes = {}
        i = 0

        for arrival, departures in self.realtime_graph.items():
            for departure, edge_time in departures.items():
                self.realtime_graph_hashed[tuple_hash(arrival)][tuple_hash(departure)] = edge_time

        for vertex, info in self.realtime_vertices_info.items():
            self.realtime_vertices_info_hashed[tuple_hash(vertex)] = info

        self.parsed_data['graph'] =
    """

    def to_json(self, attempt=0):
        """dumps self.parsed_data to realtime.json
        """
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
                exit()

            parser_logger.info('%s/realtime.json does not exist, attempting to create it', json_path)
            try:
                os.makedirs(json_path, exist_ok=True)

            except PermissionError:
                parser_logger.error('Do not have permission to write to %s/realtime.json\n', json_path)
                exit()

            self.to_json(attempt=attempt+1)

    def __init__(self, gtfs_settings, name=''):
        self.name = name
        self.gtfs_settings = gtfs_settings
        self.realtime_data = None
        self.static_data = self.parsed_data = {}

        # load the trip_to_shape_long dict
        with open(f'{self.gtfs_settings.static_data_path}/trip_id_to_shape.json', 'r') as json_file_stream:
            nested_dict_ = json.loads(json_file_stream.read())
            self.trip_to_shape_long = {
                outer_k: {
                    int(inner_k): inner_v
                        for inner_k, inner_v in outer_v.items()
                    }
                for outer_k, outer_v in nested_dict_.items()
            }


def main():
    """Creates new RealtimeHandler, then calls get_static() and check_feed()
    """
    with misc.TimeLogger('realtime.py') as _tl:
        realtime_handler = RealtimeHandler(MTA_SETTINGS, name='MTA')
        realtime_handler.get_static()

        feed_is_new = realtime_handler.check_feed()
        if feed_is_new:
            realtime_handler.parse_feed()
            realtime_handler.add_transfer_edges()
            #realtime_handler.hash_graph()
            realtime_handler.to_json()

        return feed_is_new

if __name__ == '__main__':
    main()
