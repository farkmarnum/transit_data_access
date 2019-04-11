"""Classes and methods for static GTFS data
"""
import logging
import time
import os
import sys
import base64
import heapq
from collections import defaultdict, OrderedDict
import bisect
import json
import eventlet
from eventlet.green.urllib import request
from eventlet.green.urllib import error as urllib_error
import warnings
import random

import pprint as pp
#import networkx as nx
#import pygraphviz as pgv
#import matplotlib.pyplot as plt
from graphviz import Digraph

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
    #def add_departure(self, branch_id, departure_vertex_id, time_):
    #    bisect.insort( self.departures_by_branch[branch_id], (time_, departure_vertex_id) )

    def add_vertex(self, branch_id, vertex_id, time_):
        bisect.insort( self.vertices_by_branch[branch_id], (time_, vertex_id) )
        self.vertices.append(vertex_id)

    def pack(self):
        return self.vertices_by_branch

    def __init__(self, station_id, parent_station=None):
        self.id_ = station_id
        self.vertices_by_branch = defaultdict(list)
        self.vertices = []
        self.parent_station = parent_station

class TransitVertex():

    def add_info(self, station_id, route_id, branch_id, stop_id):
        self.station_id, self.route_id, self.branch_id, self.stop_id = station_id, route_id, branch_id, stop_id

    def add_prev_station(self, prev_station):
        self.prev_station = prev_station

    def add_next_station(self, next_station):
        self.next_station = next_station

    def set_neighbor(self, neighbor_vertex):
        self.neighbor = (neighbor_vertex.id_, neighbor_vertex.time_ - self.time_)
        self.out_degree = self.out_degree + 1
        neighbor_vertex.inc_in_degree()

    def add_transfer(self, transfer_vertex, time_before_departure):
        self.transfers[transfer_vertex.id_] = ( time_before_departure, transfer_vertex.time_ - (self.time_ + time_before_departure) )
        self.out_degree = self.out_degree + 1
        transfer_vertex.inc_in_degree()

    def inc_in_degree(self):
        self.in_degree = self.in_degree + 1

    def condense_neighbor(self, vertices):
        neighbor_vertex_id, neighbor_vertex_weight = self.neighbor
        self.condensed_neighbors[neighbor_vertex_id] = neighbor_vertex_weight

        neighbor_vertex = vertices[neighbor_vertex_id]
        new_neighbor_id, new_neighbor_weight = neighbor_vertex.neighbor
        if new_neighbor_id:
            self.neighbor = (new_neighbor_id, new_neighbor_weight + neighbor_vertex_weight)
        else:
            self.neighbor = (None, None)

        neighbor_vertex.trivial = True


    def pack(self):
        """ TODO: make pack() serialize the data in protobuf or similar?
        """
        compact_list = [
            self.station_id,
            self.branch_id,
            self.time_,
            self.neighbor,
            self.transfers,
            #self.condensed_neighbors
        ]
        return compact_list

    def __init__(self, time_, trip_id):
        self.id_ = small_hash( (time_, trip_id) )
        self.time_ = time_
        self.in_degree = self.out_degree = 0
        self.neighbor = (None, None)
        self.transfers = {}
        self.condensed_neighbors = {}
        self.prev_station = self.next_station = None
        self.trivial = False
        #self.trip_id = trip_id


class RealtimeHandler:
    """Gets a new realtime GTFS feed
    """

    def stop_name(self, stop_id):
        try:
            return self.static_data['stops'][stop_id]["info"]["name"]
        except KeyError:
            return self.static_data['stops'][stop_id + 'N']["info"]["name"]

    def get_static(self):
        """Loads the static.json file into self.static_data"""
        static_json = self.gtfs_settings.static_json_path+'/static.json'
        with open(static_json, mode='r') as static_json_file:
            self.static_data = json.loads(static_json_file.read())
        self.static_data['trains'] = misc.NestedDict()

    def eventlet_fetch(self, url, attempt=1):
        max_attempts = 2
        with eventlet.Timeout(misc.TIMEOUT):
            try:
                with request.urlopen(url) as response:
                    return response.read()
            except (OSError, urllib_error.URLError, eventlet.Timeout) as err:
                if attempt < 2:
                    parser_logger.info('%s: unable to connect to %s, RETRYING', err, url)
                    self.eventlet_fetch(url, attempt + 1)
                else:
                    parser_logger.error('%s: unable to connect to %s, FAILED after %s attempts', err, url, attempt)
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

        with open(f'{self.gtfs_settings.realtime_data_path}/realtime', 'wb') as realtime_raw_file:
            realtime_raw_file.write(all_bytes)

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
        self.vertices = {}
        self.stations = {}

        self.parsed_data = self.static_data
        self.parsed_data['realtime_timestamp'] = self.realtime_data.header.timestamp

        for _, route_data in self.parsed_data['routes'].items():
            route_data.pop('shapes')

        for entity in self.realtime_data.entity:
            eventlet.greenthread.sleep(0) # yield time to other server processes if necessary

            if entity.HasField('trip_update'):
                trip_id, branch_id, route_id = self.entity_info(entity.trip_update)
                if not branch_id:
                    continue

                prev_vertex_time = prev_vertex = prev_station_id = None
                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id
                    try:
                        station_id = self.static_data['stops'][stop_id]['info']['parent_station']
                    except KeyError:
                        continue

                    if stop_time_update.arrival.time > time.time():
                        vertex_time = stop_time_update.arrival.time

                        vertex = TransitVertex(vertex_time, trip_id)
                        vertex.add_info(station_id, route_id, branch_id, stop_id)

                        self.vertices[vertex.id_] = vertex

                        try:
                            station = self.stations[station_id]
                            station.add_vertex(branch_id, vertex.id_, vertex_time)
                        except KeyError:
                            station = Station(station_id)
                            station.add_vertex(branch_id, vertex.id_, vertex_time)
                            self.stations[station_id] = station

                        if prev_vertex:
                            prev_vertex.add_next_station( station_id )
                            prev_vertex.set_neighbor( vertex )

                            vertex.add_prev_station( prev_station_id )

                        prev_station_id, prev_vertex, prev_vertex_time = station_id, vertex, vertex_time


            elif entity.HasField('vehicle'):
                trip_id, branch_id, route_id = self.entity_info(entity.vehicle)
                if not branch_id:
                    continue

                self.parsed_data['trains'][route_id][branch_id][trip_id]['next_stop'] = entity.vehicle.stop_id
                self.parsed_data['trains'][route_id][branch_id][trip_id]['current_status'] = entity.vehicle.current_status
                self.parsed_data['trains'][route_id][branch_id][trip_id]['last_detected_movement'] = entity.vehicle.timestamp

    def add_transfer_edges(self): # TODO: FIX THIS, it's not working
        vertices = self.vertices

        for _, vertex in vertices.items():

            station = self.stations[vertex.station_id]
            add_tranfers_for_vertex_and_station(vertices, vertex, station)

            try:
                for transfer_station_id, min_transfer_time in self.static_data['transfers'][vertex.station_id].items():
                    transfer_station = self.stations[transfer_station_id]
                    add_tranfers_for_vertex_and_station(vertices, vertex, transfer_station, int(min_transfer_time))
            except KeyError:
                # (no additional transfer stations)
                pass

        V = E = 0
        for _, vertex in self.vertices.items():
            V = V + 1

            if vertex.neighbor[0]:
                E = E + 1

            for transfer_id, _ in vertex.transfers.items():
                E = E + 1

        print('Generated a graph with', V, 'vertices, and', E, 'edges.\n')

    def condense_trivial_vertices(self):
        for _, vertex in self.vertices.items():
            if vertex.trivial:
                continue

            try:
                neighbor_vertex = self.vertices[vertex.neighbor[0]]
                while neighbor_vertex.in_degree == 1 and neighbor_vertex.out_degree <= 1:
                    vertex.condense_neighbor(self.vertices)
                    neighbor_vertex = self.vertices[vertex.neighbor[0]]

            except KeyError:
                pass

        self.compressed_vertices = {}
        for vertex_id, vertex in self.vertices.items():
            if not vertex.trivial:
                self.compressed_vertices[vertex_id] = vertex


    def pack_graph(self):
            self.packed_vertices = {
                vertex.id_: vertex.pack() for _, vertex in self.vertices.items()
            }

            self.packed_stations = {
                station.id_: station.pack() for _, station in self.stations.items()
            }

            self.parsed_data['vertices'] = self.packed_vertices
            self.parsed_data['stations'] = self.packed_stations


    def viz(self):

        dot = Digraph()

        #random_vertex_id = random.choice( list( self.vertices.keys() ) )
        #branch_ = self.vertices[random_vertex_id].branch_id
        for vertex_id, vertex in self.vertices.items():
            #if '4' not in vertex.branch_id:
            #    continue

            stop_info = self.static_data['stops'][vertex.station_id+'N']['info']
            #x_ = int( (float(stop_info["lat"]) - 40.7590) * 2500 )
            #y_ = int( (float(stop_info["lon"]) + 73.9845) * 2500 )
            dot.node(vertex_id, f'{stop_info["name"]} {vertex.branch_id}')#, pos=f'{x_},{y_}!')

            if vertex.neighbor[0]:
                dot.edge(vertex_id, vertex.neighbor[0], str(vertex.neighbor[1]))

            for transfer_vertex_id, tup in vertex.transfers.items():
                time_before_departure, weight = tup
                dot.edge(vertex_id, transfer_vertex_id, f'{time_before_departure}, {weight}')

            #for c_n, weight in vertex.condensed_neighbors.items():
            #    dot.edge(vertex_id, c_n, f'condensed: {weight}')

        print('rendering...')
        dot.render('neato', format='pdf')

        V = E = 0
        for _, vertex in self.compressed_vertices.items():
            V = V + 1

            if vertex.neighbor[0]:
                E = E + 1

            for transfer_id, _ in vertex.transfers.items():
                E = E + 1

        print(V, 'vertices, and', E, 'edges')

        degrees = {}
        for _, vertex in self.compressed_vertices.items():
            #if vertex.in_degree == vertex.out_degree == 1:
            degree_tup = (vertex.in_degree, vertex.out_degree)
            try:
                degrees[degree_tup] = degrees[degree_tup] + 1
            except KeyError:
                degrees[degree_tup] = 1

        pp.pprint(degrees)

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
                exit()

            parser_logger.info('%s/realtime.json does not exist, attempting to create it', json_path)
            try:
                os.makedirs(json_path, exist_ok=True)

            except PermissionError:
                parser_logger.error('Do not have permission to write to %s/realtime.json\n', json_path)
                exit()

            self.to_json(attempt=attempt+1)

    def get_prev_feed(self):
        with open('/data/transit_data_access/db_server/MTA/realtime/raw/realtime', 'rb') as example_realtime_feed:
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(example_realtime_feed.read())

        self.realtime_data = feed_message

        return True

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

def add_tranfers_for_vertex_and_station(vertices, vertex, station, min_transfer_time=0):
    for branch_id, vertices_for_branch in station.vertices_by_branch.items():
        # TODO method to get next allowable time based on transfer time between branches at stop and vertex.time_
        if vertex.branch_id == branch_id:
            continue

        split_index = bisect.bisect_right( vertices_for_branch, (vertex.time_ + min_transfer_time, ) )

        try:
            next_vertex_in_branch_time, next_vertex_in_branch_id = vertices_for_branch[split_index]

            wait_time = next_vertex_in_branch_time - vertex.time_

            transfer_vertex_id, _ = vertices[next_vertex_in_branch_id].neighbor
            try:
                transfer_vertex = vertices[ transfer_vertex_id ]
            except KeyError:
                continue

            if transfer_vertex.station_id not in [vertex.prev_station, vertex.next_station]: # (if the transfer_vertex is not headed to the current vertex's station or to the neighbor's next station)
            #if transfer_vertex.station_id != vertex.prev_station:
                vertex.add_transfer( transfer_vertex, wait_time )

        except IndexError:
            # there are no vertices for this branch after this vertex's time
            pass


def dijkstra(realtime_handler_obj, starting_station_id, ending_station_id):
    """ implements dijkstra's shortest-path algorithm
    """
    vertices, stations, stop_name = realtime_handler_obj.vertices, realtime_handler_obj.stations, realtime_handler_obj.stop_name

    if starting_station_id not in stations:
        print(f'{starting_station_id} not found')
        return False

    if ending_station_id not in stations:
        print(f'{ending_station_id} not found')
        return False

    starting_station = stations[starting_station_id]
    ending_station = stations[ending_station_id]

    starting_vertex = TransitVertex(time.time(), 'unique')
    starting_vertex.add_info(starting_station_id, None, None, None)
    vertices[starting_vertex.id_] = starting_vertex
    ending_vertices = ending_station.vertices

    add_tranfers_for_vertex_and_station(vertices, starting_vertex, starting_station)

    weights = {vertex_id: float('infinity') for vertex_id, _ in vertices.items()}
    paths = {vertex_id: [] for vertex_id, _ in vertices.items()}

    weights[starting_vertex.id_] = 0
    paths[starting_vertex.id_] = [ (False, starting_station_id, None, 0) ]

    queue = []
    #orig_queue = []

    for vertex_id, weight in weights.items():
        heapq.heappush(queue, (weight, vertex_id))
        #heapq.heappush(orig_queue, (weight, vertex_id))

    best_end_vertex = best_end_vertex_id = None

    removed = []

    while len(queue) > 0:
        entry = heapq.heappop(queue)
        if entry in removed:
            continue

        current_weight, current_vertex_id = entry

        vertex = vertices[current_vertex_id]

        if current_vertex_id in ending_vertices:
            best_end_vertex_id = current_vertex_id
            best_end_vertex = vertices[best_end_vertex_id]
            break

        neighbor_nodes = {}
        if vertex.neighbor[0]:
            neighbor_nodes[vertex.neighbor[0]] = int(vertex.neighbor[1])

        # TODO check for transfers not None?
        for transfer_node_id, transfer_node_time in vertex.transfers.items():
                neighbor_nodes[transfer_node_id] = int(transfer_node_time[0])+int(transfer_node_time[1])

        for neighbor_id, neighbor_weight in neighbor_nodes.items():
            new_weight = weights[current_vertex_id] + neighbor_weight

            #if neighbor_id in ending_vertices:
            #    print(neighbor_id, new_weight)

            if new_weight < weights[neighbor_id]:
                #try:
                removed.append( (weights[neighbor_id], neighbor_id) )
                heapq.heappush(queue, (new_weight, neighbor_id) )
                #except ValueError:
                #    pass

                weights[neighbor_id] = new_weight

                if neighbor_id in vertex.transfers.keys():
                    is_transfer = True
                    travel_time = int(transfer_node_time[1])
                    wait_time   = int(transfer_node_time[0])
                else:
                    is_transfer = False
                    travel_time = neighbor_weight
                    wait_time = 0

                new_path_item = (
                    is_transfer,
                    vertices[neighbor_id].station_id,
                    vertices[neighbor_id].route_id,
                    travel_time,
                    wait_time
                )

                new_path = paths[current_vertex_id] + [new_path_item]
                paths[neighbor_id] = new_path

    """
    best_weight = float('infinity')
    print(ending_vertices)
    for ending_vertex_id in ending_vertices:
        print(ending_vertex_id, weights[ending_vertex_id])
        if weights[ending_vertex_id] < best_weight:
            best_weight = weights[ending_vertex_id]
            best_end_vertex_id = ending_vertex_id

    best_end_vertex = vertices[best_end_vertex_id]
    """

    if weights[best_end_vertex_id] == float('infinity'):
        print('No path found.')
        return False

    out_str = f'The shortest path from {stop_name(starting_station_id)} to {stop_name(best_end_vertex.station_id)} takes {misc.hr_min_sec(weights[best_end_vertex_id])}'
    print(out_str)
    print('-'*len(out_str),'\n')

    full_path = paths[best_end_vertex_id]
    full_path.pop(0)

    for path_item in full_path:
        is_transfer, station_id, route_id, travel_time, wait_time = path_item
        if is_transfer:
            print('TRANSFER TO', route_id, '- wait time:', misc.hr_min_sec(wait_time))
            print('    ride to', stop_name(station_id), 'which takes', misc.hr_min_sec(travel_time))
        else:
            print('    ride to', stop_name(station_id), 'which takes', misc.hr_min_sec(travel_time))

    print('\n')

def main():
    """Creates new RealtimeHandler, then calls get_static() and check_feed()
    """
    parser_logger.info('\n')

    with misc.TimeLogger('realtime.py') as _tl_all:
        with misc.TimeLogger('get_static()') as _tl:
            realtime_handler = RealtimeHandler(MTA_SETTINGS, name='MTA')
            realtime_handler.get_static()

        with misc.TimeLogger('check_feed()') as _tl:
            feed_is_new = realtime_handler.check_feed()

        if not feed_is_new:
            realtime_handler.get_prev_feed()

        with misc.TimeLogger('parse_feed()') as _tl:
            realtime_handler.parse_feed()

        with misc.TimeLogger('add_transfer_edges()') as _tl:
            realtime_handler.add_transfer_edges()

        #with misc.TimeLogger('condense_trivial_vertices()') as _tl:
        #    realtime_handler.condense_trivial_vertices()

        with misc.TimeLogger('dijkstra()') as _tl:
            dijkstra(realtime_handler, sys.argv[1], sys.argv[2])

        realtime_handler.pack_graph()
        realtime_handler.to_json()

        return feed_is_new

if __name__ == '__main__':
    main()
