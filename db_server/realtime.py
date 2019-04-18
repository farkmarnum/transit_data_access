"""Classes and methods for static GTFS data
"""
from collections import defaultdict, OrderedDict
from contextlib import suppress
import base64
import bisect
import copy
import gzip
import heapq
import json
import logging
import os
import random
import sys
import time
import warnings

from eventlet.green.urllib import error as urllib_error
from eventlet.green.urllib import request
from google.protobuf import message as protobuf_message
from google.transit import gtfs_realtime_pb2
from graphviz import Digraph
import eventlet
import pprint as pp
import pyhash

from transit_system_config import MTA_SETTINGS
import misc


RIDE, TRANSFER, WALK = list(range(0,3))
STATION, LOC = list(range(0,2))
DIRECTIONS = {
    'N': 0,
    'S': 1
}

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
        if edge_type == RIDE:
            if self.neighbors[RIDE]:
                print('ERROR: can\'t reassign RIDE neighbor for ', neighbor_vertex.id_)
                # there's only one RIDE neighbor possible
                return False
            else:
                self.neighbors[RIDE] = (neighbor_vertex.id_, ride_time)

        elif edge_type == TRANSFER:
            transfer_time, wait_time = times
            self.neighbors[TRANSFER][neighbor_vertex.id_] = (
                transfer_time,
                wait_time,
                ride_time
            )

        elif edge_type == WALK:
            exit_time, walk_time, enter_time, wait_time = times
            self.neighbors[WALK][neighbor_vertex.id_] = (
                exit_time,
                walk_time,
                enter_time,
                wait_time,
                ride_time
            )

        self.degree[1], neighbor_vertex.degree[0] += 1


    def pack(self):
        """ TODO: make pack() serialize the data in flatbuffers or similar?
        """
        pass

    def __init__(self, trip_id, vertex_time, vertex_type=STATION, station_id=None, route_id=None, direction=None, loc=None):
        self.type_ = vertex_type
        self.time_ = vertex_time
        self.trip_id = trip_id

        # Generate a unique ID:
        if self.type_ == STATION:
            self.id_ = small_hash( (trip_id, station_id) ) # forms a unique ID
            self.station_id = station_id
            self.route_id = route_id
            self.direction = direction
        elif self.type_ == LOC:
            self.id_ = small_hash( (trip_id, loc) ) # forms a unique ID
            self.loc = loc

        self.neighbors = {
            RIDE: tuple(),
            TRANSFER: defaultdict(),
            WALK: defaultdict()
        }
        self.degree = (0, 0)
        self.prev_station = None
        self.next_station = None


class RealtimeHandler:
    """Gets a new realtime GTFS feed
    """
    def parse_stop_id(self, stop_id):
        try:
            station_id = self.station_id_lookup[stop_id]
        except KeyError:
            parser_logger.warning('stop_id %s not found in self.station_id_lookup', stop_id)
            return False, False

        try:
            direction = DIRECTIONS[stop_id[-1]]
        except KeyError:
            parser_logger.warning('direction %s not found in DIRECTIONS', direction)
            return False, False

        return direction, station_id

    def add_arrival(self, station_id, vertex_id, route_id, direction, arrival_time):
        arrivals = self.realtime_data['stations'][station_id]['arrivals']
        bisect.insort(
            arrivals[(route_id, direction)],
            (arrival_time, vertex_id)
        )

    def get_static(self):
        """Loads the static.json file into self.static_data"""
        static_json = self.gtfs_settings.static_json_path+'/static.json'
        with open(static_json, mode='r') as static_json_file:
            self.static_data = json.loads(static_json_file.read())

        self.static_data['trains'] = misc.NestedDict()
        self.station_id_lookup = self.static_data['station_id_lookup']
        self.route_id_lookup = self.static_data['route_id_lookup']


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


    def parse_feed(self):
        """ Walks through self.realtime_data and creates self.parsed_data
        Uses self.static_data as a starting point

        Also creates self.realtime_graph, a directed graph of all trip stops
        """
        self.vertices = {}
        self.parsed_data = self.static_data
        self.parsed_data['realtime_timestamp'] = self.realtime_data.header.timestamp
        self.parsed_data['trains'] = defaultdict(dict)

        for entity in self.realtime_data.entity:
            eventlet.greenthread.sleep(0) # yield time to other server processes if necessary

            if entity.HasField('trip_update'):
                trip_id = entity_body.trip.trip_id
                route_id = entity_body.trip.route_id
                route_id = self.route_id_lookup[route_id]

                prev_vertex_time, prev_vertex, prev_station_id = [None] * 3
                for stop_time_update in entity.trip_update.stop_time_update:
                    direction, station_id = parse_stop_id(stop_time_update.stop_id)

                    if stop_time_update.arrival.time > time.time():
                        vertex_time = stop_time_update.arrival.time

                        vertex = TransitVertex(
                            trip_id,
                            vertex_time,
                            vertex_type=STATION,
                            station_id=station_id,
                            route_id=route_id,
                            direction=direction
                        )
                        self.vertices[vertex.id_] = vertex

                        add_arrival(station_id, vertex.id_, route_id, direction, vertex_time)

                        if prev_vertex:
                            prev_vertex.add_next_station(station_id)
                            prev_vertex.set_neighbor(vertex)

                            vertex.add_prev_station(prev_station_id)

                        prev_station_id, prev_vertex, prev_vertex_time = station_id, vertex, vertex_time

            elif entity.HasField('vehicle'):
                trip_id = entity_body.trip.trip_id
                route_id = entity_body.trip.route_id
                route_id = self.route_id_lookup[route_id]
                direction, station_id = parse_stop_id(entity.vehicle.stop_id)
                # TODO: figure out previous station!!
                # TODO add last stop??
                self.parsed_data['trains'][(route_id, direction)][trip_id] = {
                    'current_station': station_id,
                    'previous_station': None, # TODO TODO TODO
                    'current_status': entity.vehicle.current_status,
                    'last_detected_movement': entity.vehicle.timestamp
                }

    def add_edges_to_vertex_at_station(vertex, edge_type, station, times):
        if edge_type == TRANSFER:
            min_transfer_time = times
            next_allowable_time = vertex.time_ + min_transfer_time
        elif edge_type == WALK:
            exit_time, walk_time, enter_time = times
            next_allowable_time = vertex.time_ + exit_time + walk_time + enter_time

        for branch_id, vertices_for_branch in station.vertices_by_branch.items():
            if vertex.branch_id == branch_id and vertex.station_id == station.id_: # don't add edges to vertices from the same branch at the same station
                continue

            split_index = bisect.bisect_right( vertices_for_branch, (next_allowable_time, ) )
            try:
                nextvertex_time, nextvertex_id = vertices_for_branch[split_index]
            except IndexError: # there are no vertices for this branch after this vertex's time
                continue

            transfer_vertex_id, transfer_vertex_time = self.vertices[next_vertex_in_branch_id].neighbor
            transfer_vertex = self.vertices[transfer_vertex_id]

            if transfer_vertex.station_id not in [vertex.prev_station, vertex.next_station]:
                # (if the transfer_vertex is not headed to the current vertex's station or to the neighbor's next station)
                continue

            wait_time = nextvertex_time - next_allowable_time
            ride_time = transfer_vertex_time - nextvertex_time

            if edge_type == TRANSFER:
                neighbor_times_arg = (min_transfer_time, wait_time)
            elif edge_type == WALK:
                neighbor_times_arg = (exit_time, walk_time, enter_time, wait_time)
            else:
                return False

            vertex.add_neighbor(TRANSFER, transfer_vertex, ride_time, neighbor_times_arg )


    def add_transfer_edges(self, vertex):
        """ This is called during the creation of the realtime graph.
        It adds transfer edges to each vertex where possible -- as defined in self.static_data['transfers']
        """
        try:
            transfer_stations = self.static_data['transfers'][vertex.station_id]
        except KeyError:
            transfer_stations = {}

        if vertex.station_id and not transfer_stations[vertex.station_id]: # TODO: this seems problematic...
            transfer_stations[vertex.station_id] = 0

        for transfer_station_id, min_transfer_time in transfer_stations:
            transfer_station = self.stations[transfer_station_id]
            add_edges_to_vertex_at_station(vertex, TRANSFER, transfer_station, min_transfer_time)

    def add_all_transfer_edges(self):
        for _, vertex in self.vertices.items():
            self.add_transfer_edges(vertex)

    def add_walk_edges(self, vertex):
        """ This is called during the creation of the realtime graph.
        It adds walk edges to each vertex where possible -- as defined in self.static_data['walkable']
        """
        try:
            walkable_stations = self.static_data['walkable'][vertex.station_id]
        except KeyError:
            walkable_stations = {}

        if vertex.station_id and not walkable_stations[vertex.station_id]: # TODO: this seems problematic...
            walkable_stations[vertex.station_id] = 0

        for walkable_station_id, times in walkable_stations:
            walkable_station = self.stations[walkable_station_id]
            add_edges_to_vertex_at_station(vertex, WALK, walkable_station, times)


    def graph_info(self):
        V = E = 0
        for _, vertex in self.vertices.items():
            V = V + 1

            if vertex.neighbor[0]:
                E = E + 1

            for transfer_id, _ in vertex.transfers.items():
                E = E + 1

        print('Generated a graph with', V, 'vertices, and', E, 'edges.\n')

    def shortest_paths(self, starting_station_id, ending_station_id):
        """ implements dijkstra's shortest-path algorithm
        """
        shortest_paths_count = 0


        """ add a vertex for the origin and for the destination: """
        starting_station = stations[starting_station_id]
        #starting_vertex = TransitVertex(trip_id='unique trip_id 1', time_=time.time(), station_id=None, branch_id=None)

        # TODO: add actual walk edges from starting_vertex
        starting_vertex = TransitVertex(trip_id='unique trip_id 1', time_=time.time(), station_id=starting_station_id, branch_id=None)
        vertices[starting_vertex.id_] = starting_vertex

        ending_station = stations[ending_station_id]
        # TODO: add actual walk edges from ending_vertex
        ending_vertex = TransitVertex(trip_id='unique trip_id 2', time_=None, station_id=ending_station_id, branch_id=None)
        vertices[ending_vertex.id_] = ending_vertex


        """add walk edges from the origin vertex to upcoming trains:
        1) determine which stations are in walking distance
        2) for each station, add walking paths to that station

        For now, since I'm testing with sources that are station_ids, just add 'walk' edges to the station from directly above the station
        """
        try:
            starting_stations = self.static_data['transfers'][starting_station_id]
        except KeyError:
            starting_stations = {}

        if not starting_stations[starting_station_id]: # TODO: this seems problematic...
            starting_stations[starting_station_id] = 0

        for _station_id, _ in starting_stations:
            _station  = self.stations[_station_id]
            #
            times = (0, 0, 0) # 'walking' to the station takes exit_time = walk_time = enter_time = 0 if you're in the station already
            add_edges_to_vertex_at_station(vertex, WALK, _station, times)


        ###### add walk edges from the origin vertex to upcoming trains: ######
        penultimate_vertices = set()
        for tup_list in ending_station.vertices_by_branch.values():
            for _, vertex_id in tup_list:
                penultimate_vertices.add(vertex_id)

        for penultimate_vertex_id in penultimate_vertices:
            penultimate_vertex = vertices[penultimate_vertex_id]

            travel_time_to_target = 0 #TODO for non-station destinations

            penultimate_vertex.neighbor = (ending_vertex.id_, neighbor_vertex.time_ - self.time_)
            # OLD:
            # success_vertices.append(success_vertex.id_)

            # TODO: add actual walk edges
            times = (0, 0, 0)
            penultimate_vertex.add_neighbor(WALK, ending_vertex, 0, times)

        # TODO implement multiple shortest paths
        self.dijkstra(starting_vertex.id_, ending_vertex.id_)


    def dijkstra(self, starting_vertex, ending_vertex):

        weights = {vertex_id: float('infinity') for vertex_id, _ in self.vertices.items()}
        paths = {vertex_id: [] for vertex_id, _ in self.vertices.items()}

        weights[starting_vertex.id_] = 0
        paths[starting_vertex.id_] = [ (False, starting_station_id, None, 0) ]

        queue = []
        removed = []

        best_end_vertex_id = None

        for vertex_id, weight in weights.items():
            heapq.heappush(queue, (weight, vertex_id))


        while len(queue) > 0:
            entry = heapq.heappop(queue)
            if entry in removed:
                continue

            current_weight, current_vertex_id = entry

            vertex = vertices[current_vertex_id]


            if current_vertex_id == ending_vertex:
                best_end_vertex_id = current_vertex_id
                break

            neighbor_nodes = {
                neighbor_id: sum(times) for neighbor_id, times in {
                    **vertex.neighbors[RIDE],
                    **vertex.neighbors[TRANSFER]
                }
            }
            if vertex.neighbors[RIDE]:
                neighbor_nodes[vertex.neighbors[RIDE][0]] = vertex.neighbors[RIDE][1]

            for transfer_node_id, transfer_node_time in vertex.transfers.items():
                    neighbor_nodes[transfer_node_id] = int(transfer_node_time[0])+int(transfer_node_time[1])

            for neighbor_id, neighbor_weight in neighbor_nodes.items():
                new_weight = weights[current_vertex_id] + neighbor_weight

                if new_weight < weights[neighbor_id]:
                    removed.append( (weights[neighbor_id], neighbor_id) )
                    heapq.heappush(queue, (new_weight, neighbor_id) )

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
                        vertices[neighbor_id].branch_id,
                        travel_time,
                        wait_time
                    )

                    new_path = paths[current_vertex_id] + [new_path_item] # TODO add details from times tuple
                    paths[neighbor_id] = new_path

        # TODO: put this output stuff back in shortest_paths()
        out_str = f'The shortest path from {get_stop_name(starting_station_id)} to {get_stop_name(vertices[best_end_vertex_id].station_id)} takes {misc.hr_min_sec(weights[best_end_vertex_id])}'
        print(out_str)
        print('-'*len(out_str),'\n')

        full_path = paths[best_end_vertex_id]
        full_path.pop(0)

        for path_item in full_path:
            is_transfer, station_id, branch_id, travel_time, wait_time = path_item
            if is_transfer:
                print('TRANSFER TO', branch_id, 'at', get_stop_name(station_id), '- wait time:', misc.hr_min_sec(wait_time))
                print('    ride to', get_stop_name(station_id), 'which takes', misc.hr_min_sec(travel_time))
            else:
                print('    ride to', get_stop_name(station_id), 'which takes', misc.hr_min_sec(travel_time))
                pass

        print()

    """
    def condense_trivial_vertices(self):
        for _, vertex in self.vertices.items():
            if vertex.trivial:
                continue

            try:
                neighbor_vertex = self.vertices[vertex.neighbor[0]]
                while neighbor_vertex.degree[0] == 1 and neighbor_vertex.degree[1] <= 1:
                    vertex.condense_neighbor(self.vertices)
                    neighbor_vertex = self.vertices[vertex.neighbor[0]]

            except KeyError:
                pass

        self.compressed_vertices = {}
        for vertex_id, vertex in self.vertices.items():
            if not vertex.trivial:
                self.compressed_vertices[vertex_id] = vertex
    """

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
            #if vertex.degree == (1, 1):
            try:
                degrees[degree] = degrees[degree] + 1
            except KeyError:
                degrees[degree] = 1

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
        self.static_data = {}
        self.parsed_data = {}

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
                realtime_handler.add_all_transfer_edges()

        #with misc.TimeLogger('condense_trivial_vertices()') as _tl:
        #    realtime_handler.condense_trivial_vertices()

        with misc.TimeLogger('dijkstra()') as _tl:
            dijkstra(realtime_handler, sys.argv[1], sys.argv[2])

        realtime_handler.pack_graph()
        realtime_handler.to_json()

        return feed_is_new

if __name__ == '__main__':
    main()
