"""Classes and methods for static GTFS data
"""
import logging
import time
import os
from collections import defaultdict
import bisect
import json
import eventlet
from eventlet.green.urllib import request
from eventlet.green.urllib import error as urllib_error
import warnings
import pprint as pp

import gzip
from google.transit import gtfs_realtime_pb2
from google.protobuf import message as protobuf_message

from transit_system_config import MTA_SETTINGS
import misc

parser_logger = misc.parser_logger

eventlet.monkey_patch()

DEP, ARR = 0, 1

def add_sorted(list_, element):
    index = bisect.bisect_left(list_, element)
    list_.insert(index, element)


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
        self.realtime_graph = defaultdict(dict)
        self.realtime_vertices_by_stop = defaultdict(lambda: defaultdict(list))
        self.realtime_vertices_info = defaultdict(dict)

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

                prev_departure_time = prev_station = None
                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id
                    try:
                        station_id = self.static_data['stops'][stop_id]['info']['parent_station']
                    except KeyError:
                        #print(f'{stop_id} not found in stops.txt')
                        continue

                    if stop_time_update.arrival.time > time.time():
                        arrival_time = stop_time_update.arrival.time
                        try:
                            self.parsed_data['stops'][stop_id]['arrivals'][route_id][branch_id].append(arrival_time)
                        except AttributeError: #if self...[branch_id] is not yet a list
                            self.parsed_data['stops'][stop_id]['arrivals'][route_id][branch_id] = [arrival_time]
                        except KeyError: #if self...['arrivals'] is not yet a NestedDict
                            try:
                                self.parsed_data['stops'][stop_id]['arrivals'] = misc.NestedDict()
                                self.parsed_data['stops'][stop_id]['arrivals'][route_id][branch_id] = [arrival_time]
                            except KeyError:
                                parser_logger.debug('stop_id %s is not in stops.txt', stop_id)

                        try:
                            self.parsed_data['trains'][route_id][branch_id][trip_id]['arrival_time'] = arrival_time
                        except KeyError: #if self.parsed_data['trains'] is not yet a NestedDict
                            self.parsed_data['trains'] = misc.NestedDict()
                            self.parsed_data['trains'][route_id][branch_id][trip_id]['arrival_time'] = arrival_time

                        # GRAPH STUFF:

                        # create the current arrival vertex and add it to realtime_vertices_by_stop
                        vertex_this_A = (ARR, arrival_time, trip_id, station_id)
                        add_sorted(self.realtime_vertices_by_stop[station_id][branch_id], vertex_this_A)

                        self.realtime_vertices_info[vertex_this_A] = {
                            'type': ARR,
                            'station_id': station_id,
                            'prev_station': prev_station,
                            'arrival_time': arrival_time
                        }

                        # if this isn't the first stop_time_update for this trip_id, add the edge of previous departure -> this arrival to realtime_graph
                        if prev_departure_time:
                            vertex_prev_D = (DEP, prev_departure_time, trip_id, prev_station)
                            self.realtime_vertices_info[vertex_prev_D]['next_station'] = station_id

                            self.realtime_graph[vertex_prev_D][vertex_this_A] = arrival_time - prev_departure_time

                        # if this isn't the last stop_time_update for this trip_id, create the current departure vertex and add it to realtime_vertices_by_stop
                        # and, add the edge of this arrival -> this departure to realtime_graph
                        # and, load this departure_time into prev_departure_time to use with the next stop_time_update
                        departure_time = stop_time_update.departure.time

                        if departure_time:
                            vertex_this_D = (DEP, departure_time, trip_id, station_id)
                            add_sorted(self.realtime_vertices_by_stop[station_id][branch_id], vertex_this_D)

                            self.realtime_vertices_info[vertex_this_D] = {
                                'type': DEP,
                                'station_id': station_id,
                                'next_station': None,
                                'departure_time': departure_time
                            }

                            self.realtime_graph[vertex_this_A][vertex_this_D] = departure_time - arrival_time # this is 0 for all MTA subway GTFS data

                            prev_departure_time = departure_time
                            prev_station = station_id



            elif entity.HasField('vehicle'):
                trip_id, branch_id, route_id = self.entity_info(entity.vehicle)
                if not branch_id:
                    continue

                self.parsed_data['trains'][route_id][branch_id][trip_id]['next_stop'] = entity.vehicle.stop_id
                self.parsed_data['trains'][route_id][branch_id][trip_id]['current_status'] = entity.vehicle.current_status
                self.parsed_data['trains'][route_id][branch_id][trip_id]['last_detected_movement'] = entity.vehicle.timestamp

        #pp.pprint(self.realtime_vertices_by_stop)

    def add_transfer_edges(self):
        for station_id, branches in self.realtime_vertices_by_stop.items():
            for branch_id, vertices in branches.items():
                split_index = bisect.bisect_right( vertices, (ARR, ) )
                departures = vertices[:split_index]
                arrivals = vertices[split_index:]

                for arrival in arrivals:
                    arrival_time = arrival[1]
                    other_branches = dict(branches)
                    other_branches.pop(branch_id)

                    if other_branches:
                        for other_branch_id, other_vertices in other_branches.items():
                            split_index = bisect.bisect_right( other_vertices, (ARR, ) )
                            other_departures = other_vertices[:split_index]

                            _index = bisect.bisect_left( other_departures, (DEP, arrival_time, ) )
                            try:
                                transfer_departure = other_departures[_index]
                                transfer_departure_time = transfer_departure[1]

                                prev_station = self.realtime_vertices_info[arrival]['prev_station']
                                transfer_next_station = self.realtime_vertices_info[transfer_departure]['next_station']

                                #print(prev_station,transfer_next_station)
                                if transfer_next_station != prev_station and transfer_next_station: # if it's not taking you back to the previous stop or taking you nowhere
                                    #print(arrival, transfer_departure, transfer_departure_time - arrival_time)
                                    self.realtime_graph[arrival][transfer_departure] = transfer_departure_time - arrival_time
                                    if transfer_departure_time < arrival_time:
                                        print(transfer_departure_time, arrival_time)

                            except IndexError:
                                #print('no upcoming departures')
                                pass

                            except KeyError as e:
                                print('key error:', self.realtime_vertices_info[arrival])
                                print(e)
                                exit()

        #pp.pprint(self.realtime_graph)


    def hash_graph(self):
        self.realtime_graph_hashed = defaultdict(dict)
        self.realtime_vertices_info_hashed = defaultdict(dict)
        hashes = {}
        i = 0

        for arrival, departures in self.realtime_graph.items():
            for departure, edge_time in departures.items():
                try:
                    arrival_number = hashes[hash(arrival)]
                except KeyError:
                    arrival_number = i
                    hashes[hash(arrival)] = arrival_number
                    i = i+1

                try:
                    departure_number = hashes[hash(departure)]
                except KeyError:
                    departure_number = i
                    hashes[hash(departure)] = departure_number
                    i = i+1

                self.realtime_graph_hashed[arrival_number][departure_number] = edge_time

        for vertex, info in self.realtime_vertices_info.items():
            try:
                vertex_number = hashes[hash(vertex)]
            except KeyError:
                vertex_number = i
                hashes[hash(vertex)] = vertex_number
                i = i+1
            self.realtime_vertices_info_hashed[vertex_number] = info


        #pp.pprint(self.realtime_graph_hashed)
        #pp.pprint(self.realtime_vertices_info_hashed)

        self.parsed_data['graph'] = {}

        self.parsed_data['graph']['edges'] = self.realtime_graph_hashed
        self.parsed_data['graph']['vertices'] = self.realtime_vertices_info_hashed

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
            realtime_handler.hash_graph()
            realtime_handler.to_json()

        return feed_is_new

if __name__ == '__main__':
    main()
