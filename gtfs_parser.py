#!/usr/bin/python3

import os
import shutil
import types
import time
import csv
from collections import namedtuple
import zipfile
import pickle

import requests
from google.transit import gtfs_realtime_pb2
import graphviz
import pandas as pd

import transit_systems as ts

'''
with open("data_file.txt", newline="") as infile:
    reader = csv.reader(infile)
    _fields = ('a', 'few', 'fields')
    route_info = namedtuple('route_info', _fields)
    next(reader)
    for row in map(route_info._make, reader):
        yield row
'''

def _locate_csv(name, gtfs_settings):
    return '%s/%s.txt' % (gtfs_settings.static_data_path, name)

'''
class Simp:
    """ Class for simple object that can take attributes

        Usage: o = Simp(attr1 = val1, attr2 = val2, attr3 = val3)
    """
    def attr(self):
        return self.__dict__.items()

    def __init__(self, **kwds):
        self.__dict__.update(kwds)
'''

STATUS_MESSAGES = ['approaching', 'stopped at', 'in transit to']



''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''
' '     The following classes describe STATIC components of the transit system:
' '
'''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

class Stop:
    """Analogous to GTFS 'stop_id'.

    Usage: Stop(stop_id)
    id = stop_id, '201S' for example
    """
    id_ = None

    def display(self):
        print('            '+self.id_)

    def __init__(self, stop_id):
        self.id_ = stop_id
        self.upcoming_trains = {}
        self.routes_that_stop_here = {}


class ShapeBuilder:
    """Analogous to GTFS 'shape_id'.

    Usage: Shape(parent_route, shape_id)
    id = shape_id, '2..S08R' for example
    """
    id_ = stops = None

    def add_stop(self, stop_id):
        #setattr(self.stops, stop_id, Stop(stop_id))
        self.stops.update({stop_id:Stop(stop_id)})

    def __init__(self, parent_route, shape_id):
        self.route = parent_route
        self.id_ = shape_id
        #self.stops = types.SimpleNamespace()
        self.stops = {}

class Shape(ShapeBuilder):
    def display(self):
        print('        '+self.id_)
        #for _, stop in self.stops.attr():
        for _, stop in self.stops.items():
            stop.display()

    def __init__(self, parent_route, shape_id):
        super().__init__(parent_route, shape_id)


class RouteBuilder:
    shapes = branches = None

    def add_shape(self, shape_id):
        #setattr(self.shapes, shape_id, Shape(self, shape_id))
        self.shapes.update({shape_id:Shape(self, shape_id)})

    def __init__(self, transit_system, route_info):
        self.transit_system = transit_system
        self.id_ = route_info['id_']
        self.long_name = route_info['long_name']
        self.desc = route_info['desc']
        self.color = route_info['color']
        self.text_color = route_info['text_color']
        #self.shapes = types.SimpleNamespace()
        self.shapes = {}
        #self.branches = types.SimpleNamespace()
        self.branches = {}

class Route(RouteBuilder):
    def display(self):
        print('    '+self.id_)
        #for _, shape in self.shapes.attr():
        for _, shape in self.shapes.items():
            shape.display()

    def __init__(self, transit_system, route_info):
        super().__init__(transit_system, route_info)


class TransitSystemBuilder:
    """Not analogous to GTFS type.

    TODO: docstring for this class
    """
    routes = gtfs_settings = stops_info = routes_info =None

    _has_parent_station_column = True
    _rswn_list_of_columns = [
        'route_id',
        'shape_id',
        'stop_sequence',
        'stop_id',
        'stop_name',
        #'stop_lat',
        #'stop_lon',
        'parent_station'
    ]

    _rswn_sort_by = [
        'route_id',
        'shape_id',
        'stop_sequence'
    ]

    def _parse_stop_id(self, row):
        if self._has_parent_station_column:
            if row['parent_station'] is not None:
                return row['parent_station']
        return row['stop_id']

    def _merge_trips_and_stops(self):
        print("Cross referencing route, stop, and trip information...")

        trips_csv = _locate_csv('trips', self.gtfs_settings)
        stops_csv = _locate_csv('stops', self.gtfs_settings)
        stop_times_csv = _locate_csv('stop_times', self.gtfs_settings)

        trips = pd.read_csv(trips_csv, dtype=str)
        stops = pd.read_csv(stops_csv, dtype=str)
        stop_times = pd.read_csv(stop_times_csv, dtype=str)

        stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(int)

        parse_shape_from_trip = lambda trip_id: trip_id.str.replace(r'.*_(.*?$)', r'\1', regex=True)

        trips['shape_id'] = trips['shape_id'].fillna(parse_shape_from_trip(trips['trip_id']))

        composite = pd.merge(trips, stop_times, how='inner', on='trip_id')
        composite = pd.merge(composite, stops, how='inner', on='stop_id')
        composite = composite[self._rswn_list_of_columns]
        composite = composite.drop_duplicates().sort_values(by=self._rswn_sort_by)

        rswn_csv = _locate_csv('route_stops_with_names', self.gtfs_settings)
        composite.to_csv(rswn_csv, index=False)

    def _load_travel_time_for_stops(self):
        # TODO
        print("Calculating travel time between each stop...")

    def _download_new_schedule_data(self, url, to_):
        if not os.path.exists(to_):
            os.makedirs(to_)
            print(f'Creating {to_}')

        print(f'Downloading GTFS static data from {url}')
        try:
            _new_data = requests.get(url, allow_redirects=True)
            open(f'{to_}/schedule_data.zip', 'wb').write(_new_data.content)
        except requests.exceptions.ConnectionError:
            exit(f'\nERROR:\nFailed to connect to {url}\n')


    def _unzip_new_schedule_data(self, temp_dir, to_):
        _old_data = None
        if os.path.exists(to_):
            _old_data = f'{to_}_OLD'
            print(f'Moving {to_} to {_old_data}')
            os.rename(to_, _old_data)

        os.makedirs(to_)

        print(f'Unzipping schedule_data.zip to {to_}')

        with zipfile.ZipFile(f'{temp_dir}/schedule_data.zip', "r") as zip_ref:
            zip_ref.extractall(to_)

        print(f'Deleting {temp_dir}')
        shutil.rmtree(temp_dir)
        if _old_data:
            print(f'Deleting {_old_data}')
            shutil.rmtree(_old_data)

    def update(self):
        print(f'Updating GTFS static files for {self.name}')
        url = self.gtfs_settings.gtfs_static_url
        path = self.gtfs_settings.static_data_path

        if not os.path.exists('/tmp'):
            os.mkdir('/tmp')
        temp_dir = f'/tmp/gtfs_parser-{int(time.time()*10)}'

        self._download_new_schedule_data(url=url, to_=temp_dir)
        self._unzip_new_schedule_data(temp_dir=temp_dir, to_=path)
        self._merge_trips_and_stops()
        self._load_travel_time_for_stops()

    def load_routes_info(self):
        with open(_locate_csv('routes', self.gtfs_settings), mode='r') as infile:
            reader = csv.DictReader(infile)
            # route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
            for row in reader:
                _route_name = types.SimpleNamespace(
                    long_name=['route_long_name'],
                    desc=row['route_desc'],
                    color=row['route_color'],
                    text_color=row['route_text_color']
                )
                self.routes_info[row['route_id']] = _stop_name

    def load_stops_info(self):
        with open(_locate_csv('stops', self.gtfs_settings), mode='r') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                _stop_name = types.SimpleNamespace(
                    name=row['stop_name'],
                    lat=row['stop_lat'],
                    lon=row['stop_lon']
                )
                self.stops_info[row['stop_id']] = _stop_name

    def add_route(self, route_info):
        route_id = route_info['id_']
        print(f'Adding route: {route_id}')
        self.routes.update({route_id:Route(self, route_info)})

    def load_all_routes(self):
        with open(_locate_csv('routes', self.gtfs_settings), mode='r') as infile:
            csv_reader = csv.DictReader(infile)
            for row in csv_reader:
                route_info = {}
                route_info['id_'] = row['route_id']

                if row['route_color'].strip():
                    route_info['color'] = '#'+row['route_color']
                else:
                    route_info['color'] = 'lightgrey'

                if row['route_text_color'].strip():
                    route_info['text_color'] = '#'+row['route_text_color']
                else:
                    route_info['text_color'] = 'black'

                route_info['long_name'] = row['route_id']+': '+row['route_long_name']
                route_info['desc'] = row['route_desc']

                self.add_route(route_info)

    def build_routes_shapes_stops(self):
        with open(_locate_csv('route_stops_with_names', self.gtfs_settings), mode='r') as rswn:
            csv_reader = csv.DictReader(rswn)
            current_route_id = current_shape_id = None
            for line in csv_reader:
                new_route_id = line['route_id']
                if new_route_id == current_route_id:
                    new_shape_id = line['shape_id']
                    if new_shape_id == current_shape_id:
                        stop_id = self._parse_stop_id(line)
                        shape.add_stop(stop_id)
                    else:
                        current_shape_id = new_shape_id
                        route.add_shape(current_shape_id)
                        shape = route.shapes[current_shape_id]
                else:
                    current_route_id = new_route_id
                    print(f'trying {current_route_id}')
                    route = self.routes[current_route_id]

    def build_branches(self):
        # is this necessary?
        pass

    def build(self):
        print("Loading stop names...")
        self.load_stops_info()
        print("Done.\nLoading route information...")
        self.load_all_routes()
        print("Done.\nLoading GTFS static data into Route Shape and Stop objects...")
        self.build_routes_shapes_stops()
        print("Done.\nConsolidating 'shape.txt' data into a full network for each route...")
        self.build_branches()

    def store(self, outfile):
        with open(outfile, 'wb') as pickle_file:
            pickle.dump(self, pickle_file, pickle.HIGHEST_PROTOCOL)

    #TODO fix this (dill?)
    '''
    def load(self, infile):
        with open(infile, 'rb') as pickle_file:
            self = pickle.load(pickle_file)
    '''

    def __init__(self, name, gtfs_settings):
        self.name = name
        self.gtfs_settings = gtfs_settings
        self.routes = {}
        self.stops_info = {}

class TransitSystem(TransitSystemBuilder):
    def display(self):
        print(self.name)
        for _, route in self.routes.items():
            route.display()

    def map_each_route(self, route_desc_filter=None):
        _stop_networks = {}
        _r = graphviz.Graph()
        print('Mapping the network of stops for: ', end='')
        _first = True
        for route_id, route in self.routes.items():
            if not _first:
                print(', ', end='')
            _first = False
            print(route_id, end='')
            if route_desc_filter is None or route.desc in route_desc_filter:
                _stop_networks[route_id] = graphviz.Graph(name='cluster_'+route_id)
                _stop_networks[route_id].graph_attr['label'] = route_id
                _stop_networks[route_id].graph_attr['fontsize'] = '36'
                _stop_networks[route_id].graph_attr['pencolor'] = '#bbbbbb'

        list_of_edge_str = []
        list_of_nodes = []
        with open(_locate_csv('route_stops_with_names', self.gtfs_settings), mode='r') as infile:
            csv_reader = csv.DictReader(infile)
            prev_stop = ''
            prev_shape = ''
            for row in csv_reader:
                stop_id = self._parse_stop_id(row)
                route_id = row['route_id']
                this_stop = stop_id + route_id
                if this_stop not in list_of_nodes:
                    list_of_nodes.append(this_stop)
                    route = self.routes[route_id]
                    _stop_networks[route_id].node(
                        this_stop,
                        label=self.stops_info[stop_id].name,
                        style='filled',
                        fontsize='14',
                        fillcolor=route.color,
                        fontcolor=route.text_color
                    )

                this_shape = row['shape_id']
                if this_shape == prev_shape:
                    edge_str = this_stop+' > '+prev_stop
                    if edge_str not in list_of_edge_str:
                        list_of_edge_str.append(this_stop+' > '+prev_stop)
                        list_of_edge_str.append(prev_stop+' > '+this_stop)
                        _stop_networks[route_id].edge(prev_stop, this_stop)
                prev_stop = this_stop
                prev_shape = this_shape

        format_ = 'pdf'
        outfile_ = f'{self.name}_routes_graph'
        print(f'\nWriting network graph to {outfile_}.{format_}')
        for route_id, route in self.routes.items():
            _r.subgraph(_stop_networks[route_id])
        graph_dir = '/var/www/html/route_viz'
        _r.render(filename=outfile_, directory=graph_dir, cleanup=True, format=format_)

    def __init__(self, name, gtfs_settings):
        super().__init__(name, gtfs_settings)


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''
' '     The following classes describe REALTIME components of the system:
' '     Train, Feed
'''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


class Train:
    """Analogous to GTFS trip_id

    next_stop = analogous to GTFS trip_update.stop_time_update[0].stop_id
    next_stop_arrival = analogous to GTFS trip_update.stop_time_update[0].arrival
    """
    next_stop = None
    next_stop_arrival = None

    def __init__(self, trip_id, route_id):# branch_id = None):
        self.id_ = trip_id
        self.route = route_id
        #self.branch = branch_id


class Feed:
    """Gets a new realtime GTFS feed
    """
    def get_feed(self):
        response = requests.get(self.gtfs_feed_url)
        gtfs_feed = gtfs_realtime_pb2.FeedMessage()
        gtfs_feed.ParseFromString(response.content)
        return gtfs_feed

    def trains_by_route(self, route):
        #n_bound_trains = []
        for entity in self.feed.entity:
            if entity.HasField('vehicle'):
                if route is entity.vehicle.trip.route_id:
                    _status = STATUS_MESSAGES[entity.vehicle.current_status]
                    _name = self.transit_system.stops_info[entity.vehicle.stop_id].name
                    print(f'Train is {_status} {_name}')

    def interate_stop_time_update_arrivals(self, entity):
        arrivals = []
        for stop_time_update in entity.trip_update.stop_time_update:
            if stop_time_update.stop_id == stop:
                if stop_time_update.arrival.time > time.time():
                    arrivals.append(stop_time_update.arrival.time)
        return arrivals

    def next_arrivals(self, train, stop):
        for entity in self.feed.entity:
            if entity.HasField('trip_update'):
                if entity.trip_update.trip.route_id == train:
                    return self.interate_stop_time_update_arrivals(entity)


    def timestamp(self):
        return self.feed.header.timestamp

    def feed_size(self):
        return len(str(self.feed))

    def print_feed(self):
        print(self.feed)

    def __init__(self, route_id, transit_system):
        self.transit_system = transit_system
        self.feed_id = transit_system.gtfs_settings.which_feed[route_id]
        self.gtfs_feed_url = transit_system.gtfs_settings.gtfs_feed_url(route_id)
        self.feed = self.get_feed()


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

def main():
    '''
    mta =  MTASubway('MTASubway')
    mta.build()
    mta.store('stored_transit_systems/MTA.pkl')
    new_boi = mta.load('stored_transit_systems/MTA.pkl')
    new_boi.display()
    '''
    mta = ts.MTASubway('MTASubway')
    mta.build()
    mta.display()
    #mta.map_each_route()


if __name__ == "__main__":
    main()


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
