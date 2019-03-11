#!/usr/bin/python3
"""Classes and methods for static GTFS data
"""
import os
import shutil
import types
import time
import csv
import zipfile
import logging
#import sys
#import pickle
import requests
import graphviz
import pandas as pd

LIST_OF_FILES = [
    'agency.txt',
    'calendar_dates.txt',
    'calendar.txt',
    'route_stops_with_names.txt',
    'routes.txt',
    'shapes.txt',
    'stop_times.txt',
    'stops.txt',
    'transfers.txt',
    'trips.txt'
]

STATUS_MESSAGES = ['approaching', 'stopped at', 'in transit to']

def _locate_csv(name, gtfs_settings):
    """Generates the path/filname for the csv given the name & gtfs_settings
    """
    return '%s/%s.txt' % (gtfs_settings.static_data_path, name)


class Stop:
    """Analogous to GTFS 'stop_id'.

    Usage: Stop(stop_id)
    id = stop_id, '201S' for example
    """
    #id_ = None

    def display(self):
        """Prints self._id"""
        print('            '+self.id_)

    def __init__(self, stop_id):
        self.id_ = stop_id
        self.upcoming_trains = {}
        self.routes_that_stop_here = {}

class Shape:
    """Analogous to GTFS 'shape_id'.

    Usage: Shape(parent_route, shape_id)
    id = shape_id, '2..S08R' for example
    """
    #id_ = stops = None
    def display(self):
        """Prints self._id and then calls display() for each Stop in self.stops"""
        print('        '+self.id_)
        for _, stop in self.stops.items():
            stop.display()

    def add_stop(self, stop_id):
        """Creates a new Stop object
        Then, adds it (w/ stop_id) as a new entry in the self.stops dict.
        """
        self.stops.update({stop_id:Stop(stop_id)})

    def __init__(self, parent_route, shape_id):
        self.route = parent_route
        self.id_ = shape_id
        self.stops = {}


class Route:
    """Analogous to GTFS 'route_id'.
    """
    #shapes = branches = route_info = None

    def display(self):
        """Prints self.route_info['id_'] and then calls display() for each Shape in self.shapes"""
        print('    '+self.route_info['id_'])
        for _, shape in self.shapes.items():
            shape.display()

    def add_shape(self, shape_id):
        """Creates a new Shape object
        Then, adds it (w/ shape_id) as a new entry in the self.shapes dict.
        """
        self.shapes.update({shape_id:Shape(self, shape_id)})

    def __init__(self, transit_system, route_info):
        self.transit_system = transit_system
        self.route_info = route_info
        self.shapes = {}
        self.branches = {}


class TransitSystem:
    """Construction functions for TransitSystem"""
    #routes = gtfs_settings = stops_info = routes_info = None

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
        """ Converts a stop_id like '123N' to its parent station '123' if there is one
        """
        if self._has_parent_station_column:
            if row['parent_station'] is not None:
                return row['parent_station']
        return row['stop_id']

    def _merge_trips_and_stops(self):
        """Combines trips.csv stops.csv and stop_times.csv into one csv
        Keeps the columns in _rswn_list_of_columns
        """
        logging.debug("Cross referencing route, stop, and trip information...")

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
        """Parses stop_times.csv and generates a csv of time_between_stops
        """
        # TODO
        logging.debug("Calculating travel time between each stop...")

    def _download_new_schedule_data(self, url, to_):
        """Gets static GTFS data using requests.get()
        """
        if not os.path.exists(to_):
            os.makedirs(to_)
            logging.debug('Creating %s', to_)

        logging.debug('Downloading GTFS static data from %s', url)
        try:
            _new_data = requests.get(url, allow_redirects=True)
            open(f'{to_}/schedule_data.zip', 'wb').write(_new_data.content)
        except requests.exceptions.ConnectionError:
            exit(f'\nERROR:\nFailed to connect to {url}\n')

    def _unzip_new_schedule_data(self, temp_dir, to_):
        """Unzips a GTFS zip to to_
        """
        _old_data = None
        if os.path.exists(to_):
            _old_data = f'{to_}_OLD'
            logging.debug('Moving %s to %s', to_, _old_data)
            os.rename(to_, _old_data)

        os.makedirs(to_)

        logging.debug('Unzipping schedule_data.zip to %s', to_)

        with zipfile.ZipFile(f'{temp_dir}/schedule_data.zip', "r") as zip_ref:
            zip_ref.extractall(to_)

        logging.debug('Deleting %s', temp_dir)
        shutil.rmtree(temp_dir)
        if _old_data:
            logging.debug('Deleting %s', _old_data)
            shutil.rmtree(_old_data)

    def update_ts(self):
        """Downloads new static GTFS data, unzips, and generates additional csv files
        """
        url = self.gtfs_settings.gtfs_static_url
        path = self.gtfs_settings.static_data_path

        if not os.path.exists('/tmp'):
            os.mkdir('/tmp')
        temp_dir = f'/tmp/gtfs_parser-{int(time.time()*10)}'

        self._download_new_schedule_data(url=url, to_=temp_dir)
        self._unzip_new_schedule_data(temp_dir=temp_dir, to_=path)
        self._merge_trips_and_stops()
        self._load_travel_time_for_stops()

    def is_loaded(self):
        """Checks if static_data_path is populated with the csv files in LIST_OF_FILES
        Returns a bool
        """
        all_files_are_loaded = True
        for file_ in LIST_OF_FILES:
            file_full = f'{self.gtfs_settings.static_data_path}/{file_}'
            all_files_are_loaded *= os.path.isfile(file_full)
        return all_files_are_loaded

    def _load_stops_info(self):
        """Loads info about each stop from stops.csv into self.stops_info
        """
        with open(_locate_csv('stops', self.gtfs_settings), mode='r') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                _stop_name = types.SimpleNamespace(
                    name=row['stop_name'],
                    lat=row['stop_lat'],
                    lon=row['stop_lon']
                )
                self.stops_info[row['stop_id']] = _stop_name

    def _add_route(self, route_info):
        """Creates a new Route object
        Then, adds it (w/ route_id) as a new entry in the self.routes dict.
        """
        route_id = route_info['id_']
        logging.debug('Adding route: %s', route_id)
        self.routes.update({route_id:Route(self, route_info)})

    def _load_all_routes(self):
        """Parses route.csv and creates a new Route for each route_id
        Also, populates route_info with information for each route_id
        """
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

                self._add_route(route_info)

    def _build_routes_shapes_stops(self):
        """For each route, creates Shape objects for each shape, and Stop objects for each of those
        """
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
                    route = self.routes[current_route_id]

    def _build_branches(self):
        """Analyzes all the Shapes in each Route and generates the Branches
        """

    def build(self):
        """Loads stop_info and route_info, generates the Routes>Shapes>Stops|Branches>Stops tree
        """
        logging.debug("Loading stop info...")
        self._load_stops_info()
        logging.debug("Done.\nLoading Routes and route info...")
        self._load_all_routes()
        logging.debug("Done.\nLoading GTFS static data into Route Shape and Stop objects...")
        self._build_routes_shapes_stops()
        logging.debug("Done.\nGenerating Branches...")
        self._build_branches()

    def display(self):
        """Prints self.name and then calls display() for each Route in self.routes
        """
        print(self.name)
        for _, route in self.routes.items():
            route.display()

    def map_each_route(self, route_desc_filter=None):
        """ Maps each route's stop network using graphviz
        """
        _stop_networks = {}
        _r = graphviz.Graph()
        logging.debug('Mapping the network of stops for: ')
        for route_id, route in self.routes.items():
            logging.debug(route_id)
            if route_desc_filter is None or route.route_info.desc in route_desc_filter:
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
                        fillcolor=route.route_info.color,
                        fontcolor=route.route_info.text_color
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
        outfile = f'{self.name}_routes_graph'
        logging.debug('\nWriting network graph to %s.%s', outfile, format_)
        for route_id, route in self.routes.items():
            _r.subgraph(_stop_networks[route_id])
        graph_dir = '/var/www/html/route_viz'
        _r.render(filename=outfile, directory=graph_dir, cleanup=True, format=format_)


    def __init__(self, name, gtfs_settings):
        self.name = name
        self.gtfs_settings = gtfs_settings
        self.routes = {}
        self.stops_info = {}
        self.routes_info = {}