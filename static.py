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
from collections import defaultdict
import types
#import sys
#import pickle
import requests
import graphviz
import pandas as pd

LIST_OF_FILES = [
    'agency',
    'calendar_dates',
    'calendar',
    'route_stops_with_names',
    'routes',
    'shapes',
    'stop_times',
    'stops',
    'transfers',
    'trips'
]

STATUS_MESSAGES = ['approaching', 'stopped at', 'in transit to']

def display_ts(ts, route_id_only):
    """Loops through all routes>shapes>stops to print full structure
    """
    print(ts.name)
    for route_id, route in ts.routes.items():
        if route_id == route_id_only:
            print(route_id)
            #print('SHAPES:')
            #for shape_id, shape in route.shapes.items():
            #    print('  '+shape_id)
            #    for stop_id in shape:
            #        print(f'    {stop_id}')
            print('BRANCHES:')
            for branch_id, branch in route.branches.items():
                print('  '+branch_id)
                for stop_id in branch:
                    print(f'    {stop_id}')

class StaticLoader:
    """Construction functions for TransitSystem"""

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

    def _locate_csv(self, name):
        """Generates the path/filname for the csv given the name & gtfs_settings
        """
        return '%s/%s.txt' % (self.gtfs_settings.static_data_path, name)

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

        trips_csv = self._locate_csv('trips')
        stops_csv = self._locate_csv('stops')
        stop_times_csv = self._locate_csv('stop_times')

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

        rswn_csv = self._locate_csv('route_stops_with_names')
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

    def has_static_data(self):
        """Checks if static_data_path is populated with the csv files in LIST_OF_FILES
        Returns a bool
        """
        all_files_are_loaded = True
        for file_ in LIST_OF_FILES:
            all_files_are_loaded *= os.path.isfile(self._locate_csv(file_))
        return all_files_are_loaded


    def build_ts(self):
        """Generates the Routes>Shapes>Stops|Branches>Stops tree and returns it
        """
        ts = types.SimpleNamespace(
            name = self.name,
            routes = {}
        )

        with open(self._locate_csv('routes'), mode='r') as route_file:
            route_csv_reader = csv.DictReader(route_file)
            for row in route_csv_reader:
                ts.routes[row['route_id']] = types.SimpleNamespace(
                    desc=row['route_desc'],
                    color=row['route_color'],
                    text_color=row['route_text_color'],
                    shapes = defaultdict(list),
                    branches = defaultdict(list),
                    all_stops = {},
                    shape_to_branch = {}
                )

        with open(self._locate_csv('route_stops_with_names'), mode='r') as rswn_file:
            rwsn_csv_reader = csv.DictReader(rswn_file)
            for row in rwsn_csv_reader:
                ts.routes[row['route_id']].shapes[row['shape_id']].append(row['stop_id'])

        for route_id, route in ts.routes.items():
            br_count = {}
            br_count['N'] = br_count['S'] = 0 # how many branches in each direction

            for shape_id, shape_stop_list in route.shapes.items():
                for branch_id, branch_stop_list in route.branches.items():

                    if set(shape_stop_list).issubset(set(branch_stop_list)):
                        route.shape_to_branch[shape_id] = branch_id
                        break

                    if set(branch_stop_list).issubset(set(shape_stop_list)):
                        route.shape_to_branch[shape_id] = branch_id
                        route.branches[branch_id] = shape_stop_list
                        break

                else: # no branches contained this shape (or were contained in it)
                    for i in range(len(shape_id)):
                        if shape_id[i-1] == '.' and shape_id[i] != '.':  # the N or S follows the dot(s)
                            direction = shape_id[i]
                            break
                    else:
                        raise Exception(f'Error: cannot determine direction from shape id {shape_id}')
                    new_branch_id = f'{route_id}{direction}_{br_count[direction]}'

                    route.branches[new_branch_id] = shape_stop_list
                    route.shape_to_branch[shape_id] = new_branch_id
                    br_count[direction] += 1

        '''
        self.ts = SimpleNamespace(
            name = 'MTA',
            last_updated = '359242398419',
            routes = {
                '1': SimpleNamespace(
                    desc = 'this is route 1',
                    color = 'red',
                    text_color = 'black',
                    all_stops = {'123', '124', '125'},
                    shapes = {
                        '1..S03R': {
                            '124S',
                            '123S'
                        },
                        '1..S08R': {'124S', '123S'},
                        '1..S04R': {'125S', '124S', '123S'},
                        '1..N03R': {1:'123N',2:'124N'},
                        '1..N08R': {1:'123N',2:'124N'},
                        '1..N04R': {1:'123N',2:'124N',3:'125N'}
                    },
                    shape_to_branch = {
                        '1..S03R': '1S_0',
                        '1..S08R': '1S_0',
                        '1..S04R': '1S_0',
                        '1..N03R': '1N_0',
                        '1..N08R': '1N_0',
                        '1..N04R': '1N_0'
                    },
                    branches = {
                        '1N_0': {'123N', '124N', '125N'},
                        '1S_0': {'125S', '124S', '123S'}
                    }
                )
            }
        )
        '''
        ts.last_updated = int(time.time())
        return ts

    '''
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
        with open(self._locate_csv('route_stops_with_names'), mode='r') as infile:
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
    '''

    def __init__(self, name, gtfs_settings):
        self.gtfs_settings = gtfs_settings
        self.name = name
        #self.routes = {}
        #self.stops_info = {}
        #self.routes_info = {}
        self.ts = None









#EOF
