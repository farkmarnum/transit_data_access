#!/usr/bin/python3
"""Classes and methods for static GTFS data
"""
import logging
import os
import shutil
#import types
import time
import csv
import zipfile
from collections import defaultdict
import json
import filecmp
import requests
#import graphviz
import pandas as pd
import datetime
import pprint as pp

from ts_config import MTA_SETTINGS, LIST_OF_FILES
from misc import NestedDict, trip_to_shape


class StaticHandler:
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


    def _load_time_between_stops(self): # TODO NOT WORKINGS
        """Parses stop_times.csv to populate self.static_data['stops'] with time between stops info
        """
        logging.debug("Loading stop info and time between stops")
        with open(self._locate_csv('stop_times'), 'r') as stop_times_infile:
            stop_times_csv = csv.DictReader(stop_times_infile)
            branch_id = None
            prev_row = {'trip_id': None}
            for row in stop_times_csv:

                if prev_row['trip_id'] == row['trip_id']:
                    arrival = row['arrival_time'].split(':')
                    departure = prev_row['departure_time'].split(':')
                    arrival = datetime.timedelta(hours=int(arrival[0]), minutes=int(arrival[1]), seconds=int(arrival[2]))
                    departure = datetime.timedelta(hours=int(departure[0]), minutes=int(departure[1]), seconds=int(departure[2]))
                    travel_time = int((arrival-departure).total_seconds())

                    if branch_id:
                        stop_id = row['stop_id']
                        self.static_data['stops'][stop_id]['travel_time'][branch_id] = travel_time
                    else:
                        logging.debug('no existing branch_id found for %s',row['trip_id'])

                else:
                    shape_id = trip_to_shape(row['trip_id'])
                    route_id = shape_id.split('.')[0]

                    try:
                        branch_id = self.static_data['routes'][route_id]['shape_to_branch'][shape_id]
                    except KeyError:
                        logging.debug('shape_id %s not found in static_data[\'routes\'][\'%s\'][\'shape_to_branch\']', shape_id, route_id)
                        branch_id = None

                prev_row = row



    def has_static_data(self):
        """Checks if static_data_path is populated with the csv files in LIST_OF_FILES
        Returns a bool
        """
        all_files_are_loaded = True
        for file_ in LIST_OF_FILES:
            all_files_are_loaded *= os.path.isfile(self._locate_csv(file_))
        return all_files_are_loaded

    def update_(self):
        """Downloads new static GTFS data, checks if different than existing data,
        unzips, and then generates additional csv files:

        _download_new_schedule_data() downloads the zip to {data_path}/tmp/static_data.zip
        _unzip_new_schedule_data() unzips it to {data_path}/tmp/static_data
            if successful, it then deletes {data_path}/static/GTFS/ and moves the new data there
        _merge_trips_and_stops combines trips, stops, and stop_times to make route_stops_with_names
        _load_time_between_stops calculates time b/w each pair of adjacent stops using stop_times
        """
        url = self.gtfs_settings.static_url
        end_path = self.gtfs_settings.static_data_path
        tmp_path = self.gtfs_settings.static_tmp_path
        zip_path = tmp_path+'/static_data.zip'

        os.makedirs(tmp_path, exist_ok=True)
        os.makedirs(end_path, exist_ok=True)

        logging.debug('Downloading GTFS static data from %s', url)
        try:
            _new_data = requests.get(url, allow_redirects=True)
        except requests.exceptions.ConnectionError:
            exit(f'\nERROR:\nFailed to connect to {url}\n')

        open(zip_path, 'wb').write(_new_data.content)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmp_path)

        logging.debug('removing %s', zip_path)
        os.remove(zip_path)

        if self.has_static_data():
            if not filecmp.dircmp(end_path, tmp_path).diff_files:
                logging.debug('No difference found b/w existing static/GTFS/raw and new downloaded data')
                logging.debug('Deleting %s and exiting update_ts() early', tmp_path)
                shutil.rmtree(tmp_path)
                return False

        logging.debug('Deleting %s to make room for new static data', end_path)
        shutil.rmtree(end_path)

        logging.debug('Moving new data from %s to %s', tmp_path, end_path)
        os.rename(tmp_path, end_path)


        self._merge_trips_and_stops()
        return True

    def to_json(self, attempt=0):
        json_path = self.gtfs_settings.static_json_path

        try:
            with open(json_path+'/static.json', 'w') as out_file:
                json.dump(self.static_data, out_file)

        except OSError:
            if attempt != 0:
                exit(f'Unable to write to {json_path}/static.json')
            print(f'{json_path}/static.json does not exist, attempting to create it')

            try:
                os.makedirs(json_path)
            except PermissionError:
                exit(f'ERROR: Do not have permission to write to {json_path}/static.json')
            except FileExistsError:
                exit(f'ERROR: The file {json_path}/static.json exists, no permission to overwrite')

            self.to_json(attempt=attempt+1)


    def build(self, force=False):
        """Builds JSON from the GTFS (with improvements)

        Generates the routes>shapes>stops|branches>stops tree
        Merges shapes into branches
        """
        json_path = self.gtfs_settings.static_json_path
        if os.path.isfile(json_path) and not force:
            logging.debug('%s already exists, build() is unneccessary', json_path)
            logging.debug('use force=True to force a build')
            return

        # Initialize
        ts = {
            'name': self.name,
            'routes': {},
            'stops': NestedDict()
        }
        # Load info for each stop (not including parent stops)
        with open(self._locate_csv('stops'), mode='r') as stops_file:
            stops_csv_reader = csv.DictReader(stops_file)
            for row in stops_csv_reader:
                if row['parent_station']:
                    ts['stops'][row['stop_id']] = {
                        'info': {
                            'name': row['stop_name'],
                            'lat': row['stop_lat'],
                            'lon': row['stop_lon'],
                            'parent_station': row['parent_station'],
                            'direction': row['stop_id'][-1]
                        },
                        'arrivals': NestedDict(),
                        'travel_time': NestedDict()
                    }

        # Load info for each route
        with open(self._locate_csv('routes'), mode='r') as route_file:
            route_csv_reader = csv.DictReader(route_file)
            for row in route_csv_reader:
                if row['route_color'].strip():
                    color = row['route_color']
                else:
                    color = 'lightgrey'

                if row['route_text_color'].strip():
                    text_color = row['route_text_color']
                else:
                    text_color = 'black'

                ts['routes'][row['route_id']] = {
                    'desc': row['route_desc'],
                    'color': color,
                    'text_color': text_color,
                    'shapes': defaultdict(list),
                    'branches': defaultdict(list),
                    'shape_to_branch': {}
                }

        # Load route>shape>stop tree
        with open(self._locate_csv('route_stops_with_names'), mode='r') as rswn_file:
            rwsn_csv_reader = csv.DictReader(rswn_file)
            for row in rwsn_csv_reader:
                route = ts['routes'][row['route_id']]
                route['shapes'][row['shape_id']].append(row['stop_id'])

        # Merge shapes into branches
        for route_id, route in ts['routes'].items():
            br_count = {}
            br_count['N'] = br_count['S'] = 0 # how many branches in each direction

            for shape_id, shape_stop_list in route['shapes'].items():
                for branch_id, branch_stop_list in route['branches'].items():

                    if set(shape_stop_list).issubset(set(branch_stop_list)):
                        route['shape_to_branch'][shape_id] = branch_id
                        break

                    if set(branch_stop_list).issubset(set(shape_stop_list)):
                        route['shape_to_branch'][shape_id] = branch_id
                        route['branches'][branch_id] = shape_stop_list
                        break

                else: # no branches contained this shape (or were contained in it)
                    for i in range(len(shape_id)):
                        if shape_id[i-1] == '.' and shape_id[i] != '.':  # the N or S follows the dot(s)
                            direction = shape_id[i]
                            break
                    else:
                        raise Exception(f'Error: cannot determine direction from shape id {shape_id}')
                    new_branch_id = f'{route_id}{direction}_{br_count[direction]}'

                    route['branches'][new_branch_id] = shape_stop_list
                    route['shape_to_branch'][shape_id] = new_branch_id
                    br_count[direction] += 1

        ts['last_updated'] = int(time.time())

        self.static_data = ts
        self._load_time_between_stops()
        self.to_json()
        logging.debug("New static build written to JSON")



    def __init__(self, gtfs_settings, name=''):
        self.gtfs_settings = gtfs_settings
        self.name = name
        self.static_data = None


def main():
    """Creates new StaticHandler, which calls update_() and build()
    """
    #time_before = time.time()
    static_handler = StaticHandler(MTA_SETTINGS, name='MTA')
    static_handler.update_()
    static_handler.build(force=True)
    #time_after = time.time()
    #print(f'static.py took {time_after-time_before} seconds')


if __name__ == '__main__':
    main()
