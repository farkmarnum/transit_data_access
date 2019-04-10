"""Classes and methods for static GTFS data
"""
from profilehooks import profile
import logging
import os
import sys
import shutil
import time
import csv
import zipfile
from collections import defaultdict
import json
import eventlet
import filecmp
import datetime
import requests
import pandas as pd

import misc
from transit_system_config import MTA_SETTINGS, LIST_OF_FILES

parser_logger = misc.parser_logger

eventlet.monkey_patch()

class StaticHandler:
    """Construction functions for TransitSystem"""

    _has_parent_station_column = True
    _rswn_list_of_columns = [
        'route_id',
        'shape_id',
        'stop_sequence',
        'stop_id',
        'stop_name',
        'parent_station',
        #'stop_lat',
        #'stop_lon',
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
        """Combines trips.csv stops.csv and stop_times.csv into _locate_csv('route_stops_with_names')
        Keeps the columns in _rswn_list_of_columns

        Also, compiles a csv of truncated_trip_id, shape_id and writes it to _locate_csv('trip_id_to_shape')
        """
        parser_logger.info("Cross referencing route, stop, and trip information...")

        trips_csv = self._locate_csv('trips')
        stops_csv = self._locate_csv('stops')
        stop_times_csv = self._locate_csv('stop_times')

        trips = pd.read_csv(trips_csv, dtype=str)
        stops = pd.read_csv(stops_csv, dtype=str)
        stop_times = pd.read_csv(stop_times_csv, dtype=str)

        stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(int)

        parser_logger.info("Loaded trips, stops, and stop_times into DataFrames")

        x_to_r = lambda str_: str_.replace(r'(.*?_.*?_.*\..*?)X.*', r'\1R', regex=True) # convert the weird shape_ids with 'X' in them to be <everything up to the X> + 'R'
        slice_shape_from_trip = lambda str_: str_.replace(r'.*_(.*?$)', r'\1', regex=True) # slice what's after the last '_'

        trips['shape_id'] = trips['shape_id'].fillna(
            slice_shape_from_trip(
                x_to_r(
                    trips['trip_id'].str
                )
            )
        ) # for any rows with empty 'shape_id', take the trip_id value, fix the 'X' if necessary, then slice to get shape_id

        composite = pd.merge(trips, stop_times, how='inner', on='trip_id')
        composite = pd.merge(composite, stops, how='inner', on='stop_id')
        composite = composite[self._rswn_list_of_columns]
        composite = composite.drop_duplicates().sort_values(by=self._rswn_sort_by)

        rswn_csv = self._locate_csv('route_stops_with_names')
        composite.to_csv(rswn_csv, index=False)
        parser_logger.info('%s created', rswn_csv)

        trip_id_to_shape = trips[['trip_id', 'shape_id']].copy()

        _, trip_id_to_shape['trip_start_time'], trip_id_to_shape['trip_shape_trunc'] = trip_id_to_shape['trip_id'].str.split('_').str

        trip_id_to_shape['trip_shape_trunc'] = trip_id_to_shape['trip_shape_trunc'].replace(r'(.*\.[NS]).*$', r'\1', regex=True)
        trip_id_to_shape = trip_id_to_shape[['trip_shape_trunc','trip_start_time','shape_id']].drop_duplicates()
        trip_id_to_shape = trip_id_to_shape.sort_values(['trip_shape_trunc','trip_start_time'])

        dict_ = defaultdict(dict)
        for _, row in trip_id_to_shape.iterrows():
            dict_[row.trip_shape_trunc][int(row.trip_start_time)] = row.shape_id


        tits_fname = f'{self.gtfs_settings.static_data_path}/trip_id_to_shape.json' # get your mind out of the gutter! 'tits' is clearly an abbreviation for trip_id_to_shape
        with open(tits_fname, 'w') as tits_file:
            json.dump(dict_, tits_file)

        parser_logger.info('%s created', tits_fname)

    def has_static_data(self):
        """Checks if static_data_path is populated with the csv files in LIST_OF_FILES
        Returns a bool
        """
        all_files_are_loaded = True
        for file_ in LIST_OF_FILES:
            all_files_are_loaded *= os.path.isfile(f'{self.gtfs_settings.static_data_path}/{file_}')
        return all_files_are_loaded

    def update_(self, force=False):
        """Downloads new static GTFS data, checks if different than existing data,
        unzips, and then generates additional csv files:

        downloads the zip to {data_path}/tmp/static_data.zip
        unzips it to {data_path}/tmp/static_data
            if successful, it then deletes {data_path}/static/GTFS/ and moves the new data there
        _merge_trips_and_stops combines trips, stops, and stop_times to make route_stops_with_names
        _load_time_between_stops calculates time b/w each pair of adjacent stops using stop_times
        """
        url = self.gtfs_settings.static_url
        end_path = self.gtfs_settings.static_data_path
        tmp_path = self.gtfs_settings.static_tmp_path
        zip_path = tmp_path+'/static_data.zip'


        try:
            os.makedirs(tmp_path, exist_ok=True)
            os.makedirs(end_path, exist_ok=True)
        except PermissionError:
            parser_logger.error('Don\'t have permission to write to %s or %s', end_path, tmp_path)
            exit()

        parser_logger.info('Downloading GTFS static data from %s to %s', url, zip_path)
        try:
            _new_data = requests.get(url, allow_redirects=True, timeout=5)
        except requests.exceptions.RequestException as err:
            parser_logger.error('%s: Failed to connect to %s\n', err, url)
            if self.has_static_data():
                print('Can\'t connect to', url, 'but do have existing static data (err=',err)
                return False
            else:
                print('Can\'t connect to', url, 'and don\'t have existing static data (err=',err)
                return False


        with open(zip_path, 'wb') as zip_outfile:
            zip_outfile.write(_new_data.content)

        parser_logger.info('Extracting zip to %s', tmp_path)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmp_path)

        parser_logger.info('removing %s', zip_path)
        os.remove(zip_path)

        if self.has_static_data() and not force:
            if not filecmp.dircmp(end_path, tmp_path).diff_files:
                parser_logger.info('No difference found b/w existing static/GTFS/raw and new downloaded data')
                parser_logger.info('Deleting %s and exiting update_ts() early', tmp_path)
                parser_logger.info('use force=True to force an update')
                shutil.rmtree(tmp_path)
                return False

        parser_logger.info('Deleting %s to make room for new static data', end_path)
        shutil.rmtree(end_path)

        parser_logger.info('Moving new data from %s to %s', tmp_path, end_path)
        os.rename(tmp_path, end_path)

        self._merge_trips_and_stops()
        return True

    def to_json(self, attempt=0):
        """ Stores self.static_data in self.gtfs_settings.static_json_path+'/static.json'
        """
        json_path = self.gtfs_settings.static_json_path

        try:
            with open(json_path+'/static.json', 'w') as out_file:
                json.dump(self.static_data, out_file)

        except OSError:
            if attempt != 0:
                parser_logger.error('Unable to write to %s/static.json', json_path)
                exit()
            parser_logger.info('%s/static.json does not exist, attempting to create it', json_path)

            try:
                os.makedirs(json_path)
            except PermissionError:
                parser_logger.error('Don\'t have permission to write to %s/static.json', json_path)
                exit()
            except FileExistsError:
                parser_logger.error('The file %s/static.json exists, no permission to overwrite', json_path)
                exit()

            self.to_json(attempt=attempt+1)

    def _load_time_between_stops(self):
        """Parses stop_times.csv to populate self.static_data['stops'] with time between stops info
        """
        parser_logger.info("Loading stop info and time between stops")
        branch_id = departure = arrival = prev_trip = None
        seen = defaultdict(dict)

        with open(self._locate_csv('stop_times'), 'r') as stop_times_infile:
            stop_times_csv = csv.DictReader(stop_times_infile)
            for row in stop_times_csv:
                trip_id = row['trip_id']
                shape_id = misc.trip_to_shape(trip_id)
                stop_id = row['stop_id']
                if not shape_id in seen or not stop_id in seen[shape_id]:
                    seen[shape_id][stop_id] = True
                else:
                    continue


                arrival = row['arrival_time'].split(':')
                arrival = datetime.timedelta(hours=int(arrival[0]), minutes=int(arrival[1]), seconds=int(arrival[2]))

                if prev_trip == trip_id:
                    route_id = shape_id.split('.')[0]
                    travel_time = int((arrival-departure).total_seconds())
                    if branch_id:
                        self.static_data['stops'][stop_id]['travel_time'][route_id][branch_id] = travel_time

                else:
                    try:
                        branch_id = self.static_data['shape_to_branch'][shape_id]
                    except KeyError:
                        parser_logger.warning('In _load_time_between_stops(), branch_id %s NOT FOUND', branch_id)
                        branch_id = None

                prev_trip = trip_id
                departure = arrival

    def build(self, force=False):
        """Builds JSON from the GTFS (with improvements)
        """
        json_path = self.gtfs_settings.static_json_path
        if os.path.isfile(f'{json_path}/static.json') and not force:
            parser_logger.info('%s already exists, build() is unneccessary', json_path)
            parser_logger.info('use force=True to force a build')
            return

        # Initialize
        transit_system = {
            'name': self.name,
            'routes': {},
            'stops': misc.NestedDict(),
            'shape_to_branch': {}
        }
        # Load info for each stop (not including parent stops)
        with open(self._locate_csv('stops'), mode='r') as stops_file:
            stops_csv_reader = csv.DictReader(stops_file)
            for row in stops_csv_reader:
                #if row['parent_station']:
                transit_system['stops'][row['stop_id']] = {
                    'info': {
                        'name': row['stop_name'],
                        'lat': row['stop_lat'],
                        'lon': row['stop_lon'],
                        'parent_station': row['parent_station'],
                        'direction': row['stop_id'][-1]
                    },
                    'travel_time': misc.NestedDict()
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

                transit_system['routes'][row['route_id']] = {
                    'desc': row['route_desc'],
                    'color': color,
                    'text_color': text_color,
                    'shapes': defaultdict(list),
                    'branches': {
                        'N': defaultdict(list),
                        'S': defaultdict(list)
                    }
                }

        # Load route>shape>stop tree
        with open(self._locate_csv('route_stops_with_names'), mode='r') as rswn_file:
            rwsn_csv_reader = csv.DictReader(rswn_file)
            for row in rwsn_csv_reader:
                route = transit_system['routes'][row['route_id']]
                route['shapes'][row['shape_id']].append(row['stop_id'])

        # Merge shapes into branches
        for route_id, route in transit_system['routes'].items():
            br_count = {}
            br_count['N'] = br_count['S'] = 0 # how many branches in each direction

            for shape_id, shape_stop_list in route['shapes'].items():
                for i in range(len(shape_id)):
                    if shape_id[i-1] == '.' and shape_id[i] != '.':  # the N or S follows the dot(s)
                        direction = shape_id[i]
                        break
                else:
                    raise Exception(f'Error: cannot determine direction from shape id {shape_id}')

                for branch_id, branch_stop_list in route['branches'][direction].items():

                    if set(shape_stop_list).issubset(set(branch_stop_list)):
                        transit_system['shape_to_branch'][shape_id] = branch_id
                        break

                    if set(branch_stop_list).issubset(set(shape_stop_list)):
                        transit_system['shape_to_branch'][shape_id] = branch_id
                        break

                else: # no branches contained this shape (or were contained in it)
                    new_branch_id = f'{route_id}{direction}_{br_count[direction]}'

                    route['branches'][direction][new_branch_id] = shape_stop_list
                    transit_system['shape_to_branch'][shape_id] = new_branch_id
                    br_count[direction] += 1

        transit_system['static_timestamp'] = int(time.time())

        self.static_data = transit_system
        self._load_time_between_stops()
        self.to_json()
        parser_logger.info("New static build written to JSON")


    def __init__(self, gtfs_settings, name=''):
        self.gtfs_settings = gtfs_settings
        self.name = name
        self.static_data = None


def main(force=False):
    """Creates new StaticHandler, then calls update_() and build()
    """
    with misc.TimeLogger('static.py') as _tl:
        static_handler = StaticHandler(MTA_SETTINGS, name='MTA')
        static_handler.update_(force)
        static_handler.build(force)


if __name__ == '__main__':
    try:
        assert(sys.argv[1] in ('force'))
    except AssertionError:
        print('Usage: static.py force or static.py')
    except IndexError:
        main()
    else:
        main(force=True)
