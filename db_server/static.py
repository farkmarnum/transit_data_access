"""Classes and methods for static GTFS data
"""
import csv
import datetime
from collections import defaultdict
import eventlet
import filecmp
import json
import logging
import os
import pandas as pd
import requests
import shutil
import sys
import time
import zipfile

import misc
from transit_system_config import MTA_SETTINGS, LIST_OF_FILES

parser_logger = misc.parser_logger

eventlet.monkey_patch()

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

class StaticHandler:
    def __init__(self, gtfs_settings):
        self.gtfs_settings = gtfs_settings
        self.name = gtfs_settings.ts_name
        self.static_data = {}
        self.rswn_columns = [
            'route_id',
            'shape_id',
            'stop_sequence',
            'stop_id',
        ]
        self.rswn_sort_by = [
            'route_id',
            'shape_id',
            'stop_sequence'
        ]

    def locate_csv(self, name):
        """Generates the path/filname for the csv given the name & gtfs_settings
        """
        return '%s/%s.txt' % (self.gtfs_settings.static_data_path, name)

    def has_static_data(self):
        """Checks if static_data_path is populated with the csv files in LIST_OF_FILES
        Returns a bool
        """
        all_files_are_loaded = True
        for file_ in LIST_OF_FILES:
            all_files_are_loaded *= os.path.isfile(f'{self.gtfs_settings.static_data_path}/{file_}')
        return all_files_are_loaded


    def merge_trips_and_stops(self):
        """Combines trips.csv stops.csv and stop_times.csv into locate_csv('route_stops_with_names')
        Keeps the columns in rswn_columns

        Also, compiles a csv of truncated_trip_id, shape_id and writes it to locate_csv('trip_id_to_shape')
        """
        parser_logger.info("Cross referencing route, stop, and trip information...")

        trips_csv = self.locate_csv('trips')
        stops_csv = self.locate_csv('stops')
        stop_times_csv = self.locate_csv('stop_times')

        trips = pd.read_csv(trips_csv, dtype=str)
        stops = pd.read_csv(stops_csv, dtype=str)
        stop_times = pd.read_csv(stop_times_csv, dtype=str)

        stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(int)

        parser_logger.info("Loaded trips, stops, and stop_times into DataFrames")

        x_to_r = lambda str_: str_.replace(r'(.*?_.*?_.*\..*?)X.*', r'\1R', regex=True) # convert the weird shape_ids with 'X' in them to be <everything up to the X> + 'R'
        slice_shape_from_trip = lambda str_: str_.replace(r'.*_(.*?$)', r'\1', regex=True) # slice what's after the last '_'

        # For any rows with empty 'shape_id', take the trip_id value, fix the 'X' if necessary, then slice to get shape_id
        trips['shape_id'] = trips['shape_id'].fillna(
            slice_shape_from_trip(
                x_to_r(
                    trips['trip_id'].str
                )
            )
        )

        composite = pd.merge(trips, stop_times, how='inner', on='trip_id')
        composite = pd.merge(composite, stops, how='inner', on='stop_id')
        composite = composite[self.rswn_columns]
        composite = composite.drop_duplicates().sort_values(by=self.rswn_sort_by)

        rswn_csv = self.locate_csv('route_stops_with_names')
        composite.to_csv(rswn_csv, index=False)
        parser_logger.info('%s created', rswn_csv)

    def update(self, force=False):
        """Downloads new static GTFS data, checks if different than existing data,
        unzips, and then generates additional csv files:

        downloads the zip to {data_path}/tmp/static_data.zip
        unzips it to {data_path}/tmp/static_data
            if successful, it then deletes {data_path}/static/GTFS/ and moves the new data there
        merge_trips_and_stops combines trips, stops, and stop_times to make route_stops_with_names
        load_time_between_stops calculates time b/w each pair of adjacent stops using stop_times
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
            new_data = requests.get(url, allow_redirects=True, timeout=5)
        except requests.exceptions.RequestException as err:
            parser_logger.error('%s: Failed to connect to %s\n', err, url)
            if self.has_static_data():
                print('Can\'t connect to', url, 'but do have existing static data (err=',err)
                return False
            else:
                print('Can\'t connect to', url, 'and don\'t have existing static data (err=',err)
                return False


        with open(zip_path, 'wb') as zip_outfile:
            zip_outfile.write(new_data.content)

        parser_logger.info('Extracting zip to %s', tmp_path)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmp_path)

        parser_logger.info('removing %s', zip_path)
        os.remove(zip_path)

        if self.has_static_data() and not force:
            if not filecmp.dircmp(end_path, tmp_path).diff_files:
                parser_logger.info('No difference found b/w existing static/GTFS/raw and new downloaded data')
                parser_logger.info('Deleting %s and exiting update() early', tmp_path)
                parser_logger.info('use force=True to force an update')
                shutil.rmtree(tmp_path)
                return False

        parser_logger.info('Deleting %s to make room for new static data', end_path)
        shutil.rmtree(end_path)

        parser_logger.info('Moving new data from %s to %s', tmp_path, end_path)
        os.rename(tmp_path, end_path)

        self.merge_trips_and_stops()
        return True


    def load_station_info(self):
        with open(self.locate_csv('stops'), mode='r') as stops_file:
            station_id_counter = 0
            stops_with_parent_stations = []
            stops_csv_reader = csv.DictReader(stops_file)

            # First, load the stations (parent_station)
            for row in stops_csv_reader:
                stop_id, parent_station = row['stop_id'], row['parent_station']
                if not parent_station:
                    self.static_data['stations'][station_id_counter] = {
                        'name': stop_id,
                        'name': row['stop_name'],
                        'lat': row['stop_lat'],
                        'lon': row['stop_lon'],
                        'arrivals': misc.NestedDict(),
                        'travel_time': misc.NestedDict()
                    }
                    self.station_id_lookup[stop_id] = station_id_counter
                    station_id_counter += 1
                else:
                    stops_with_parent_stations.append(row)

            # Next, load the stops
            for row in stops_with_parent_stations:
                stop_id, parent_station = row['stop_id'], row['parent_station']
                if parent_station:
                    self.station_id_lookup[stop_id] = self.station_id_lookup[parent_station]

    def load_route_info(self):
        """ Load info for each route, and assign a route_id (int16), stored in route_id_lookup
        """
        route_id_counter = 0
        with open(self.locate_csv('routes'), mode='r') as route_file:
            route_csv_reader = csv.DictReader(route_file)
            for row in route_csv_reader:
                route_color = row['route_color'].strip()
                text_color = row['route_text_color'].strip()
                if not route_color:
                    route_color = 'D3D3D3' #light grey
                if not text_color:
                    text_color = '000000'
                route_color = int(route_color, 16)
                text_color = int(text_color, 16)
                route_id = route_id_counter

                self.route_id_lookup[row['route_id']] = route_id
                self.static_data['routes'][route_id] = {
                    'name': row['route_id'],
                    'desc': row['route_desc'],
                    'color': route_color,
                    'text_color': text_color,
                    'stations': set()
                }
                route_id_counter += 1

        with open(self.locate_csv('route_stops_with_names'), mode='r') as rswn_file:
            rwsn_csv_reader = csv.DictReader(rswn_file)
            for row in rwsn_csv_reader:
                route_id = self.route_id_lookup[row['route_id']]
                stop_id = self.station_id_lookup[row['stop_id']]
                self.static_data['routes'][route_id]['stations'].add(stop_id)

    def load_transfers(self):
        with open(self.locate_csv('transfers'), mode='r') as transfers_file:
            transfers_csv_reader = csv.DictReader(transfers_file)
            for row in transfers_csv_reader:
                if row['transfer_type'] == '2':
                    min_transfer_time = int(row['min_transfer_time'])
                    _from, _to = row['from_stop_id'], row['to_stop_id']
                    _from = self.station_id_lookup[_from]
                    _to = self.station_id_lookup[_to]

                    self.static_data['transfers'][_from][_to] = min_transfer_time

    def load_time_between_stops(self):
        """Parses stop_times.csv to populate self.static_data['stops'] with time between stops info
        """
        parser_logger.info("Loading stop info and time between stops")
        prev_stop_seq = -999
        prev_stop = None
        prev_arrival = None
        seen = set()

        with open(self.locate_csv('stop_times'), 'r') as stop_times_infile:
            stop_times_csv = csv.DictReader(stop_times_infile)
            for row in stop_times_csv:
                stop_id, stop_sequence = row['stop_id'], int(row['stop_sequence'])
                arrival = row['arrival_time'].split(':')
                arrival = datetime.timedelta(hours=int(arrival[0]), minutes=int(arrival[1]), seconds=int(arrival[2]))

                if (prev_stop, stop_id) not in seen:
                    seen.add( (prev_stop, stop_id) )
                    if stop_sequence - prev_stop_seq == 1:
                        station_id = self.station_id_lookup[stop_id]
                        prev_station = self.station_id_lookup[prev_stop]
                        travel_time = int((arrival-prev_arrival).total_seconds())
                        self.static_data['stations'][station_id]['travel_time'][prev_station] = travel_time

                prev_arrival, prev_stop, prev_stop_seq = arrival, stop_id, stop_sequence

    def to_json(self, attempt=0):
        """ Stores self.static_data in self.gtfs_settings.static_json_path+'/static.json'
        """
        json_path = self.gtfs_settings.static_json_path

        try:
            with open(json_path+'/static.json', 'w') as out_file:
                json.dump(self.static_data, out_file, cls=SetEncoder)

        except OSError:
            if attempt != 0:
                parser_logger.error('Unable to write to %s/static.json', json_path)
                exit()
            parser_logger.info('%s/static.json does not exist, attempting to create it', json_path)

            try:
                os.makedirs(json_path)
            except PermissionError:
                parser_logger.error('Don\'t have permission to create %s', json_path)
                exit()
            except FileExistsError:
                parser_logger.error('The file %s/static.json exists, no permission to overwrite', json_path)
                exit()

            self.to_json(attempt=attempt+1)

    def build(self, _tl, force=False):
        """Builds JSON from the GTFS (with improvements)

        For example:
        self.static_data = {
            'name': '<transit system name>',
            'static_timestamp': 1555298447,
            'routes': {
                0: { # <- route_id (int16)
                    'desc': 'Trains operate between A and B...',
                    'color': int('EE352E', 16), # (int32)
                    'text_color': int('000000', 16), # (int32)
                    'stations': [ #list of station_ids ( [int16*] )
                        0, 1, 2, 3, 4, ...
                    ]
                },
                ...
            },
            'route_id_lookup': {
                '1': 0,
                '2': 1,
                '3': 2,
                '4': 3,
                ...
            },
            'stations': {
                0: { # <- station_id (int16)
                    'long_name': 'Van Cortlandt Park - 242 St',
                    'lat': 40.889248, # <- will be 32 bit float
                    'lon': -73.898583, # <- will be 32 bit float
                    'travel_time': { #station_id (int16): time (int16)
                        <prev station_id>: 90,
                        <other prev station_id>: 270
                    },
                    'arrivals': {
                        (route_id, direction): {
                            (arrival_time, vertex_id),
                            ...
                        },
                        ...
                    }
                },
                ...
            },
            'station_id_lookup': {
                '101': 0,
                '101S': 0,
                '101N': 0,
                '102': 1,
                '102N': 1,
                '102S': 1,
                ...
            },
            'transfers': {
                <from_station_id>: { #(int16)
                    <to_station_id>: 0, #(int16)
                    <to_station_id>: 180,
                    <to_station_id>: 300
                },
                <from_station_id>: {
                    <to_station_id>: 0,
                    <to_station_id>: 180
                },
                ...
            }
        }
        """
        self.static_data = {
            'name': self.name,
            'routes': {},
            'route_id_lookup': {},
            'stations': {},
            'station_id_lookup': {},
            'transfers': defaultdict(dict)
        }
        self.station_id_lookup = self.static_data['station_id_lookup']
        self.route_id_lookup = self.static_data['route_id_lookup']

        self.load_station_info()
        _tl.tlog('load_station_info()')

        self.load_route_info()
        _tl.tlog('load_route_info()')

        self.load_transfers()
        _tl.tlog('load_transfers()')

        self.load_time_between_stops()
        _tl.tlog('load_time_between_stops()')

        self.static_data['static_timestamp'] = int(time.time())

        self.to_json()
        _tl.tlog('to_json()')
        parser_logger.info("New static build written to JSON")



def main(force=False):
    """Creates new StaticHandler, then calls update() and build()
    """
    with misc.TimeLogger() as _tl:
        parser_logger.info("\nSTATIC.PY")
        static_handler = StaticHandler(MTA_SETTINGS)
        _tl.tlog('StaticHandler()')

        static_is_new = static_handler.update(force)
        _tl.tlog('update()')

        if static_is_new:
            static_handler.build(_tl, force)
            _tl.tlog('build()')


if __name__ == '__main__':
    try:
        assert(sys.argv[1] in ('force'))
    except AssertionError:
        print('Usage: static.py force or static.py')
    except IndexError:
        main()
    else:
        main(force=True)
