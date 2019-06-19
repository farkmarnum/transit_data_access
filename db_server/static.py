""" downloads static GTFS data, checks if it's new, parses it, and stores it
"""
import time
import os
from collections import defaultdict
import requests
import shutil
import csv
import zipfile
import json
import pandas as pd
import util as u  # type: ignore
from gtfs_conf import GTFS_CONF, LIST_OF_FILES  # type: ignore


class StaticHandler(object):
    """docstring for StaticHandler
    """

    def has_static_data(self):
        """Checks if static_data_path is populated with the csv files in LIST_OF_FILES
        Returns a bool
        """
        all_files_are_loaded = True
        for file_ in LIST_OF_FILES:
            all_files_are_loaded *= os.path.isfile(u.STATIC_RAW_PATH + file_)
        return all_files_are_loaded

    def get_feed(self) -> None:
        """Downloads new static GTFS data, checks if different than existing data,
        unzips, and then generates additional csv files:

            if successful, it then deletes {data_path}/static/GTFS/ and moves the new data there
        merge_trips_and_stops combines trips, stops, and stop_times to make route_stops_with_names
        load_time_between_stops calculates time b/w each pair of adjacent stops using stop_times
        """
        has_static_data = self.has_static_data()
        tmp_path = u.STATIC_TMP_PATH
        raw_path = u.STATIC_RAW_PATH
        zip_filepath = u.STATIC_ZIP_PATH + 'static_data.zip'
        old_zip_filepath = u.STATIC_ZIP_PATH + 'static_data_OLD.zip'

        try:
            os.makedirs(tmp_path, exist_ok=True)
            os.makedirs(raw_path, exist_ok=True)
            os.makedirs(u.STATIC_ZIP_PATH, exist_ok=True)
        except PermissionError:
            u.parser_logger.error('Don\'t have permission to write to %s or %s', tmp_path, raw_path)
            raise u.UpdateFailed('PermissionError')

        u.parser_logger.info('Downloading GTFS static data from %s to %s', self.url, zip_filepath)
        try:
            new_data = requests.get(self.url, allow_redirects=True, timeout=5)
        except requests.exceptions.RequestException as err:
            u.parser_logger.error('%s: Failed to connect to %s\n', err, self.url)
            raise u.UpdateFailed('Connection failure, couldn\'t get feed')

        if has_static_data:
            try:
                shutil.move(zip_filepath, old_zip_filepath)
            except FileNotFoundError:
                has_static_data = False

        with open(zip_filepath, 'wb') as zip_outfile:
            zip_outfile.write(new_data.content)

        if has_static_data:
            no_new_data = (u.checksum(zip_filepath) == u.checksum(old_zip_filepath))
            os.remove(old_zip_filepath)
            u.parser_logger.info('removing %s', old_zip_filepath)
            if no_new_data:
                raise u.UpdateFailed('Static data checksum matches previously parsed static data. No new data!')

        u.parser_logger.info('Extracting zip to %s', tmp_path)
        try:
            with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
                zip_ref.extractall(tmp_path)
        except zipfile.BadZipFile as err:
                raise u.UpdateFailed(err)

        u.parser_logger.info('Deleting %s to make room for new static data', raw_path)
        shutil.rmtree(raw_path)

        u.parser_logger.info('Moving new data from %s to %s', tmp_path, raw_path)
        os.rename(tmp_path, raw_path)

        self.merge_trips_and_stops()


    def locate_csv(self, name: str) -> str:
        """Generates the path/filname for the csv given the name & gtfs_settings
        """
        return u.STATIC_RAW_PATH + name + '.txt'


    def merge_trips_and_stops(self):
        """Combines trips.csv stops.csv and stop_times.csv into locate_csv('route_stops_with_names')
        Keeps the columns in rswn_columns

        Also, compiles a csv of truncated_trip_id, shape_id and writes it to locate_csv('trip_id_to_shape')
        """
        rswn_columns = [
            'route_id',
            'stop_sequence',
            'stop_id',
        ]
        rswn_sort_by = [
            'route_id',
            'stop_sequence'
        ]
        u.parser_logger.info("Cross referencing route, stop, and trip information...")

        trips_csv = self.locate_csv('trips')
        stops_csv = self.locate_csv('stops')
        stop_times_csv = self.locate_csv('stop_times')

        trips = pd.read_csv(trips_csv, dtype=str)
        stops = pd.read_csv(stops_csv, dtype=str)
        stop_times = pd.read_csv(stop_times_csv, dtype=str)

        stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(int)

        u.parser_logger.info("Loaded trips, stops, and stop_times into DataFrames")

        composite = pd.merge(trips, stop_times, how='inner', on='trip_id')
        composite = pd.merge(composite, stops, how='inner', on='stop_id')
        composite = composite[rswn_columns]
        composite = composite.drop_duplicates().sort_values(by=rswn_sort_by)

        rswn_csv = self.locate_csv('route_stops_with_names')
        composite.to_csv(rswn_csv, index=False)
        u.parser_logger.info('%s created', rswn_csv)


    def load_station_info(self) -> None:
        """ Loads info for each station
        """
        with open(self.locate_csv('stops'), mode='r') as stops_file:
            stops_csv_reader = csv.DictReader(stops_file)
            for row in stops_csv_reader:
                stop_id, parent_station = row['stop_id'], row['parent_station']
                if parent_station.strip() == '':
                    station_hash = u.short_hash(stop_id, u.StationHash)
                    self.data.stations[station_hash] = u.Station(
                        id_=station_hash,
                        name=row['stop_name'],
                        lat=float(row['stop_lat']),
                        lon=float(row['stop_lon']),
                        travel_times={})
                    self.data.stationhash_lookup[stop_id] = station_hash
                else:
                    station_hash = u.short_hash(parent_station, u.StationHash)
                    self.data.stationhash_lookup[stop_id] = station_hash


    def load_route_info(self) -> None:
        """ Loads info for each route
        """
        with open(self.locate_csv('routes'), mode='r') as route_file:
            route_csv_reader = csv.DictReader(route_file)
            for row in route_csv_reader:
                route_id = row['route_id']
                route_color = int(row['route_color'].strip() or 'D3D3D3', 16)
                text_color = int(row['route_text_color'].strip() or '000000', 16)
                route_hash = u.short_hash(route_id, u.RouteHash)
                self.data.routes[route_hash] = u.RouteInfo(
                    desc=row['route_desc'],
                    color=route_color,
                    text_color=text_color,
                    stations=set())
                self.data.routehash_lookup[route_id] = route_hash

        with open(self.locate_csv('route_stops_with_names'), mode='r') as rswn_file:
            rwsn_csv_reader = csv.DictReader(rswn_file)
            for row in rwsn_csv_reader:
                route_hash = self.data.routehash_lookup[row['route_id']]
                station_hash = self.data.stationhash_lookup[row['stop_id']]
                self.data.routes[route_hash].stations.add(station_hash)


    def load_transfers(self):
        with open(self.locate_csv('transfers'), mode='r') as transfers_file:
            transfers_csv_reader = csv.DictReader(transfers_file)
            for row in transfers_csv_reader:
                if row['transfer_type'] == '2':
                    _min_transfer_time = int(row['min_transfer_time'])
                    _from = self.data.stationhash_lookup[row['from_stop_id']]
                    _to   = self.data.stationhash_lookup[row['to_stop_id']]  # noqa
                    self.data.transfers[_from][_to] = _min_transfer_time
                else:
                    print('transfer_type != 2  ——  what do we do?!!')


    def parse(self) -> None:
        self.load_station_info()
        self.load_route_info()
        self.load_transfers()
        self.data.static_timestamp = int(time.time())


    def serialize(self, attempt=0) -> None:
        """ Stores self.data in u.STATIC_PARSED_PATH+'static.json'
        """
        json_path = u.STATIC_PARSED_PATH

        try:
            with open(json_path + 'static.json', 'w') as out_file:
                json.dump(self.data, out_file, cls=u.StaticJSONEncoder)
            u.parser_logger.info('Wrote parsed static data to %sstatic.json', json_path)

        except OSError as err:
            if attempt != 0:
                u.parser_logger.error('Unable to write to %sstatic.json', json_path)
                raise u.UpdateFailed(err)

            u.parser_logger.info('%sstatic.json does not exist, attempting to create it', json_path)

            try:
                os.makedirs(json_path)
            except PermissionError as err:
                u.parser_logger.error('Don\'t have permission to create %s', json_path)
                raise u.UpdateFailed(err)
            except FileExistsError as err:
                u.parser_logger.error('The file %sstatic.json exists, no permission to overwrite', json_path)
                raise u.UpdateFailed(err)

            self.serialize(attempt=attempt + 1)

    def update(self):
        u.parser_logger.info('~~~~~~~~~~ Running STATIC.py ~~~~~~~~~~')
        try:
            self.get_feed()
            self.parse()
            self.serialize()
        except u.UpdateFailed as err:
            u.parser_logger.error(err)

    def __init__(self) -> None:
        self.url = GTFS_CONF.static_url
        self.data: u.StaticData = u.StaticData(
            name=GTFS_CONF.name,
            static_timestamp=0,
            routes={},
            stations={},
            routehash_lookup={},
            stationhash_lookup={},
            transfers=defaultdict(dict)
        )
        self.data.static_timestamp = 0


if __name__ == '__main__':
    sh = StaticHandler()
    sh.update()
