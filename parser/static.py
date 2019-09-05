""" downloads static GTFS data, checks if it's new, parses it, and stores it
"""
# import os
from contextlib import suppress
import time
import requests
import shutil
import csv
import zipfile
import json
import pandas as pd
from redis import ResponseError
import util as u  # type: ignore
import middleware  # type: ignore


class StaticHandler(object):
    """docstring for StaticHandler
    """
    def __init__(self, redis_server) -> None:
        self.redis_server = redis_server
        self.current_checksum = None
        self.latest_checksum = None
        self.url: str = u.GTFS_CONF.static_url
        self.data: u.StaticData = u.StaticData(name=u.GTFS_CONF.name)
        self.data.static_timestamp = 0
        self.data_json_str: str = ''

    def get_feed(self) -> None:
        """Downloads new static GTFS data, checks if different than existing data,
        unzips, and then generates additional csv files:

        merge_trips_and_stops combines trips, stops, and stop_times to make route_stops_with_names
        load_time_between_stops calculates time b/w each pair of adjacent stops using stop_times
        """
        u.log.info('parser: Downloading GTFS static data from %s', self.url)
        try:
            new_data = requests.get(self.url, allow_redirects=True, timeout=10)
        except requests.exceptions.RequestException as err:
            raise u.UpdateFailed(f'{err}, failed to connect to {self.url}')

        _zipfile = f'{u.STATIC_PATH}/static_data.zip'
        with open(_zipfile, 'wb') as zip_out_stream:
            zip_out_stream.write(new_data.content)

        self.current_checksum = u.checksum(_zipfile)

        with suppress(ResponseError):
            if self.redis_server.exists('static_json'):
                if self.current_checksum == self.latest_checksum:
                    raise u.UpdateFailed(
                        'Static data checksum matches previously parsed static data. No new data!')

        _rawpath = f'{u.STATIC_PATH}/raw'
        shutil.rmtree(_rawpath, ignore_errors=True)
        u.log.info('parser: Extracting static GTFS zip')
        try:
            with zipfile.ZipFile(_zipfile, "r") as zip_ref:
                zip_ref.extractall(_rawpath)
        except zipfile.BadZipFile as err:
                raise u.UpdateFailed(err)

        self.get_additional_data()
        self.merge_trips_and_stops()

    def get_additional_data(self) -> None:
        for url in u.GTFS_CONF.additional_static_urls:
            try:
                data_file = requests.get(url, allow_redirects=True, timeout=10)
                out_path = f'{u.STATIC_PATH}/raw/{url.split("/")[-1].lower()}'
                with open(out_path, mode='w') as out_stream:
                    out_stream.write(data_file.text)
            except requests.exceptions.RequestException as err:
                raise u.UpdateFailed(f'{err}, failed to connect to {url}')


    def locate_csv(self, name: str) -> str:
        """Generates the path/filname for the csv given the name & gtfs_settings
        """
        for url in u.GTFS_CONF.additional_static_urls:
            shortened_name = url.split('/')[-1].lower()
            if shortened_name == name + ".csv":
                return f'{u.STATIC_PATH}/raw/{name}.csv'

        return f'{u.STATIC_PATH}/raw/{name}.txt'

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
        u.log.info("Cross referencing route, stop, and trip information...")

        trips_csv = self.locate_csv('trips')
        stops_csv = self.locate_csv('stops')
        stop_times_csv = self.locate_csv('stop_times')

        trips = pd.read_csv(trips_csv, dtype=str)
        stops = pd.read_csv(stops_csv, dtype=str)
        stop_times = pd.read_csv(stop_times_csv, dtype=str)

        stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(int)

        u.log.info("Loaded trips, stops, and stop_times into DataFrames")

        composite = pd.merge(trips, stop_times, how='inner', on='trip_id')
        composite = composite[rswn_columns]
        composite = pd.merge(composite, stops, how='inner', on='stop_id')
        composite.sort_values(by=rswn_sort_by, inplace=True, kind='quicksort')
        composite = composite.drop_duplicates()

        rswn_csv = self.locate_csv('route_stops_with_names')
        composite.to_csv(rswn_csv, index=False)
        u.log.info('parser: %s created', rswn_csv)

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
                        lon=float(row['stop_lon']))
                    self.data.stationhash_lookup[stop_id] = station_hash
                else:
                    station_hash = u.short_hash(parent_station, u.StationHash)
                    self.data.stationhash_lookup[stop_id] = station_hash

        with open(self.locate_csv('stations'), mode='r') as stations_file:
            stations_csv_reader = csv.DictReader(stations_file)
            for row in stations_csv_reader:
                if row['Complex ID'] != row['Station ID']:
                    station_complex = row['Complex ID']
                else:
                    station_complex = ''

                stop_id = row['GTFS Stop ID']
                station_hash = u.short_hash(stop_id, u.StationHash)

                if station_hash not in self.data.stations:
                    u.log.warning("%s -> %s not in self.data.stations", stop_id, station_hash)
                else:
                    borough, n_label, s_label = row['Borough'], row['North Direction Label'], row['South Direction Label']
                    borough = middleware.transform_borough(borough)
                    self.data.stations[station_hash].borough = borough
                    self.data.stations[station_hash].n_label = n_label
                    self.data.stations[station_hash].s_label = s_label
                    if station_complex:
                        self.data.stations[station_hash].station_complex = station_complex

        with open(self.locate_csv('stationcomplexes'), mode='r') as stations_file:
            stations_csv_reader = csv.DictReader(stations_file)
            for row in stations_csv_reader:
                self.data.station_complexes[row['Complex ID']] = row['Complex Name']


    def load_route_info(self) -> None:
        """ Loads info for each route
        """
        with open(self.locate_csv('routes'), mode='r') as route_file:
            route_csv_reader = csv.DictReader(route_file)
            for row in route_csv_reader:
                route_id = row['route_id']
                route_id = middleware.transform_route(route_id)
                route_color = int(row['route_color'].strip() or 'D3D3D3', 16)
                text_color = int(row['route_text_color'].strip() or '000000', 16)
                route_hash = u.short_hash(route_id, u.RouteHash)
                self.data.routes[route_hash] = u.RouteInfo(
                    desc=row['route_desc'],
                    color=route_color,
                    text_color=text_color,
                    stations=[])
                self.data.routehash_lookup[route_id] = route_hash

        with open(self.locate_csv('route_stops_with_names'), mode='r') as rswn_file:
            rwsn_csv_reader = csv.DictReader(rswn_file)
            for row in rwsn_csv_reader:
                route_id = row['route_id']
                route_id = middleware.transform_route(route_id)
                route_hash = self.data.routehash_lookup[route_id]
                station_hash = self.data.stationhash_lookup[row['stop_id']]
                stations = self.data.routes[route_hash].stations
                if station_hash not in stations:
                    stations.append(station_hash)

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
                    u.log.error('parser: transfer_type != 2  ——  what do we do?!!')

    def parse(self) -> None:
        self.load_station_info()
        self.load_route_info()
        self.load_transfers()
        self.data.static_timestamp = int(time.time())

    def serialize(self, attempt=0) -> None:
        """ Stores self.data in JSON format
        """
        _jsonfile = f'{u.STATIC_PATH}/parsed/static.json'
        self.data_json_str = json.dumps(self.data, cls=u.StaticJSONEncoder)

        with open(_jsonfile, 'w') as out_file:
            out_file.write(self.data_json_str)
        u.log.info('parser: Wrote parsed static data JSON to %s', _jsonfile)


    def update(self):
        u.log.info('parser: ~~~~~~~~~~ Running STATIC.py ~~~~~~~~~~')
        try:
            try:
                self.latest_checksum = self.redis_server.get('static:latest_checksum').decode('utf-8')
            except AttributeError:
                self.latest_checksum = None
            self.get_feed()
            self.parse()
            self.serialize()
            # TODO!!! improve with piping:
            self.redis_server.set('static:latest_checksum', self.current_checksum)
            self.redis_server.set('static:json_full', self.data_json_str)
        except u.UpdateFailed as err:
            u.log.error(err)
