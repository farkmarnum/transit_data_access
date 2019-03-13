#!/usr/bin/python3
"""Classes and methods for static GTFS data
"""
import logging
import time
import os
import json
import pprint as pp
import requests
from google.transit import gtfs_realtime_pb2

from ts_config import MTA_SETTINGS
from misc import NestedDict, trip_to_shape

logging.basicConfig(level=logging.DEBUG)

class RealtimeHandler:
    """Gets a new realtime GTFS feed
    """
    def get_static(self):
        """Loads the static.json file into self.static_data"""
        static_json = self.gtfs_settings.static_json_path+'/static.json'
        with open(static_json, mode='r') as static_json_file:
            self.static_data = json.loads(static_json_file.read())
        self.static_data['trains'] = NestedDict()

    def get_feed(self):
        """Loads a new realtime feed into self.realtime_data"""
        response = requests.get(self.gtfs_settings.realtime_url)
        feed_message = gtfs_realtime_pb2.FeedMessage()
        feed_message.ParseFromString(response.content)
        self.realtime_data = feed_message

    def entity_info(self, entity_body):
        """ pulls route_id, trip_id, and shape_id from entity
        """
        trip_id = entity_body.trip.trip_id
        shape_id = trip_to_shape(trip_id)

        if 'X' in shape_id: # TODO figure this out...
            shape_id = shape_id.split('X')[0]+'R'

        try:
            branch_id = self.static_data['shape_to_branch'][shape_id]
        except KeyError:
            route_id = entity_body.trip.route_id
            logging.debug('REALTIME ERROR: shape_id %s NOT FOUND (parsed from trip_id %s)', shape_id, trip_id)
            branch_id = None
        return [trip_id, branch_id]

    def parse_feed(self):
        """ Walks through self.realtime_data and creates self.parsed_data
        Uses self.static_data as a starting point
        """
        self.parsed_data = self.static_data
        for route, route_data in self.parsed_data['routes'].items():
            route_data.pop('shapes')

        for entity in self.realtime_data.entity:
            if entity.HasField('trip_update'):
                trip_id, branch_id = self.entity_info(entity.trip_update)
                if not branch_id:
                    continue

                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id
                    if stop_time_update.arrival.time > time.time():
                        arrival = stop_time_update.arrival.time
                        try:
                            self.parsed_data['stops'][stop_id]['arrivals'][branch_id].append(arrival)
                        except KeyError:
                            self.parsed_data['stops'][stop_id]['arrivals'][branch_id] = [arrival]

                        self.parsed_data['trains'][branch_id][trip_id]['arrival_time'] = arrival

            elif entity.HasField('vehicle'):
                trip_id, branch_id = self.entity_info(entity.vehicle)
                if not branch_id:
                    continue

                self.parsed_data['trains'][branch_id][trip_id]['next_stop'] = entity.vehicle.stop_id
                self.parsed_data['trains'][branch_id][trip_id]['current_status'] = entity.vehicle.current_status
                self.parsed_data['trains'][branch_id][trip_id]['last_detected_movement'] = entity.vehicle.timestamp


    def to_json(self, attempt=0):
        """dumps self.parsed_data to realtime.json
        """
        json_path = self.gtfs_settings.realtime_json_path

        try:
            with open(json_path+'/realtime.json', 'w') as out_file:
                json.dump(self.parsed_data, out_file)

        except OSError:
            if attempt != 0:
                exit(f'Unable to write to {json_path}/realtime.json')
            print(f'{json_path}/realtime.json does not exist, attempting to create it')

            try:
                os.makedirs(json_path)
            except PermissionError:
                exit(f'Do not have permission to write to {json_path}/realtime.json')

            self.to_json(attempt=attempt+1)

    def get_example_feed(self):
        """Simulate a new realtime feed with an example file"""
        with open(f'{self.gtfs_settings.realtime_json_path}/gtfs_realtime_example', mode='rb') as example_file:
            response = example_file.read()
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(response)
            self.realtime_data = feed_message


    def __init__(self, gtfs_settings, name=''):
        self.name = name
        self.gtfs_settings = gtfs_settings
        self.realtime_data = None
        self.static_data = self.parsed_data = {}


def main():
    """Creates new RealtimeHandler, which calls get_feed()
    """
    #last_time = time.time()
    realtime_handler = RealtimeHandler(MTA_SETTINGS, name='MTA')
    #last_time = time_test('RealtimeHandler', last_time)
    realtime_handler.get_static()
    #last_time = time_test('get_static', last_time)
    realtime_handler.get_feed()
    #last_time = time_test('get_feed', last_time)
    realtime_handler.parse_feed()
    #last_time = time_test('parse_feed', last_time)
    realtime_handler.to_json()
    #last_time = time_test('to_json', last_time)
    #pp.pprint(realtime_handler.parsed_data['stops'])

if __name__ == '__main__':
    main()
