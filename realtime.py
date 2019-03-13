#!/usr/bin/python3
"""Classes and methods for static GTFS data
"""
import logging
import time
import requests
import os
import json
#import asyncio
#import aiohttp
from google.transit import gtfs_realtime_pb2

from ts_config import mta_settings

STATUS_MESSAGES = ['approaching', 'stopped at', 'in transit to']

class NestedDict(dict):
    def __getitem__(self, key):
        if key in self:
            return self.get(key)
        return self.setdefault(key, NestedDict())

class RealtimeHandler:
    """Gets a new realtime GTFS feed
    """
    '''
    def trains_by_route(self, route_id):
        """Gets all the trains running on a given route
        """
        for entity in self.data_.entity:
            if entity.HasField('vehicle'):
                if entity.vehicle.trip.route_id is route_id:
                    _status = STATUS_MESSAGES[entity.vehicle.current_status]
                    #_name = self.transit_system.stops_info[entity.vehicle.stop_id].name
                    print(f'Train is {_status} {_name}')

    def next_arrivals(self, route_id, stop):
        """Gets the next arrivals for a stop & route
        """
        #data_ = self.data_[self.which_feed[route_id]]
        #print(f'{self.which_feed[route_id]} for {route_id}')
        data_ = self.data_['1']
        arrivals = []
        for entity in data_.entity:
            if entity.HasField('trip_update'):
                #print(entity.trip_update.trip.route_id)
                if entity.trip_update.trip.route_id == route_id:
                    for stop_time_update in entity.trip_update.stop_time_update:
                        if stop_time_update.stop_id == stop:
                            print(stop_time_update.stop_id)
                            if stop_time_update.arrival.time > time.time():
                                arrivals.append(stop_time_update.arrival.time)
        return arrivals

    def timestamp(self):
        """Gets the feed timestamp from the header
        """
        return self.data_.header.timestamp

    def feed_size(self):
        """Gets the size of the feed
        """
        print(len(str(self.data_)))
    '''

    def get_static(self):
        static_json = self.gtfs_settings.static_json_path+'/static.json'
        with open(static_json, mode='r') as static_json_file:
            self.static_data = json.loads(static_json_file.read())
        self.static_data['trains'] = NestedDict()

    def get_feed(self):
        response = requests.get(self.gtfs_settings.realtime_url)
        feed_message = gtfs_realtime_pb2.FeedMessage()
        feed_message.ParseFromString(response.content)
        self.realtime_data = feed_message

    def trip_to_shape(self, trip_id):
        """Takes a trip_id in form '092200_6..N03R' and returns just what's after the last underscore
        This should be the shape_id ('6..N03R')
        """
        return trip_id.split('_').pop()

    def entity_info(self, entity_body):
        """ pulls route_id, trip_id, and shape_id from entity
        """
        route_id = entity_body.trip.route_id
        trip_id = entity_body.trip.trip_id
        shape_id = self.trip_to_shape(trip_id)
        try:
            branch_id = self.static_data['routes'][route_id]['shape_to_branch'][shape_id]
        except KeyError:
            logging.debug('shape_id %s not found in static_data[\'routes\'][\'%s\'][\'shape_to_branch\']', shape_id, route_id)
            #logging.debug('%s',self.static_data['routes'][route_id]['shape_to_branch'])
            branch_id = None
        return [route_id, trip_id, shape_id, branch_id]

    def parse_feed(self):
        self.parsed_data = self.static_data
        for entity in self.realtime_data.entity:
            if entity.HasField('trip_update'):
                route_id, trip_id, shape_id, branch_id = self.entity_info(entity.trip_update)
                if not branch_id: continue

                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id
                    if stop_time_update.arrival.time > time.time():
                        arrival = stop_time_update.arrival.time
                        try:
                            self.parsed_data['stops'][stop_id]['arrivals'][branch_id].append(arrival)
                        except KeyError:
                            self.parsed_data['stops'][stop_id]['arrivals'][branch_id] = [arrival]

                        try:
                            self.parsed_data['trains'][branch_id][trip_id]['arrival_time'] = arrival
                        except KeyError:
                            self.parsed_data['stops'][stop_id]['arrivals'][branch_id] = [arrival]

            elif entity.HasField('vehicle'):
                route_id, trip_id, shape_id, branch_id = self.entity_info(entity.vehicle)
                if not branch_id: continue

                #self.parsed_data['trains'][branch_id] = {}
                #self.parsed_data['trains'][branch_id][trip_id] = {}
                self.parsed_data['trains'][branch_id][trip_id]['next_stop'] = entity.vehicle.stop_id
                self.parsed_data['trains'][branch_id][trip_id]['current_status'] = entity.vehicle.current_status
                self.parsed_data['trains'][branch_id][trip_id]['last_detected_movement'] = entity.vehicle.timestamp


    def to_json(self, attempt=0):
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
        with open(f'{self.gtfs_settings.realtime_json_path}/gtfs_realtime_example', mode='rb') as example_file:
            response = example_file.read()
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(response)
            self.realtime_data = feed_message


    def __init__(self, gtfs_settings, name=''):
        self.name = name
        self.gtfs_settings = gtfs_settings
        self.realtime_data = None
        self.static_data = self.data_parsed = {}

def time_test(name_, last_time):
    print(f'{name_}: {time.time()-last_time}')
    return time.time()

def main():
    """Creates new RealtimeHandler, which calls get_feed()
    """
    last_time = time.time()
    realtime_handler = RealtimeHandler(mta_settings, name='MTA')
    last_time = time_test('RealtimeHandler', last_time)
    realtime_handler.get_static()
    last_time = time_test('get_static', last_time)
    realtime_handler.get_feed()
    last_time = time_test('get_feed', last_time)
    realtime_handler.parse_feed()
    last_time = time_test('parse_feed', last_time)
    realtime_handler.to_json()
    last_time = time_test('to_json', last_time)

if __name__ == '__main__':
    main()
