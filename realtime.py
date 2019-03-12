#!/usr/bin/python3
"""Classes and methods for static GTFS data
"""
import logging
logging.basicConfig(level=logging.DEBUG)

import time
import requests
import os
import json
#import asyncio
#import aiohttp
from google.transit import gtfs_realtime_pb2

from ts_config import mta_settings

STATUS_MESSAGES = ['approaching', 'stopped at', 'in transit to']

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
        static_json = static_data_path+'/static.json'
        with open(static_json, 'r') as static_json_file:
            self.static_data = json.loads(static_json_file)

    def get_feed(self):
        response = requests.get(self.gtfs_settings.realtime_url)
        feed_message = gtfs_realtime_pb2.FeedMessage()
        feed_message.ParseFromString(response.content)
        self.realtime_data = feed_message


    def parse_feed(self):
        self.parsed_data = self.static_data

        for entity in self.realtime_data.entity:
            if entity.HasField('trip_update'):
                # TODO
                # update self.parsed_data
                pass

        for entity in self.realtime_data.entity:
            if entity.HasField('vehicle'):
                # TODO
                # update self.parsed_data
                pass



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

    def __init__(self, gtfs_settings, name=''):
        self.name = name
        self.gtfs_settings = gtfs_settings
        self.realtime_data = None
        self.static_data = self.data_parsed = {}


def main():
    """Creates new RealtimeHandler, which calls get_feed()
    """
    realtime_handler = RealtimeHandler(mta_settings, name='MTA')
    realtime_handler.get_static()
    realtime_handler.get_feed()
    realtime_handler.parse_feed()
    realtime_handler.to_json()
    pass

if __name__ == '__main__':
    main()
