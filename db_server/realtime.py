"""Classes and methods for static GTFS data
"""
import logging
import time
import os
import json
import eventlet
import requests
#import gzip
from google.transit import gtfs_realtime_pb2

from transit_system_config import MTA_SETTINGS
import misc

parser_logger = misc.parser_logger

class RealtimeHandler:
    """Gets a new realtime GTFS feed
    """
    def get_static(self):
        """Loads the static.json file into self.static_data"""
        static_json = self.gtfs_settings.static_json_path+'/static.json'
        with open(static_json, mode='r') as static_json_file:
            self.static_data = json.loads(static_json_file.read())
        self.static_data['trains'] = misc.NestedDict()

    def check_feed(self):
        """Gets a new realtime GTFS feed and checks if its timestamp is more recent
        than previous feeds' timestamps. This is done by storing the most recent timestamp seen in
        self.gtfs_settings.realtime_data_path+'/latest_feed_timestamp.txt'
        """
        response = requests.get(self.gtfs_settings.realtime_url, allow_redirects=True)
        feed_message = gtfs_realtime_pb2.FeedMessage()
        try:
            feed_message.ParseFromString(response.content)
        except RuntimeWarning:
            parser_logger.warning('RuntimeWarning when attempting: feed_message.ParseFromString(response.content)')
            exit()

        new_feed_timestamp = feed_message.header.timestamp

        latest_timestamp_file = self.gtfs_settings.realtime_data_path+'/latest_feed_timestamp.txt'
        try:
            with open(latest_timestamp_file, 'r+') as latest_timestamp_infile:
                latest_feed_timestamp = float(latest_timestamp_infile.read())
                if latest_feed_timestamp:
                    if new_feed_timestamp <= latest_feed_timestamp:
                        parser_logger.debug('We already have the most up-to-date realtime GTFS feed')
                        parser_logger.debug('Current realtime feed\'s timestamp is %s secs old', time.time()-latest_feed_timestamp)
                        if new_feed_timestamp < latest_feed_timestamp:
                            parser_logger.debug('We\'ve received an older feed...')
                            parser_logger.debug('This timestamp is %s secs old', time.time()-new_feed_timestamp)
                        return False

            parser_logger.info('Loading new realtime GTFS. Recent by %s seconds', new_feed_timestamp-latest_feed_timestamp)

        except FileNotFoundError:
            parser_logger.info('%s/latest_feed_timestamp.txt does not exist, will create it', self.gtfs_settings.realtime_data_path)
            os.makedirs(self.gtfs_settings.realtime_data_path, exist_ok=True)
            parser_logger.info('Now, loading new realtime GTFS.')

        parser_logger.debug('This timestamp is %s secs old', time.time()-new_feed_timestamp)
        self.realtime_data = feed_message
        with open(latest_timestamp_file, 'w') as latest_response:
            latest_response.write(str(new_feed_timestamp))
        return True


    def entity_info(self, entity_body):
        """ pulls route_id, trip_id, and shape_id from entity
        """
        trip_id = entity_body.trip.trip_id
        shape_id = misc.trip_to_shape(trip_id)
        if 'X' in shape_id: # TODO figure this out...
            shape_id = shape_id.split('X')[0]+'R'

        try:
            branch_id = self.static_data['shape_to_branch'][shape_id]
        except KeyError:
            branch_id = None
        return [trip_id, branch_id]

    def parse_feed(self):
        """ Walks through self.realtime_data and creates self.parsed_data
        Uses self.static_data as a starting point
        """
        self.parsed_data = self.static_data
        for _, route_data in self.parsed_data['routes'].items():
            route_data.pop('shapes')

        for entity in self.realtime_data.entity:
            if entity.HasField('trip_update'):
                eventlet.greenthread.sleep(0) # yield time to other server processes if necessary
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
        json_str = json.dumps(self.parsed_data)

        try:
            with open(json_path+'/realtime.json', 'w') as json_file:
                json_file.write(json_str)
                parser_logger.debug('Wrote realtime parsed data to %s/realtime.json', json_path)

            #with gzip.open(json_path+'/realtime.json.gz', 'wb') as gzip_file:
            #    gzip_file.write(json_str.encode('utf-8'))

        except OSError:
            if attempt != 0:
                parser_logger.error('Unable to write to %s/realtime.json\n', json_path)
                exit()

            parser_logger.info('%s/realtime.json does not exist, attempting to create it', json_path)
            try:
                os.makedirs(json_path, exist_ok=True)

            except PermissionError:
                parser_logger.error('Do not have permission to write to %s/realtime.json\n', json_path)
                exit()

            self.to_json(attempt=attempt+1)

    def __init__(self, gtfs_settings, name=''):
        self.name = name
        self.gtfs_settings = gtfs_settings
        self.realtime_data = None
        self.static_data = self.parsed_data = {}


def main():
    """Creates new RealtimeHandler, then calls get_static() and check_feed()
    """
    with misc.TimeLogger('realtime.py') as _tl:
        realtime_handler = RealtimeHandler(MTA_SETTINGS, name='MTA')
        realtime_handler.get_static()

        feed_is_new = realtime_handler.check_feed()
        if feed_is_new:
            realtime_handler.parse_feed()
            realtime_handler.to_json()

        return feed_is_new

if __name__ == '__main__':
    main()
