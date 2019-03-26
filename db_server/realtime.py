"""Classes and methods for static GTFS data
"""
import logging
import time
import os
import json
import eventlet
from eventlet.green.urllib import request
from eventlet.green.urllib import error as urllib_error
import warnings

import gzip
from google.transit import gtfs_realtime_pb2
from google.protobuf import message as protobuf_message

from transit_system_config import MTA_SETTINGS
import misc

parser_logger = misc.parser_logger

eventlet.monkey_patch()

class RealtimeHandler:
    """Gets a new realtime GTFS feed
    """

    def get_static(self):
        """Loads the static.json file into self.static_data"""
        static_json = self.gtfs_settings.static_json_path+'/static.json'
        with open(static_json, mode='r') as static_json_file:
            self.static_data = json.loads(static_json_file.read())
        self.static_data['trains'] = misc.NestedDict()


    def eventlet_fetch(self, url):
        with eventlet.Timeout(misc.TIMEOUT):
            try:
                response = request.urlopen(url).read()
                return response
            except (OSError, urllib_error.URLError, eventlet.Timeout) as err:
                parser_logger.error('%s: unable to connect to %s', err, url)
                return False

    def check_feed(self):
        """Gets a new realtime GTFS feed and checks if its timestamp is more recent
        than previous feeds' timestamps. This is done by storing the most recent timestamp seen in
        self.gtfs_settings.realtime_data_path+'/latest_feed_timestamp.txt'
        """
        request_pool = eventlet.GreenPool(20)
        response_list = request_pool.imap(self.eventlet_fetch, self.gtfs_settings.realtime_urls)

        try:
            all_bytes = b''.join(response_list)
            parser_logger.debug('realtime.py: received all feeds')
        except TypeError:
            parser_logger.info('Unable to receive all feeds, exiting check_feed & returning False')
            return False

        feed_message = gtfs_realtime_pb2.FeedMessage()

        with warnings.catch_warnings():
            warnings.filterwarnings(action='error',category=RuntimeWarning)

            try:
                feed_message.ParseFromString(all_bytes)
            except RuntimeWarning:
                parser_logger.warning('RuntimeWarning when attempting: feed_message.ParseFromString(response.content)')
                return False
            except protobuf_message.DecodeError:
                parser_logger.error('DecodeError when attempting: feed_message.ParseFromString(response.content)')
                return False

        parser_logger.debug('realtime.py: joined feeds and converted to a single FeedMessage object')

        new_feed_timestamp = feed_message.header.timestamp

        parser_logger.debug('new_feed_timestamp: %s', new_feed_timestamp)

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
            try:
                os.makedirs(self.gtfs_settings.realtime_data_path, exist_ok=True)
            except PermissionError:
                parser_logger.error('Don\'t have permission to write to %s', self.gtfs_settings.realtime_data_path)
                exit()

            parser_logger.info('Now, loading new realtime GTFS.')

        parser_logger.debug('This timestamp is %s secs old', time.time()-new_feed_timestamp)
        self.realtime_data = feed_message

        with open(latest_timestamp_file, 'w') as latest_response:
            latest_response.write(str(new_feed_timestamp))

        with open(f'{self.gtfs_settings.realtime_data_path}/realtime', 'w') as realtime_raw_file:
            realtime_raw_file.write(str(feed_message))

        return True


    def entity_info(self, entity_body):
        """ pulls route_id, trip_id, and shape_id from entity
        """
        trip_id = entity_body.trip.trip_id
        route_id = entity_body.trip.route_id

        if '6..N04' in trip_id or '6..S04' in trip_id: # i don't even know anymore...
            trip_id = trip_id.split('X')[0][:-2]


        shape_id = misc.trip_to_shape(trip_id, trip_to_shape_long_dict=self.trip_to_shape_long)

        try:
            branch_id = self.static_data['shape_to_branch'][shape_id]
        except KeyError:
            if shape_id:
                parser_logger.debug('entity_info(): couldn\'t find a branch_id for shape_id %s route_id %s trip_id %s', shape_id, route_id, trip_id)
            branch_id = None

        return [trip_id, branch_id, route_id]

    def parse_feed(self):
        """ Walks through self.realtime_data and creates self.parsed_data
        Uses self.static_data as a starting point
        """
        self.parsed_data = self.static_data
        self.parsed_data['realtime_timestamp'] = self.realtime_data.header.timestamp

        for _, route_data in self.parsed_data['routes'].items():
            route_data.pop('shapes')

        for entity in self.realtime_data.entity:
            if entity.HasField('trip_update'):
                eventlet.greenthread.sleep(0) # yield time to other server processes if necessary
                trip_id, branch_id, route_id = self.entity_info(entity.trip_update)
                if not branch_id:
                    #print('WHOOPS', trip_id)
                    continue

                for stop_time_update in entity.trip_update.stop_time_update:
                    stop_id = stop_time_update.stop_id
                    if stop_time_update.arrival.time > time.time():
                        arrival = stop_time_update.arrival.time
                        try:
                            self.parsed_data['stops'][stop_id]['arrivals'][route_id][branch_id].append(arrival)
                        except AttributeError: #if self...[branch_id] is not yet a list
                            self.parsed_data['stops'][stop_id]['arrivals'][route_id][branch_id] = [arrival]
                        except KeyError: #if self...['arrivals'] is not yet a NestedDict
                            try:
                                self.parsed_data['stops'][stop_id]['arrivals'] = misc.NestedDict()
                                self.parsed_data['stops'][stop_id]['arrivals'][route_id][branch_id] = [arrival]
                            except KeyError:
                                parser_logger.debug('stop_id %s is not in stops.txt', stop_id)

                        try:
                            self.parsed_data['trains'][route_id][branch_id][trip_id]['arrival_time'] = arrival
                        except KeyError: #if self.parsed_data['trains'] is not yet a NestedDict
                            self.parsed_data['trains'] = misc.NestedDict()
                            self.parsed_data['trains'][route_id][branch_id][trip_id]['arrival_time'] = arrival

            elif entity.HasField('vehicle'):
                trip_id, branch_id, route_id = self.entity_info(entity.vehicle)
                if not branch_id:
                    #print('WHOOPS', trip_id)
                    continue

                self.parsed_data['trains'][route_id][branch_id][trip_id]['next_stop'] = entity.vehicle.stop_id
                self.parsed_data['trains'][route_id][branch_id][trip_id]['current_status'] = entity.vehicle.current_status
                self.parsed_data['trains'][route_id][branch_id][trip_id]['last_detected_movement'] = entity.vehicle.timestamp



    def to_json(self, attempt=0):
        """dumps self.parsed_data to realtime.json
        """
        json_path = self.gtfs_settings.realtime_json_path
        json_str = json.dumps(self.parsed_data)

        try:
            with open(json_path+'/realtime.json', 'w') as json_file:
                json_file.write(json_str)
                parser_logger.debug('Wrote realtime parsed data to %s/realtime.json', json_path)

            with gzip.open(json_path+'/realtime.json.gz', 'wb') as gzip_file:
                gzip_file.write(json_str.encode('utf-8'))

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

        # load the trip_to_shape_long dict
        with open(f'{self.gtfs_settings.static_data_path}/trip_id_to_shape.json', 'r') as json_file_stream:
            nested_dict_ = json.loads(json_file_stream.read())
            self.trip_to_shape_long = {
                outer_k: {
                    int(inner_k): inner_v
                        for inner_k, inner_v in outer_v.items()
                    }
                for outer_k, outer_v in nested_dict_.items()
            }


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
