import time
from collections import namedtuple
import requests
from google.transit import gtfs_realtime_pb2
import transit_systems as ts_list

class Train:
    """Analogous to GTFS trip_id

    next_stop = analogous to GTFS trip_update.stop_time_update[0].stop_id
    next_stop_arrival = analogous to GTFS trip_update.stop_time_update[0].arrival
    """
    next_stop = None
    next_stop_arrival = None

    def __init__(self, trip_id, route_id):# branch_id = None):
        self.id_ = trip_id
        self.route = route_id
        #self.branch = branch_id


class Feeds:
    """Gets a new realtime GTFS feed
    """
    def all_feeds(self, session):
        all_data = {}
        for feed_id, url in self.urls.items():
            response = session.get(url)
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(response.content)
            all_data[feed_id] = feed_message
        return all_data

    def trains_by_route(self, route_id):
        feed_id = self.which_feed[route_id]
        for entity in self.data_[feed_id].entity:
            if entity.HasField('vehicle'):
                if entity.vehicle.trip.route_id is route_id:
                    _status = STATUS_MESSAGES[entity.vehicle.current_status]
                    _name = self.transit_system.stops_info[entity.vehicle.stop_id].name
                    print(f'Train is {_status} {_name}')

    def interate_stop_time_update_arrivals(self, entity):
        arrivals = []
        for stop_time_update in entity.trip_update.stop_time_update:
            if stop_time_update.stop_id == stop:
                if stop_time_update.arrival.time > time.time():
                    arrivals.append(stop_time_update.arrival.time)
        return arrivals

    def next_arrivals(self, train, stop):
        for entity in self.data_.entity:
            if entity.HasField('trip_update'):
                if entity.trip_update.trip.route_id == train:
                    return self.interate_stop_time_update_arrivals(entity)


    def timestamp(self):
        return self.data_.header.timestamp

    def feed_size(self):
        for feed_id in self.feed_ids:
            print(len(str(self.data_[feed_id])))

    def print_feeds(self):
        print(self.data_)

    def __init__(self, ts, session):
        self.which_feed = ts.gtfs_settings.which_feed
        self.urls = ts.gtfs_settings.gtfs_realtime_urls
        self.feed_ids = ts.gtfs_settings.feed_ids
        self.data_ = self.all_feeds(session)
