import time
import requests
from google.transit import gtfs_realtime_pb2

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


class Feed:
    """Gets a new realtime GTFS feed
    """
    def get_feed(self):
        response = requests.get(self.gtfs_feed_url)
        gtfs_feed = gtfs_realtime_pb2.FeedMessage()
        gtfs_feed.ParseFromString(response.content)
        return gtfs_feed

    def trains_by_route(self, route):
        #n_bound_trains = []
        for entity in self.feed.entity:
            if entity.HasField('vehicle'):
                if route is entity.vehicle.trip.route_id:
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
        for entity in self.feed.entity:
            if entity.HasField('trip_update'):
                if entity.trip_update.trip.route_id == train:
                    return self.interate_stop_time_update_arrivals(entity)


    def timestamp(self):
        return self.feed.header.timestamp

    def feed_size(self):
        return len(str(self.feed))

    def print_feed(self):
        print(self.feed)

    def __init__(self, route_id, transit_system):
        self.transit_system = transit_system
        self.feed_id = transit_system.gtfs_settings.which_feed[route_id]
        self.gtfs_feed_url = transit_system.gtfs_settings.gtfs_feed_url(route_id)
        self.feed = self.get_feed()
