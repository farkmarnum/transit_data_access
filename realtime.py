#!/usr/bin/python3
"""Classes and methods for static GTFS data
"""
#import time
#from collections import namedtuple
import asyncio
import aiohttp
from google.transit import gtfs_realtime_pb2
#import transit_systems as ts_list


async def fetch_http(session, feed_id, url):
    """Gets an http response with async from url & passes it along with feed_id
    """
    async with session.get(url) as response:
        if response.status != 200:
            response.raise_for_status()
        response_body = await response.read()
        return [feed_id, response_body]


async def fetch_all(session, urls):
    """Sets up an asyncio http request for each feed
    """
    results = await asyncio.gather(
        *[asyncio.create_task(fetch_http(session, feed_id, url)) for feed_id, url in urls]
    )
    return results

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
    async def all_feeds(self):
        """Get all feeds concurrently with asyncio via fetch_all()
        """
        async with aiohttp.ClientSession() as session:
            response = await fetch_all(session, self.urls.items())
            all_data = {}
            for feed_id, resp in response:
                #print(feed_id)
                gtfs_feed = gtfs_realtime_pb2.FeedMessage()
                gtfs_feed.ParseFromString(resp)
                all_data.update({feed_id: gtfs_feed})
            return all_data

    def trains_by_route(self, route_id):
        """Gets all the trains running on a given route
        """
        feed_id = self.which_feed[route_id]
        for entity in self.data_[feed_id].entity:
            if entity.HasField('vehicle'):
                if entity.vehicle.trip.route_id is route_id:
                    #_status = STATUS_MESSAGES[entity.vehicle.current_status]
                    _status = entity.vehicle.current_status
                    _name = self.transit_system.stops_info[entity.vehicle.stop_id].name
                    print(f'Train is {_status} {_name}')
    '''
    def iterate_stop_time_update_arrivals(self, entity, stop):
        arrivals = []
        for stop_time_update in entity.trip_update.stop_time_update:
            if stop_time_update.stop_id == stop:
                if stop_time_update.arrival.time > time.time():
                    arrivals.append(stop_time_update.arrival.time)
        return arrivals

    def next_arrivals(self, route_id, stop):
        #data_ = self.data_[self.which_feed[route_id]]
        #print(f'{self.which_feed[route_id]} for {route_id}')
        data_ = self.data_['1']
        for entity in data_.entity:
            if entity.HasField('trip_update'):
                print(entity.trip_update.trip.route_id)
                if entity.trip_update.trip.route_id == route_id:
                    return self.iterate_stop_time_update_arrivals(entity, stop)
    '''

    def timestamp(self, feed_id):
        """Gets the feed timestamp from the header
        """
        return self.data_[feed_id].header.timestamp

    def feed_size(self):
        """Gets the size of all feeds put together
        """
        for feed_id in self.feed_ids:
            print(len(str(self.data_[feed_id])))

    def __init__(self, ts):
        self.transit_system = ts
        self.which_feed = ts.gtfs_settings.which_feed
        self.urls = ts.gtfs_settings.gtfs_realtime_urls
        self.feed_ids = ts.gtfs_settings.feed_ids
        self.data_ = asyncio.run(self.all_feeds())
