#!/usr/bin/python3
"""Classes and methods for static GTFS data
"""
import time
import requests

import asyncio
import aiohttp
from google.transit import gtfs_realtime_pb2
#import transit_systems as ts_list

'''
async def fetch_http(session, url):
    """Gets an http response with async from url
    """
    print(f'fetching feed from {url}')
    async with session.get(url) as response:
        if response.status != 200:
            response.raise_for_status()
        response_body = await response.read()
        return response_body

async def fetch_all(session, urls):
    """Sets up an asyncio http request for each feed
    """
    results = await asyncio.gather(
        *[asyncio.create_task(fetch_http(session, url)) for url in urls]
    )
    return results
'''

class Train:
    """Analogous to GTFS trip_id

    next_stop = analogous to GTFS trip_update.stop_time_update[0].stop_id
    next_stop_arrival = analogous to GTFS trip_update.stop_time_update[0].arrival
    """
    #next_stop = None
    #next_stop_arrival = None

    def __init__(self, trip_id, route_id):# branch_id = None):
        self.id_ = trip_id
        self.route = route_id
        #self.branch = branch_id

class Feeds:
    """Gets a new realtime GTFS feed
    """
    async def get_feed(self):
        """Get all feeds concurrently with asyncio via fetch_all()
        """
        # TODO make session continue past one request
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                if response.status != 200:
                    response.raise_for_status()
                response_body = await response.read()

        gtfs_feed = gtfs_realtime_pb2.FeedMessage()
        gtfs_feed.ParseFromString(response_body)

        return gtfs_feed

    def trains_by_route(self, route_id):
        """Gets all the trains running on a given route
        """
        #feed_id = self.which_feed[route_id]
        for entity in self.data_.entity:
            if entity.HasField('vehicle'):
                if entity.vehicle.trip.route_id is route_id:
                    #_status = STATUS_MESSAGES[entity.vehicle.current_status]
                    _status = entity.vehicle.current_status
                    _name = self.transit_system.stops_info[entity.vehicle.stop_id].name
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
        """Gets the size of all feeds put together
        """
        print(len(str(self.data_)))

    def get_feed_with_responses(self):
        response = requests.get(self.url)
        feed_message = gtfs_realtime_pb2.FeedMessage()
        feed_message.ParseFromString(response.content)
        return feed_message

    def __init__(self, ts):
        self.transit_system = ts
        #self.session = session
        #self.which_feed = ts.gtfs_settings.which_feed
        #self.feed_ids = ts.gtfs_settings.feed_ids
        self.url = ts.gtfs_settings.gtfs_realtime_url
        self.data_ = asyncio.run(self.get_feed())
