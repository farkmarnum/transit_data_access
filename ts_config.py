#!/usr/bin/python3
"""Configuration information for specific transit systems
"""
import types

MTA_API_KEY = 'f775a76bd1960c98831b3c2b06c19bb5'

LIST_OF_FILES = [
    'agency',
    'calendar_dates',
    'calendar',
    'route_stops_with_names',
    'routes',
    'shapes',
    'stop_times',
    'stops',
    'transfers',
    'trips'
]

#STATUS_MESSAGES = ['approaching', 'stopped at', 'in transit to']

MTA_SETTINGS = types.SimpleNamespace(
    static_url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
    realtime_url=f'http://datamine.mta.info/mta_esi.php?key={MTA_API_KEY}',
    path_='/data/GTFS/MTA',
    realtime_json_path='/data/GTFS/MTA/realtime/json',
    static_data_path='/data/GTFS/MTA/static/raw',
    static_json_path='/data/GTFS/MTA/static/json',
    static_tmp_path='/data/GTFS/MTA/static/tmp'
)
