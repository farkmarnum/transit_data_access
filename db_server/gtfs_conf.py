"""Configuration information for specific transit systems
"""

from typing import NamedTuple, Dict


class GTFSConf(NamedTuple):
    name: str
    static_url: str
    realtime_urls: Dict[str, str]

LIST_OF_FILES = [
    'agency.txt',
    'calendar_dates.txt',
    'calendar.txt',
    'route_stops_with_names.txt',
    'routes.txt',
    'shapes.txt',
    'stop_times.txt',
    'stops.txt',
    'transfers.txt',
    'trips.txt'
]

API_KEY: str = 'f775a76bd1960c98831b3c2b06c19bb5'

_base_url = 'http://datamine.mta.info/mta_esi.php'
_feed_ids = sorted(['1', '2', '11', '16', '21', '26', '31', '36', '51'], key=int)
_url_dict = {feed_id: f'{_base_url}?key={API_KEY}&feed_id={feed_id}' for feed_id in _feed_ids}

GTFS_CONF = GTFSConf(
    name='MTA_subway',
    static_url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
    realtime_urls=_url_dict
)
