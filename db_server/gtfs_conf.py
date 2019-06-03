"""Configuration information for specific transit systems
"""

from typing import NamedTuple

class GTFSConf(NamedTuple):
    name: str
    static_url: str
    realtime_urls: list

API_KEY = 'f775a76bd1960c98831b3c2b06c19bb5'

_base_url = 'http://datamine.mta.info/mta_esi.php'
_feed_ids = ['1', '2', '11', '16', '21', '26', '31', '51']
_url_list = [f'{_base_url}?key={API_KEY}&feed_id={feed_id}' for feed_id in _feed_ids]

GTFS_CONF = GTFSConf(
    name='MTA_subway',
    static_url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
    realtime_urls=_url_list
)
