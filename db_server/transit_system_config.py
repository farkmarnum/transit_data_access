"""Configuration information for specific transit systems
"""
import types
from misc import DATA_PATH

MTA_API_KEY = 'f775a76bd1960c98831b3c2b06c19bb5'

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
    'trips.txt',
    'trip_id_to_shape.json'
]

def _mta_get_url(feed_id):
    _base_url = 'http://datamine.mta.info/mta_esi.php'
    _key = MTA_API_KEY
    return f'{_base_url}?key={_key}&feed_id={feed_id}'

_mta_feed_ids = ['1', '2', '11', '16', '21', '26', '31', '51']
_mta_url_list = [_mta_get_url(feed_id) for feed_id in _mta_feed_ids]

MTA_SETTINGS = types.SimpleNamespace(
    static_url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
    realtime_urls=_mta_url_list,
    realtime_data_path=f'{DATA_PATH}/MTA/realtime/raw',
    realtime_json_path=f'{DATA_PATH}/MTA/realtime/json',
    static_data_path=f'{DATA_PATH}/MTA/static/raw',
    static_json_path=f'{DATA_PATH}/MTA/static/json',
    static_tmp_path=f'{DATA_PATH}/MTA/static/tmp'
)
