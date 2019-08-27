""" This contains all utility functions and package variables.
"""
import os
import sys
from typing import \
    NamedTuple, List, Set, Dict, DefaultDict, \
    Type, TypeVar, NewType, \
    Any, Optional, Union
from collections import defaultdict
import time
import json
import asyncio
import logging
import logging.config
from dataclasses import dataclass, is_dataclass, field
import pyhash  # type: ignore
import hashlib

loop = asyncio.get_event_loop()


os.makedirs('/data/static/parsed', exist_ok=True)
os.makedirs('/data/realtime/parsed', exist_ok=True)


#####################################
#            CUSTOM TYPES           #
#####################################
Num = Union[int, float]

def to_num(_str: Any) -> Num:
    try:
        return int(_str)
    except ValueError:
        try:
            return float(_str)
        except ValueError as err:
            print(f'{_str} cannot be converted to int or float', file=sys.stderr)
            raise ValueError(err)

ShortHash = NewType('ShortHash', int)

StationHash  = NewType('StationHash', ShortHash)    # unique hash of station id  # noqa
RouteHash    = NewType('RouteHash', ShortHash)      # unique hash of route id    # noqa
TripHash     = NewType('TripHash', ShortHash)       # unique hash of trip id     # noqa

SpecifiedHash = TypeVar('SpecifiedHash', StationHash, RouteHash, TripHash)

ArrivalTime  = NewType('ArrivalTime', int)          # POSIX time                 # noqa
TravelTime   = NewType('TravelTime', int)           # number of seconds          # noqa
TransferTime = NewType('TransferTime', int)         # number of seconds          # noqa
TimeDiff = NewType('TimeDiff', int)                 # number of seconds

TripStatus = NewType('TripStatus', int)
STOPPED, DELAYED, ON_TIME = list(map(TripStatus, range(3)))


#####################################
#          CONSTANTS / ENV          #
#####################################
LOG_LEVEL: str = os.environ.get('LOG_LEVEL', 'INFO')

STATIC_PATH: str = f'/data/static'
REALTIME_PATH: str = f'/data/realtime'

REALTIME_FREQ: Num = to_num(os.environ.get('REALTIME_FREQ', 15))
REALTIME_TIMEOUT: Num = to_num(os.environ.get('REALTIME_TIMEOUT', 3.2))
REALTIME_MAX_ATTEMPTS: int = int(os.environ.get('REALTIME_MAX_ATTEMPTS', 3))

REALTIME_DATA_DICT_CAP: int = int(os.environ.get('REALTIME_DATA_DICT_CAP', 20))

MTA_API_KEY: str
MTA_REALTIME_BASE_URL: str = os.environ.get('MTA_REALTIME_BASE_URL', 'http://datamine.mta.info/mta_esi.php')
MTA_STATIC_URL: str = os.environ.get('MTA_STATIC_URL', 'http://web.mta.info/developers/data/nyct/subway/google_transit.zip')

REDIS_HOST: str  # redis_server
REDIS_PORT: int  # 6379

BRANCH_SERIALIZED_SENTINEL_CHAR = chr(30)

try:
    REDIS_HOSTNAME = os.environ['REDIS_HOSTNAME']
    REDIS_PORT = int(os.environ['REDIS_PORT'])
    MTA_API_KEY = os.environ['MTA_API_KEY']
except KeyError:
    print('ERROR: necessary environment variables (REDIS_HOSTNAME & REDIS_PORT & MTA_API_KEY) not set', file=sys.stderr)
    exit()

try:
    assert REALTIME_FREQ > REALTIME_TIMEOUT * REALTIME_MAX_ATTEMPTS
except AssertionError:
    print('WARNING: REALTIME_TIMEOUT * REALTIME_MAX_ATTEMPTS > REALTIME_FREQ\nDefault values substituted.', file=sys.stderr)
    REALTIME_FREQ, REALTIME_TIMEOUT, REALTIME_MAX_ATTEMPTS = 15, 3.2, 3


REDIS_EXP = REALTIME_FREQ * REALTIME_MAX_ATTEMPTS


#####################################
#              LOGGING              #
#####################################
logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s.%(msecs)03d %(module)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'handlers': {
        'stream': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        'parser': {
            'level': LOG_LEVEL,
            'handlers': ['stream']
        }
    }
})

log = logging.getLogger('parser')


#####################################
#         UTILITY METHODS           #
#####################################
def checksum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

hasher = pyhash.super_fast_hash()
def short_hash(input_: Any, type_hint: Type[SpecifiedHash]) -> SpecifiedHash:
    """ Returns a unique (TODO: collision handling!) int32 hash of a station, route, or trip id
    Examples:
        short_hash('101', StationHash) returns a short hash with type hint StationHash
        short_hash('1', RouteHash) returns a short hash with type hint RouteHash
        short_hash('092200_6..N03R', TripHash) returns a short hash with type hint TripHash
    """
    hash_int = hasher(str(input_) + type_hint.__name__)
    typed_hash = type_hint(ShortHash(hash_int))
    return typed_hash


#####################################
#      TRANSIT DATA STRUCTURES      #
#####################################
class Branch(NamedTuple):
    route: RouteHash
    final_station: StationHash
    # for DEBUGGING:
    route_name: str = ""
    def serialize(self) -> str:
        return f'{self.route}{BRANCH_SERIALIZED_SENTINEL_CHAR}{self.final_station}'

class StationArrival(NamedTuple):
    arrival_time: ArrivalTime
    trip_hash: TripHash

def dict_of_list_factory():
    return defaultdict(list)
def dict_of_dict_of_list_factory():
    return defaultdict(lambda: defaultdict(list))


@dataclass
class RouteInfo:
    desc: str
    color: int
    text_color: int
    stations: Set[StationHash]

@dataclass
class Station:
    id_: StationHash
    name: str
    lat: float
    lon: float
    travel_times: Dict[StationHash, TravelTime]

@dataclass
class Trip:
    id_: TripHash
    branch: Branch
    arrivals: Dict[StationHash, ArrivalTime] = field(default_factory=dict)
    status: TripStatus = ON_TIME
    timestamp: Optional[int] = None  # in seconds

    def add_arrival(self, station: StationHash, arrival_time: ArrivalTime):
        self.arrivals[station] = ArrivalTime(arrival_time)

@dataclass
class StaticData:
    name: str
    static_timestamp: int
    routes: Dict[RouteHash, RouteInfo]
    stations: Dict[StationHash, Station]
    routehash_lookup: Dict[str, RouteHash]
    stationhash_lookup: Dict[str, StationHash]
    transfers: DefaultDict[StationHash, Dict[StationHash, TransferTime]]

@dataclass
class RealtimeData(StaticData):
    realtime_timestamp: int
    trips: Dict[TripHash, Trip]


@dataclass
class TripDiff:
    deleted: List[TripHash] = field(default_factory=list)
    added: List[Trip] = field(default_factory=list)

@dataclass
class ArrivalsDiff:
    deleted: Dict[TripHash, List[StationHash]] = field(default_factory=dict_of_list_factory)
    added: Dict[TripHash, Dict[StationHash, ArrivalTime]] = field(default_factory=dict_of_list_factory)
    modified: Dict[TimeDiff, Dict[TripHash, List[StationHash]]] = field(default_factory=dict_of_dict_of_list_factory)

@dataclass
class StatusDiff:
    modified: Dict[TripHash, TripStatus] = field(default_factory=dict)

@dataclass
class BranchDiff:
    modified: Dict[TripHash, Branch] = field(default_factory=dict)

@dataclass
class DataDiff:
    realtime_timestamp: int
    trips: TripDiff
    arrivals: ArrivalsDiff
    status: StatusDiff
    branch: BranchDiff


class UpdateFailed(Exception):
    pass


#####################################
#            MISC CLASSES           #
#####################################
class StaticJSONEncoder(json.JSONEncoder):
    """ This is for encoding the parsed static data to JSON
    """
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if is_dataclass(obj):
            custom_obj = {
                "_type": str(type(obj)),
                "value": obj.__dict__
            }
            return custom_obj
        return json.JSONEncoder.default(self, obj)

class StaticJSONDecoder(json.JSONDecoder):
    """ This is for decoding (in realtime.py) the JSON-ized static parsed data
    """
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def keys_to_ints(self, dict_: dict) -> dict:
        data_dict = {}
        for k, v in dict_.items():
            if isinstance(v, dict):
                data_dict[k] = self.keys_to_ints(v)
            else:
                try:
                    data_dict[int(k)] = v
                except ValueError:
                    data_dict[k] = v
        return data_dict

    def object_hook(self, obj):
        if '_type' not in obj:
            return obj
        _type_dict = {
            '<class \'util.RouteInfo\'>': RouteInfo,
            '<class \'util.Branch\'>': Branch,
            '<class \'util.StationArrival\'>': StationArrival,
            '<class \'util.RouteInfo\'>': RouteInfo,
            '<class \'util.Station\'>': Station,
            '<class \'util.Trip\'>': Trip,
            '<class \'util.StaticData\'>': StaticData}
        try:
            _type = _type_dict[obj['_type']]
            data_dict = self.keys_to_ints(obj['value'])
            return _type(**data_dict)
        except IndexError:
            return obj


class RealtimeJSONEncoder(json.JSONEncoder):
    """ This is for encoding the parsed realtime data to JSON
    """
    """
    def fix_branches(self, dict_: dict) -> dict:
        data_dict = {}
        for k, v in dict_.items():
            if isinstance(v, dict):
                v = self.fix_branch_keys(v)
            elif isinstance(v, Branch):
                v = v.serialize()
            data_dict[k] = v
        return data_dict
    """

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if is_dataclass(obj):
            custom_obj = {
                "_type": str(type(obj)),
                "value": obj.__dict__
            }
            return custom_obj
        return json.JSONEncoder.default(self, obj)

class RealtimeJSONDecoder(json.JSONDecoder):
    """ This is for decoding (in realtime.py, with data from redis) the JSON-ized parsed realtime data
    """
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def fix_keys(self, dict_: dict) -> dict:
        data_dict = {}
        for k, v in dict_.items():
            if BRANCH_SERIALIZED_SENTINEL_CHAR in k:
                k = Branch(*k.split(BRANCH_SERIALIZED_SENTINEL_CHAR))
            if isinstance(v, dict):
                data_dict[k] = self.fix_keys(v)
            else:
                try:
                    data_dict[int(k)] = v
                except ValueError:
                    data_dict[k] = v
        return data_dict

    def object_hook(self, obj):
        if '_type' not in obj:
            return obj

        _type_dict = {
            '<class \'util.RealtimeData\'>': RealtimeData,
            '<class \'util.RouteInfo\'>': RouteInfo,
            '<class \'util.StationArrival\'>': StationArrival,
            '<class \'util.RouteInfo\'>': RouteInfo,
            '<class \'util.Station\'>': Station,
            '<class \'util.Trip\'>': Trip,
            '<class \'util.StaticData\'>': StaticData}

        try:
            _type = _type_dict[obj['_type']]
            data_dict = self.fix_keys(obj['value'])
            return _type(**data_dict)
        except IndexError:
            return obj


class TimeLogger:
    """ Convenient little way to log how long something takes.
    """
    def __init__(self):
        self.times = []

    def __enter__(self):
        self.tlog()
        return self

    def tlog(self, block_name=''):
        self.times.append((time.time(), block_name))

    def __exit__(self, exc_type, exc_val, exc_tb):
        prev_time, _ = self.times.pop(0)
        while len(self.times) > 0:
            time_, block_name = self.times.pop(0)
            block_time = time_ - prev_time
            log.debug('%s took %s seconds', block_name, block_time)
            prev_time = time_



#####################################
#         GTFS CONFIGURATION        #
#####################################
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

_base_url = MTA_REALTIME_BASE_URL
_feed_ids = sorted(['1', '2', '11', '16', '21', '26', '31', '36', '51'], key=int)
_url_dict = {feed_id: f'{_base_url}?key={MTA_API_KEY}&feed_id={feed_id}' for feed_id in _feed_ids}

GTFS_CONF = GTFSConf(
    name='MTA_subway',
    static_url=MTA_STATIC_URL,
    realtime_urls=_url_dict
)
