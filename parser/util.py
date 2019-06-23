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
import logging
import logging.config
from dataclasses import dataclass, is_dataclass, field
import pyhash  # type: ignore
import hashlib


#####################################
#            CUSTOM TYPES           #
#####################################
Num = Union[int, float]

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
#             CONSTANTS             #
#####################################
LOG_LEVEL: str

SOCKETIO_PORT: int
SOCKETIO_IP: str = '127.0.0.1'

STATIC_PATH: str = f'/data/static/'
REALTIME_PATH: str = f'/data/realtime/'

REALTIME_FREQ: Num
REALTIME_TIMEOUT: Num
REALTIME_MAX_ATTEMPTS: int

MTA_API_KEY: str


#####################################
#            ENVIRONMENT            #
#####################################
try:
    LOG_LEVEL             =     os.environ['PARSER_LOG_LEVEL']               # 'INFO'   # noqa
    SOCKETIO_PORT         = int(os.environ['PARSER_SOCKETIO_SERVER_PORT'])   # 45654    # noqa
    REALTIME_FREQ         = int(os.environ['PARSER_LOG_LEVEL'])              # 15       # noqa
    REALTIME_TIMEOUT      = int(os.environ['REALTIME_TIMEOUT'])              # 3.2      # noqa
    REALTIME_MAX_ATTEMPTS = int(os.environ['REALTIME_MAX_ATTEMPTS'])         # 3        # noqa
    MTA_API_KEY           =     os.environ['MTA_API_KEY']                    # <32-digit alphanumeric string> # noqa

except KeyError:
    print('''ERROR: the following environmental variables must be set:
        PARSER_SOCKETIO_SERVER_PORT: int
        PARSER_LOG_LEVEL: str (one of 'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET')
        REALTIME_FREQ: int (seconds)
        REALTIME_TIMEOUT: int (seconds)
        REALTIME_MAX_ATTEMPTS: int (seconds)
        MTA_API_KEY: str (32-digit alphanumeric)''', file=sys.stderr)
    exit()

try:
    assert REALTIME_FREQ > REALTIME_TIMEOUT * REALTIME_MAX_ATTEMPTS
except AssertionError:
    print('WARNING: REALTIME_TIMEOUT * REALTIME_MAX_ATTEMPTS > REALTIME_FREQ\nDefault values substituted.', file=sys.stderr)
    REALTIME_FREQ, REALTIME_TIMEOUT, REALTIME_MAX_ATTEMPTS = 15, 3.2, 3


#####################################
#           LOGGING SETUP           #
#####################################
_log_format = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
_log_date_format = '%Y-%m-%d %H:%M:%S'
_log_formatter = logging.Formatter(fmt=_log_format, datefmt=_log_date_format)

logging.config.dictConfig({
    'class': logging.StreamHandler,
    'formatter': _log_formatter,
    'level': LOG_LEVEL
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
    def serialize(self) -> str:
        return f'{self.route},{self.final_station}'

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


class RealtimeJSONEncoder(json.JSONEncoder):
    def fix_branch_keys(self, dict_: dict) -> dict:
        data_dict = {}
        for k, v in dict_.items():
            if isinstance(v, dict):
                v = self.fix_branch_keys(v)
            if isinstance(k, Branch):
                k = k.serialize()
            data_dict[k] = v
        return data_dict

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        if is_dataclass(obj):
            return self.fix_branch_keys(obj.__dict__)
        return json.JSONEncoder.default(self, obj)


class StaticJSONDecoder(json.JSONDecoder):
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

_base_url = 'http://datamine.mta.info/mta_esi.php'
_feed_ids = sorted(['1', '2', '11', '16', '21', '26', '31', '36', '51'], key=int)
_url_dict = {feed_id: f'{_base_url}?key={MTA_API_KEY}&feed_id={feed_id}' for feed_id in _feed_ids}

GTFS_CONF = GTFSConf(
    name='MTA_subway',
    static_url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
    realtime_urls=_url_dict
)
