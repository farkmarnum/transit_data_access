""" This contains all utility functions and package variables.
"""
import os
from typing import \
    NamedTuple, List, Set, Dict, DefaultDict, \
    Type, TypeVar, NewType, \
    Any, Optional
from collections import defaultdict
import time
import json
import logging
from dataclasses import dataclass, is_dataclass, field
import pyhash  # type: ignore
import hashlib
from gtfs_conf import GTFS_CONF  # type: ignore


#####################################
#             CONSTANTS             #
#####################################
PACKAGE_NAME = 'transit_data_access'
DATA_PATH = f'/data/{PACKAGE_NAME}/db_server'

REALTIME_RAW_PATH = f'{DATA_PATH}/{GTFS_CONF.name}/realtime/raw/'
REALTIME_PARSED_PATH = f'{DATA_PATH}/{GTFS_CONF.name}/realtime/parsed/'
STATIC_TMP_PATH = f'{DATA_PATH}/{GTFS_CONF.name}/static/tmp/'
STATIC_RAW_PATH = f'{DATA_PATH}/{GTFS_CONF.name}/static/raw/'
STATIC_PARSED_PATH = f'{DATA_PATH}/{GTFS_CONF.name}/static/parsed/'
STATIC_ZIP_PATH = f'{DATA_PATH}/{GTFS_CONF.name}/static/zip/'

LOG_PATH = f'/var/log/{PACKAGE_NAME}/db_server'
LOG_LEVEL = logging.INFO

IP = '127.0.0.1'
PORT = 8000

REALTIME_FREQ = 15
REALTIME_TIMEOUT = 2
MAX_ATTEMPTS = 3



#####################################
# TRANSIT TYPES / METHODS / CLASSES #
#####################################
ShortHash = NewType('ShortHash', int)

StationHash  = NewType('StationHash', ShortHash)    # unique hash of station id  # noqa
RouteHash    = NewType('RouteHash', ShortHash)      # unique hash of route id    # noqa
TripHash     = NewType('TripHash', ShortHash)       # unique hash of trip id     # noqa

SpecifiedHash = TypeVar('SpecifiedHash', StationHash, RouteHash, TripHash)

ArrivalTime  = NewType('ArrivalTime', int)          # POSIX time                 # noqa
TravelTime   = NewType('TravelTime', int)           # number of seconds          # noqa
TransferTime = NewType('TransferTime', int)         # number of seconds          # noqa
TimeDiff = NewType('TimeDiff', int)                 # number of seconds

hasher = pyhash.super_fast_hash()
def short_hash(input_: Any, type_hint: Type[SpecifiedHash]) -> SpecifiedHash:
    """ Returns a unique (TODO: collision handling!) hash of a station, route, or trip id
    Examples:
        short_hash('101', StationHash) returns a short hash with type hint StationHash
        short_hash('1', RouteHash) returns a short hash with type hint RouteHash
        short_hash('092200_6..N03R', TripHash) returns a short hash with type hint TripHash
    """
    hash_int = hasher(str(input_) + type_hint.__name__)
    typed_hash = type_hint(ShortHash(hash_int))
    return typed_hash



TripStatus = NewType('TripStatus', int)
STOPPED, DELAYED, ON_TIME = list(map(TripStatus, range(3)))

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
            '<class \'util.StaticData\'>': StaticData,
        }
        try:
            _type = _type_dict[obj['_type']]
            data_dict = self.keys_to_ints(obj['value'])
            # if _type == Station:
            #    del(data_dict['arrivals'])
            return _type(**data_dict)
        except IndexError:
            return obj


#####################################
#           LOGGING SETUP           #
#####################################
def log_setup(loggers: list):
    """ Creates paths and files for loggers, given a list of logger objects
    """
    # create log path if possible
    if not os.path.exists(LOG_PATH):
        print(f'Creating log path: {LOG_PATH}')
        try:
            os.makedirs(LOG_PATH)
        except PermissionError:
            print(f'ERROR: Don\'t have permission to create log path: {LOG_PATH}')
            exit()

    # set the format for log messages
    log_format = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'
    log_formatter = logging.Formatter(fmt=log_format, datefmt=log_date_format)

    # initialize the logger objects (passed in the 'loggers' param)
    for logger_obj in loggers:
        _log_file = f'{LOG_PATH}/{logger_obj.name}.log'
        try:
            _file_handler = logging.FileHandler(_log_file)
            _file_handler.setFormatter(log_formatter)
        except PermissionError:
            print(f'ERROR: Don\'t have permission to create log file: {_log_file}')
            exit()
        logger_obj.setLevel(LOG_LEVEL)
        logger_obj.addHandler(_file_handler)


parser_logger = logging.getLogger('parser')
server_logger = logging.getLogger('server')
log_setup([parser_logger, server_logger])


class TimeLogger:
    """ Convenient little way to log how long something takes. Usage:

    with TimeLogger() as _tl:
        # BLOCK 1
        _tl.log_time()
        # BLOCK 2
        _tl.log_time()
        # BLOCK 3
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
            parser_logger.info('%s took %s seconds', block_name, block_time)
            prev_time = time_


def checksum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
