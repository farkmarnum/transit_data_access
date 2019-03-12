#!/usr/bin/python3
"""Configuration information for specific transit systems
"""
import types
from static import StaticLoader

class MTASubwayLoader(StaticLoader):
    """MTA_Subway extends TransitSystem with specific settings for the MTA's GTFS implementation
    """
    '''
    def _get_url(self, feed_id):
        _base_url = 'http://datamine.mta.info/mta_esi.php'
        _key = self.gtfs_settings.api_key
        return f'{_base_url}?key={_key}&feed_id={feed_id}'
    '''
    def __init__(self, name):
        self.gtfs_settings = types.SimpleNamespace(
            api_key='f775a76bd1960c98831b3c2b06c19bb5',
            gtfs_static_url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
            static_data_path='schedule_data/MTA'
        )
        _key = self.gtfs_settings.api_key
        self.gtfs_settings.gtfs_realtime_url = f'http://datamine.mta.info/mta_esi.php?key={_key}&feed_id=1'
        super().__init__(name, self.gtfs_settings)

'''
class MBTASubwayLoader(StaticLoader):
    """MBTA_Subway extends TransitSystem with specific settings for the MBTA's GTFS implementation
    """
    def __init__(self, name):
        self.gtfs_settings = types.SimpleNamespace(
            api_key='',
            gtfs_static_url='https://cdn.mbta.com/MBTA_GTFS.zip',
            gtfs_realtime_urls={
                'https://cdn.mbta.com/realtime/Alerts.pb',
                'https://cdn.mbta.com/realtime/TripUpdates.pb',
                'https://cdn.mbta.com/realtime/VehiclePositions.pb'
            },
            static_data_path='schedule_data/MBTA'
            )
        super().__init__(name, self.gtfs_settings)
        self._rswn_list_of_columns.remove('parent_station')
        self._has_parent_station_column = False
'''
