from static import TransitSystem
import types

class MTASubway(TransitSystem):
    """MTA_Subway extends TransitSystem with specific settings for the MTA's GTFS implementation
    """
    def _get_url(self, feed_id):
        _base_url = 'http://datamine.mta.info/mta_esi.php'
        _key = self.gtfs_settings.api_key
        return f'{_base_url}?key={_key}&feed_id={feed_id}'

    def __init__(self, name):
        self.gtfs_settings = types.SimpleNamespace(
            api_key='f775a76bd1960c98831b3c2b06c19bb5',
            gtfs_static_url='http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
            static_data_path='schedule_data/MTA',
            which_feed={
                '1':'1', '2':'1', '3':'1', '4':'1', '5':'1', '6':'1', 'GS':'1',
                'A':'26', 'C':'26', 'E':'26', 'H':'26', 'FS':'26',
                'N':'16', 'Q':'16', 'R':'16', 'W':'16',
                'B':'21', 'D':'21', 'F':'21', 'M':'21',
                'L':'2',
                'SI':'11',
                'G':'31',
                'J':'36', 'Z':'36',
                '7':'51'
            }
        )
        feed_ids = ['1', '2', '11', '16', '21', '26', '31', '51']
        url_list = { feed_id:self._get_url(feed_id) for feed_id in feed_ids }
        self.gtfs_settings.gtfs_realtime_urls = url_list
        self.gtfs_settings.feed_ids = feed_ids
        super().__init__(name, self.gtfs_settings)


class MBTASubway(TransitSystem):
    """MBTA_Subway extends TransitSystem with specific settings for the MBTA's GTFS implementation
    """
    def __init__(self, name):
        self.gtfs_settings = types.SimpleNamespace(
            api_key='',
            gtfs_static_url='https://cdn.mbta.com/MBTA_GTFS.zip',
            gtfs_realtime_urls={
                'Alerts': 'https://cdn.mbta.com/realtime/Alerts.pb',
                'TripUpdates': 'https://cdn.mbta.com/realtime/TripUpdates.pb',
                'VehiclePositions': 'https://cdn.mbta.com/realtime/VehiclePositions.pb'
            },
            static_data_path='schedule_data/MBTA'
            )
        super().__init__(name, self.gtfs_settings)
        self._rswn_list_of_columns.remove('parent_station')
        self._has_parent_station_column = False
