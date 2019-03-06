import sys, os, time, csv, requests, re
import zipfile, shutil
import dill, pickle
from google.transit import gtfs_realtime_pb2
import graphviz
import pandas as pd

from project_functions import _get, _set, _locate_csv, _rotate, Simp


STATUS_MESSAGES = ['approaching', 'stopped at', 'in transit to']



''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''
' '     The following classes describe STATIC components of the transit system:
' '     
' '
'''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

class Stop:
    """Analogous to GTFS 'stop_id'.

    Usage: Stop(stop_id)
    id = stop_id, '201S' for example
    """
    id_ = None

    def display(self):
        print('            '+self.id_)

    def __init__(self, stop_id):
        self.id_ = stop_id
        self.upcoming_trains = Simp()
        self.routes_that_stop_here = []


class ShapeBuilder:
    """Analogous to GTFS 'shape_id'.

    Usage: Shape(parent_route, shape_id)
    id = shape_id, '2..S08R' for example
    """
    id_ = stops = None

    def add_stop(self, stop_id):
        _set(self.stops, stop_id, Stop(stop_id))

    def __init__(self, parent_route, shape_id):
        self.route = parent_route
        self.id_ = shape_id
        self.stops = Simp()

class Shape(ShapeBuilder):
    def display(self):
        print('        '+self.id_)
        for stop_id, stop in self.stops.attr():
            stop.display()

    def __init__(self, parent_route, shape_id):
        super().__init__(parent_route, shape_id)


class RouteBuilder:
    shapes = all_stops = None

    def add_shape(self, shape_id):
        _set(self.shapes, shape_id, Shape(self, shape_id))

    def __init__(self, transit_system, route_id, route_long_name, route_desc, color = '', text_color = ''):
        self.transit_system = transit_system
        self.id_ = route_id
        self.longname = route_long_name
        self.route_desc = route_desc
        self.color = color
        self.text_color = text_color
        self.shapes = Simp()
        self.all_stops = Simp()

class Route(RouteBuilder):
    def display(self):
        print('    '+self.id_)
        for shape_id, shape in self.shapes.attr():
            shape.display()

    def __init__(self, transit_system, route_id, route_long_name, route_desc, color = '', text_color = ''):
        super().__init__(transit_system, route_id, route_long_name, route_desc, color, text_color)


class TransitSystemBuilder:
    """Not analogous to GTFS type.

    TODO: docstring for this class
    """
    routes = gtfs_settings = stop_info = None

    _has_parent_station_column = True
    _rswn_list_of_columns = ['route_id','shape_id','stop_sequence','stop_id','stop_name','stop_lat','stop_lon','parent_station']

    def _parse_stop_id(self, row):
        if self._has_parent_station_column:
            if row['parent_station'] is not None:
                return row['parent_station']
        return row['stop_id']

    def _merge_trips_and_stops(self):
        trips = pd.read_csv(_locate_csv('trips',self.gtfs_settings),dtype=str,low_memory=False)
        stops = pd.read_csv(_locate_csv('stops',self.gtfs_settings),dtype=str,low_memory=False)
        stop_times = pd.read_csv(_locate_csv('stop_times',self.gtfs_settings),dtype=str,low_memory=False)

        stop_times['stop_sequence'] = stop_times['stop_sequence'].astype(int)
        trips['shape_id'] = trips['shape_id'].fillna(trips['trip_id'].str.replace(r'.*_(.*?$)', r'\1', regex=True))

        route_stops_with_names = pd.merge(trips, stop_times, how='inner', on='trip_id')
        route_stops_with_names = pd.merge(route_stops_with_names, stops, how='inner', on='stop_id')
        route_stops_with_names = route_stops_with_names[self._rswn_list_of_columns].drop_duplicates().sort_values(by=['route_id','shape_id','stop_sequence'])
        route_stops_with_names.to_csv(_locate_csv('route_stops_with_names',self.gtfs_settings),index=False)

    def _load_travel_time_for_stops(self):
        # TODO
        pass

    def _download_new_schedule_data(self, _url):
        print(f'Downloading GTFS static data from {_url}...')
        if not os.path.exists('tmp'):
            os.mkdir('tmp')
        r = requests.get(gtfs_static_url, allow_redirects=True)
        open('tmp/schedule_data.zip', 'wb').write(r.content)

    def _unzip_new_schedule_data(self, _path):
        if os.path.exists(_path):
            shutil.rmtree(_path)
        else:
            os.mkdir(_path)

        with zipfile.ZipFile('tmp/schedule_data.zip',"r") as zip_ref:
            zip_ref.extractall(_path)

        if os.path.isfile('tmp/schedule_data.zip'):
            os.remove('tmp/schedule_data.zip')


    def update(self):
        _path = self.gtfs_settings.static_data_path
        _url = self.gtfs_settings.gtfs_static_url

        _download_new_schedule_data(_url)
        print(f'Done\nUnzipping file to {_path}...')
        _unzip_new_schedule_data(_path)
        print("Done\nCross referencing route, stop, and trip information...")
        self._merge_trips_and_stops()
        print("Done\nCalculating travel time between each stop...")
        self._load_travel_time_for_stops()


    def load_stop_info(self):
        with open(_locate_csv('stops',self.gtfs_settings), mode='r') as infile:
            reader = csv.DictReader(infile)
            for rows in reader:
                s = Simp()
                s.name, s.lat, s.lon = rows['stop_name'], rows['stop_lat'], rows['stop_lon']
                self.stop_info[rows['stop_id']] = s


    def add_route(self, route_id, route_long_name, route_desc, color = '', text_color = ''):
        print(f'Adding route: {route_id}')
        _set(self.routes, route_id, Route(self, route_id, route_long_name, route_desc, color, text_color))

    def load_all_routes(self):
        with open(_locate_csv('routes',self.gtfs_settings), mode='r') as infile:
            csv_reader = csv.DictReader(infile)
            for row in csv_reader:
                route_id = row['route_id']

                if row['route_color'].strip():
                    route_color = '#'+row['route_color']
                else:
                    route_color = 'lightgrey'

                if row['route_text_color'].strip():
                    route_text_color = '#'+row['route_text_color']
                else:
                    route_text_color = 'black'

                route_long_name = row['route_id']+': '+row['route_long_name']
                route_desc = row['route_desc']

                self.add_route(route_id, route_long_name, route_desc, route_color, route_text_color)

    def build_routes_shapes_stops(self):
        with open(_locate_csv('route_stops_with_names',self.gtfs_settings), mode='r') as rswn:
            csv_reader = csv.DictReader(rswn)
            current_route_id = current_shape_id = None
            for line in csv_reader:
                new_route_id = line['route_id']
                if new_route_id == current_route_id:
                    new_shape_id = line['shape_id']
                    if new_shape_id == current_shape_id:
                        stop_id = self._parse_stop_id(line)
                        shape.add_stop(stop_id)
                    else:
                        current_shape_id = new_shape_id
                        route.add_shape(current_shape_id)
                        shape = _get(route.shapes, current_shape_id)
                else:
                    current_route_id = new_route_id
                    route = _get(self.routes, current_route_id)

    def consolidate_shapes(self):
        # is this necessary?
        pass

    def build(self):
        print("Loading stop names...")
        self.load_stop_info()
        print("Done.\nLoading route information...")
        self.load_all_routes()
        print("Done.\nLoading GTFS static data into Route Shape and Stop objects...")
        self.build_routes_shapes_stops()
        print("Done.\nConsolidating 'shape.txt' data into a full network for each route...")
        self.consolidate_shapes()

    def map_each_route(self, route_desc_filter = None):
        _stop_networks = {}
        r = graphviz.Graph()
        for route_id, route in self.routes.attr():
            print(f'Mapping the network of stops for {route}')
            if route_desc_filter is None or _get(route,'route_desc') in route_desc_filter:
                _stop_networks[route_id]=graphviz.Graph(name='cluster_'+route_id)
                _stop_networks[route_id].graph_attr['label'] = route_id
                _stop_networks[route_id].graph_attr['fontsize'] = '36'
                _stop_networks[route_id].graph_attr['pencolor'] = '#bbbbbb'

        list_of_edge_str = []
        list_of_nodes = []
        with open(_locate_csv('route_stops_with_names', self.gtfs_settings), mode='r') as infile:
            csv_reader = csv.DictReader(infile)
            prev_stop = ''
            prev_shape = ''
            for row in csv_reader:
                stop_id = self._parse_stop_id(row)
                route_id = row['route_id']
                this_stop = stop_id + route_id
                if this_stop not in list_of_nodes:
                    list_of_nodes.append(this_stop)
                    route = _get(self.routes, route_id)
                    _stop_networks[route_id].node(this_stop,label=self.stop_info[stop_id].name,style='filled',fontsize='14',fillcolor=route.color,fontcolor=route.text_color)

                this_shape = row['shape_id']
                if this_shape == prev_shape:
                    edge_str = this_stop+' > '+prev_stop
                    if edge_str not in list_of_edge_str:
                        list_of_edge_str.append(this_stop+' > '+prev_stop)
                        list_of_edge_str.append(prev_stop+' > '+this_stop)
                        _stop_networks[route_id].edge(prev_stop, this_stop)
                prev_stop = this_stop
                prev_shape = this_shape

    def map_transit_system(self, route_desc_filter = None):
        _network = graphviz.Graph(engine='neato')
        #_network.graph_attr['rotation'] = '-29'
        #_network.graph_attr['no_translate'] = 'True'

        list_of_edge_str = []
        list_of_nodes = []
        with open(_locate_csv('route_stops_with_names', self.gtfs_settings), mode='r') as infile:
            csv_reader = csv.DictReader(infile)
            prev_stop = ''
            prev_shape = ''
            for row in csv_reader:
                this_stop = self._parse_stop_id(row)
                route_id = row['route_id']
                route = _get(self.routes, route_id)

                if route_desc_filter is None or route.route_desc in route_desc_filter:
                    if this_stop not in list_of_nodes:
                        list_of_nodes.append(this_stop)

                        s = self.stop_info[this_stop]
                        '''
                        origin = [40.7157825,-74.009569]
                        _lat, _lon = _rotate(origin, [float(s.lat),float(s.lon)], 1.0122)
                        '''
                        _lat, _lon = float(s.lat),float(s.lon)
                        _lat = (100-_lat)*400
                        _lon = _lon*400
                        _network.node(this_stop,label=self.stop_info[this_stop].name,fontsize='14',pos=f'{_lat},{_lon}!')

                    this_shape = row['shape_id']
                    if this_shape == prev_shape:
                        edge_str = this_stop+' > '+prev_stop+' in '+route_id
                        if edge_str not in list_of_edge_str:
                            edge_id = this_stop+' > '+prev_stop+' in '+route_id
                            rev_edge_id = prev_stop+' > '+this_stop+' in '+route_id
                            list_of_edge_str.extend([edge_id,rev_edge_id])
                            _network.edge(prev_stop, this_stop, color=route.color)
                    prev_stop = this_stop
                    prev_shape = this_shape

        with open(_locate_csv('transfers', self.gtfs_settings), mode='r') as infile:
            csv_reader = csv.DictReader(infile)
            for row in csv_reader:
                from_stop_id =row['from_stop_id']
                to_stop_id = row['to_stop_id']
                if to_stop_id != from_stop_id:
                    if set([from_stop_id, to_stop_id]).issubset(list_of_nodes):
                        _network.edge(from_stop_id, to_stop_id, color='black')


        format_ = 'pdf'
        print(f'Rendering /var/www/html/route_viz/{self.name}_full_graph.{format_}')
        _network.render(filename=f'{self.name}_full_graph',directory='/var/www/html/route_viz',cleanup=True,format=format_)


    def store(self, outfile):
        with open(outfile, 'wb') as pickle_file:
            pickle.dump(self, pickle_file, pickle.HIGHEST_PROTOCOL)

    def load(self, infile):
        with open(infile, 'rb') as pickle_file:
            return pickle.load(pickle_file)

    def __init__(self, name, gtfs_settings):
        self.name = name
        self.gtfs_settings = gtfs_settings
        self.routes = Simp()
        self.stop_info = {}

class TransitSystem(TransitSystemBuilder):
    def display(self):
        print(self.name)
        for route_id, route in self.routes.attr():
            route.display()

    def __init__(self, name, gtfs_settings):
        super().__init__(name, gtfs_settings)


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''
' '     The following classes describe REALTIME components of the transit system:
' '     Train, Feed
'''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


class Train:
    """Analogous to GTFS trip_id

    next_stop = analogous to GTFS trip_update.stop_time_update[0].stop_id
    next_stop_arrival = analogous to GTFS trip_update.stop_time_update[0].arrival
    """
    next_stop = None
    next_stop_arrival = None

    def __init__(self, trip_id, route_id,):# branch_id = None):
        self.id_ = trip_id
        self.route = route_id
        #self.branch = branch_id
        pass


class Feed:
    """Gets a new realtime GTFS feed
    """
    def get_feed(self):
        response = requests.get(self.gtfs_feed_url)
        gtfs_feed = gtfs_realtime_pb2.FeedMessage()
        gtfs_feed.ParseFromString(response.content)
        return gtfs_feed

    def trains_by_route(self, route):
        n_bound_trains = []
        for entity in self.feed.entity:
            if entity.HasField('vehicle'):
                if route is entity.vehicle.trip.route_id:
                    print("Train is", STATUS_MESSAGES[entity.vehicle.current_status], self.transit_system.stop_info[entity.vehicle.stop_id].name)

    def next_arrivals(self, train, stop):
        arrivals = []
        for entity in self.feed.entity:
            if entity.HasField('trip_update'):
                if entity.trip_update.trip.route_id == train:
                    for stop_time_update in entity.trip_update.stop_time_update:
                        if stop_time_update.stop_id == stop:
                            if stop_time_update.arrival.time > time.time():
                                arrivals.append(stop_time_update.arrival.time)
        return arrivals

    def timestamp(self):
        return self.feed.header.timestamp

    def feed_size(self):
        return len(str(self.feed))

    def print_feed(self):
        print(self.feed)

    def __init__(self, route_id, transit_system):
        self.transit_system = transit_system
        self.feed_id = transit_system.gtfs_settings.which_feed[route_id]
        self.gtfs_feed_url = transit_system.gtfs_settings.gtfs_feed_url(route_id)
        self.feed = self.get_feed()


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


class MTA_Subway(TransitSystem):
    """MTA_Subway extends TransitSystem with specific settings for the MTA's implementation of GTFS
    """
    def gtfs_feed_url(route_id):
        return f'{MTA_Subway.gtfs_settings.gtfs_base_feed_url}?key={MTA_Subway.gtfs_settings.api_key}&feed_id={MTA_Subway.gtfs_settings.which_feed[route_id]}'

    gtfs_settings = Simp(
        which_feed = {
        '1':'1','2':'1','3':'1','4':'1','5':'1','6':'1','GS':'1',
        'A':'26','C':'26','E':'26','H':'26','FS':'26',
        'N':'16','Q':'16','R':'16','W':'16',
        'B':'21','D':'21','F':'21','M':'21',
        'L':'2',
        'SI':'11',
        'G':'31',
        'J':'36','Z':'36',
        '7':'51'
        },
        feeds = ['1','2','11','16','21','26','31','51'],
        api_key = 'f775a76bd1960c98831b3c2b06c19bb5',
        gtfs_static_url = 'http://web.mta.info/developers/data/nyct/subway/google_transit.zip',
        gtfs_base_feed_url = 'http://datamine.mta.info/mta_esi.php',
        gtfs_feed_url = gtfs_feed_url,
        static_data_path = 'schedule_data/MTA'
        )

    def __init__(self, name):
        super().__init__(name, self.gtfs_settings)


class MBTA_Subway(TransitSystem):
    """MBTA_Subway extends TransitSystem with specific settings for the MBTA's implementation of GTFS
    """
    gtfs_settings = Simp(
        api_key = '',
        gtfs_static_url = 'https://cdn.mbta.com/MBTA_GTFS.zip',
        gtfs_feed_url = lambda route_id: '',
        static_data_path = 'schedule_data/MBTA'
        )

    def __init__(self, name):
        super().__init__(name, self.gtfs_settings)
        self._rswn_list_of_columns.remove('parent_station')
        self._has_parent_station_column = False


def main():
    '''
    mta =  MTA_Subway('MTA_Subway')
    mta.build()
    mta.store('stored_transit_systems/MTA.pkl')
    new_boi = mta.load('stored_transit_systems/MTA.pkl')
    new_boi.display()
    '''
    mbta = MBTA_Subway('MBTA_Subway')
    mbta.build()
    mbta.display()
    #mbta.map_transit_system(['Rapid Transit'])

    pass

if __name__ == "__main__":
    main()


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
