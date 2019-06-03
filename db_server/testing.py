import eventlet
from eventlet.green.urllib import request
from eventlet.green.urllib import error as urllib_error
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
import util as ut
from gtfs_conf import GTFS_CONF
from datetime import datetime

class HeaderChecker():
    def eventlet_fetch(self, url):
        """ async fetch url HEADERS
        """
        with eventlet.Timeout(ut.TIMEOUT):
            try:
                request_headers = {'Cache-Control': 'no-store'}
                with request.urlopen(request.Request(url, method='GET', headers=request_headers)) as response:
                    return response.info(), response.read()

            except (OSError, urllib_error.URLError, eventlet.Timeout) as err:
                print('%s: unable to connect to %s, FAILED', err, url)

    def check(self):
        """get all new feeds, check each, and combine
        """
        request_pool = eventlet.GreenPool(20)
        response_list = request_pool.imap(self.eventlet_fetch, self.urls)
        for header, body in response_list:
            out_str = str(int((datetime.utcnow() - datetime.strptime(header['date'], '%a, %d %b %Y %H:%M:%S GMT')).total_seconds()))
            #out_str = str(datetime.strptime(header['date'], '%a, %d %b %Y %H:%M:%S GMT').timestamp())
            out_str += " "*(8-len(out_str))
            out_str += str(header['age'])
            out_str += " "*(16-len(out_str))
            feed_message = gtfs_realtime_pb2.FeedMessage()
            try:
                feed_message.ParseFromString(body)
                out_str += str(feed_message.header.timestamp)
            except DecodeError:
                out_str += 'DecodeError!'
            print(out_str)


    def __init__(self):
        self.urls = GTFS_CONF.realtime_urls

hc = HeaderChecker()

for _ in range(1):
    hc.check()
    eventlet.sleep(1)
    print()