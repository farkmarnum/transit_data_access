""" This script will set up a connection with the main server and receive new realtime.json.gz periodically
"""
import os
import time
import datetime
import time
import requests

import misc
from misc import client_logger

CLIENT_PATH = '/Users/markfarnum/dev/gtfs_parser/run_client/client_data'

server_url = f'http://{misc.DB_SERVER_IP}:{misc.DB_SERVER_PORT}/static/realtime.json'
json_local = CLIENT_PATH+'/realtime.json'


def get_new_json():
    new_json = requests.get(server_url)

    try:
        with open(json_local, 'wb') as outfile:
            outfile.write(new_json.content)
            client_logger.info('New realtime.json written to %s', json_local)

    except OSError:
        client_logger.error('Unable to write to %s', json_local)
        exit()

def check_for_new_json():
    # if we don't have a local realtime.json to compare, don't bother comparing
    if not os.path.exists(json_local):
        get_new_json()

    # send a request with an If-Modified-Since header with the local file's modified time
    else:
        local_modified_time = time.gmtime(os.path.getmtime(json_local))
        local_timestr = time.strftime("%a, %d %b %Y %I:%M:%S GMT", local_modified_time)
        headers = {
            "If-Modified-Since" : local_timestr
        }
        try:
            new_json_head = requests.head(server_url, headers=headers)
        except requests.exceptions.ConnectionError:
            client_logger.error('Failed to connect to %s\n', server_url)
            exit()

        if new_json_head.status_code == 200:
            get_new_json()
        elif new_json_head.status_code == 304:
            client_logger.debug('Server\'s realtime.json is not more recent than local copy')
        else:
            new_json_head.raise_for_status()

        #print(new_json_head.status_code)
        #print(new_json_head.headers)

def main():
    while True:
        check_for_new_json()
        time.sleep(1)

if __name__ == "__main__":
    main()
