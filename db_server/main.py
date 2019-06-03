""" This script manages the database server
"""
import eventlet
import schedule
import util as ut
import server
import static
import realtime
from gtfs_conf import GTFS_CONF

def static_parse():
    """ Fetches static feed, handles errors, and parses if new (which stores it in a file).
    """
    feed_status = static.get_new_feed()
    if not feed_status.feed_fetched:
        ut.parser_logger.warning('Unable to fetch new static feed.')
        return
    if not feed_status.feed_is_new:
        ut.parser_logger.info('Static feed fetched, not new.')
    else:
        static.parse_feed()

def realtime_parse_and_push(db_server):
    """ Fetches realtime feed, handles errors, and parses if new(which stores it in a file).
        Then, pushes new data to the web servers via the db_server
    """
    feed_result = realtime.get_new_feed()
    print('get_new_feed ended')
    if not feed_result.feed_fetched:
        ut.parser_logger.warning('Unable to fetch new realtime feed.')
        return
    if not feed_result.feed_is_new:
        ut.parser_logger.info('Realtime feed fetched, not new.')
    else:
        realtime.parse_feed()
        db_server.push()

def scheduler(db_server):
    """ Uses schedule package to run:
        - the realtime parse (and push to web server(s)) every REALTIME_FREQ seconds, and
        - the static parse every day at 3:30am
    """
    ut.parser_logger.info('Starting scheduler')
    schedule.every(ut.REALTIME_FREQ).seconds.do(eventlet.spawn, realtime_parse_and_push, db_server=db_server)
    schedule.every().day.at("03:30").do(eventlet.spawn, static_parse)
    while True:
        schedule.run_pending()
        eventlet.sleep(0.5)


def main():
    """ Starts server, then starts scheduler for parsing. Stops server after interupt.
    """
    print(f'Logging in {ut.LOG_PATH}')

    ut.server_logger.info('~~~~~~~~~~ server.py beginning! ~~~~~~~~~~')
    db_server = server.DatabaseServer()
    db_server.start()

    ut.parser_logger.info('~~~~~~~~~~ Beginning parsing for %s! ~~~~~~~~~~', GTFS_CONF.name)
    static_parse()
    realtime_parse_and_push(db_server)

    eventlet.spawn(scheduler, db_server)
    while True:
        try:    
            eventlet.sleep(1)
        except KeyboardInterrupt:
            print('\nKeyboardInterrupt, exiting')
            break
    db_server.stop()

if __name__ == "__main__":
    main()
