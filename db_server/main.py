""" This script manages the database server
"""
import eventlet  # noqa
import schedule  # type: ignore
import util as u
import server
import static
import realtime
from gtfs_conf import GTFS_CONF

def static_parse() -> None:
    """ Fetches static feed, handles errors, and parses if new (which stores it in a file).
    """
    static_handler = static.StaticHandler()
    static_handler.update()

def scheduler(db_server) -> None:
    """ Uses schedule package to run:
        - the static parse every day at 3:30am
    """
    u.parser_logger.info('Starting scheduler')
    schedule.every().day.at("03:30").do(eventlet.spawn, static_parse)
    while True:
        schedule.run_pending()
        eventlet.sleep(0.5)

def main() -> None:
    """ Starts server, then starts scheduler for parsing. Stops server after interupt.
    """
    print(f'Logging in {u.LOG_PATH}')

    db_server = server.DatabaseServer()
    db_server.start()

    static_parse()
    realtime_manager = realtime.RealtimeManager(db_server)
    realtime_manager.run()

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
