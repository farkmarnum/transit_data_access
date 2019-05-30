""" This script manages the database server
"""

import eventlet
import util as ut
import server

def main():
    print(f'Logging in {ut.LOG_PATH}')
    db_server = server.main()
    while True:
        try:
            eventlet.sleep(1)
        except KeyboardInterrupt:
            print('\nKeyboardInterrupt, exiting')
            break
    db_server.stop()

if __name__ == "__main__":
    main()