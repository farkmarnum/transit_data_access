""" Initializes socket I/O stuff
"""
import misc

server_logger = misc.server_logger


def main():
    """ sets up server

    1) bind to a port a start listening for incoming connections
    2) when a new connection is made with a client (from one of my web servers), keep it open
        2a) SSL or something to make it secure?
    3) maintain a record of all current connections somewhere
    4) every time realtime.json changes, send it to all the connected clients
        4a) make sure files are sent and received fully & correctly
    """
    server_logger.info('\n~~~~~~~~~~~~ BEGINNING SERVER ~~~~~~~~~~~~\n')

if __name__ == "__main__":
    main()
