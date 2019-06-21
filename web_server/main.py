import eventlet
from server import DatabaseClient, WebServer  # type: ignore
import util as u  # type: ignore


if __name__ == "__main__":
    db_client = DatabaseClient()
    db_client.start()

    # ws_ports = list(range(u.WEB_SERVER_FIRST_PORT, u.WEB_SERVER_FIRST_PORT + u.NUMBER_OF_WEB_SERVERS))
    web_server = WebServer(u.WEB_SERVER_FIRST_PORT)
    web_server.start()

    db_client.add_web_server(web_server)

    while True:
        try:
            eventlet.greenthread.sleep(1)
        except KeyboardInterrupt:
            print('KeyboardInterrupt, exiting')
            web_server.stop()
            db_client.stop()
            exit()
