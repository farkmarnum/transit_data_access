import eventlet
from server import WebServer  # type: ignore


if __name__ == "__main__":
    web_server = WebServer()
    web_server.start()

    while True:
        try:
            eventlet.greenthread.sleep(1)
        except KeyboardInterrupt:
            print('KeyboardInterrupt, exiting')
            web_server.stop()
            exit()
