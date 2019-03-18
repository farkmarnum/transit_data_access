""" This script will set up a connection with the main server and receive new realtime.json.gz periodically
"""


def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((misc.SERVER_IP, misc.SERVER_PORT))
        sock.listen()
        while True:
            data = s.recv(1024)
            #TODO


if __name__ == "__main__":
    main()
