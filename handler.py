import rpyc
import os
import socket
import pathlib

from rpyc.utils.server import ThreadedServer

DATA_DIR = str(pathlib.Path().absolute()) + "/tmp/"
DIRECTORY_ADDR = 'localhost'
DIRECTORY_PORT = 12345
PORT = 8888


# Report to Directory upon start
def report_self_to_directory(host, port):
    con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
    directory = con.root.Directory()
    directory.add_handler_address(socket.gethostbyname('localhost'), port)

    # change global PORT
    global PORT
    PORT = port


# Handler Start up
def startup():
    """Allows up to 3 ports to connect on the same machine"""
    port = 8888
    try:
        print("Trying port 8888")
        t = ThreadedServer(HandlerService, port=port)
    except OSError:
        try:
            print("Trying port 8889")
            t = ThreadedServer(HandlerService, port=8889)
            port = 8889
        except OSError:
            try:
                print("Trying port 8890")
                t = ThreadedServer(HandlerService, port=8890)
                port = 8890
            except OSError:
                print("Try another localhost")
                exit()

    report_self_to_directory(t.host, t.port)
    print("Reported to Directory")
    t.start()


class HandlerService(rpyc.Service):
    class exposed_Handler():
        ip_address = socket.gethostbyname('localhost')

        def add_to_directory_file_table(self, filename):
            con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
            directory = con.root.Directory()
            directory.add_file(filename, self.ip_address, PORT)

        def exposed_create(self, filename, data):
            with open(DATA_DIR + str(filename), 'w') as f:
                f.write(data)
            self.add_to_directory_file_table(filename)


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)

    startup()
