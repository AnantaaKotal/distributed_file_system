import rpyc
import os
import socket
import pathlib
import time
import timeout_decorator
from collections import deque

from rpyc.utils.server import ThreadedServer

DATA_DIR = str(pathlib.Path().absolute()) + "/tmp/"
REPLICA_DIR = str(pathlib.Path().absolute()) + "/rep/"
DIRECTORY_ADDR = 'localhost'
DIRECTORY_PORT = 12345
PORT = 8888
# List of lists, to keep track of the timers and leasing of the files.
leased_files = []
# a list of lists to keep track of servers requesting a particular file,
# data structure will hold another list consisting of [clientip not necessarily socket, timestamp, data to be written] and so on..
clients_in_queue = {}
# this should be global because otherwise every new thread of slave server will have empty structure for files_owned
files_owned = {} #dictionary becaus it will store filename : filestatus I couldn't figure out any other way to set the flag
files_replicated = []


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
            self.files_owned.append(filename)

        def exposed_replicated_read(self, filename):
            try:
                idx = self.files_owned.index(filename)
                file = self.files_owned[idx]
                with open(DATA_DIR + str(filename), 'r') as f:
                    file_obj = f.read()
                    return file_obj

            except ValueError:
                print("FILE NOT FOUND")
                exit()

        def exposed_read(self, filename, data):
            try:
                idx = self.files_owned.index(filename)
                file = self.files_owned[idx]
                with open(DATA_DIR + str(filename), 'r') as f:
                    data = f.read()
                return data

            except ValueError:
                try:
                    idx = self.files_replicated.index(filename)
                    file = self.files_replicated[idx]
                    with open(REPLICA_DIR + "replicated_" + str(filename), 'r') as f:
                        data = f.read()
                    return data

                except ValueError:
                    con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
                    directory = con.root.Directory()

                    primary_handler_addr = directory.get_primary_for_file(filename)
                    con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
                    primary_handler = con.root.Handler()
                    file_obj = primary_handler.replicated_read(filename)
                    with open(REPLICA_DIR + "replicated_" + str(filename), 'w') as f:
                        f.write(file_obj)

                    self.files_replicated.append(filename)

                    with open(REPLICA_DIR + "replicated_" + str(filename), 'r') as f:
                        data = f.read()
                    return data


        def exposed_get_primary_and_replicate(self,filename):
            con1 = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
            directory = con1.root.Directory()

            primary_handler_addr = directory.get_primary_for_file(filename)
            con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
            primary_handler = con.root.Handler()
            file_obj = primary_handler.replicated_read(filename)
            file_status = primary_handler.files_owned[filename]
            #Here replica is getting updated
            if filename in files_replicated:
                with open(DATA_DIR + str(filename), 'w') as file:
                    file.write(file_obj)
            #if replica is not already there on handler
            else:
                with open(DATA_DIR + str(filename), 'w') as file:
                    file.write(file_obj)
                files_replicated.append([filename,file)


        # Timer to track leasing period
        @timeout_decorator.timeout(30, timeout_exception="time ended for the lease")
        def open_file(self, filename, data):
            with open(DATA_DIR + str(filename), 'a+') as file:
                file.write(data)
            leased_files.pop(0)

        def writefile(self, filename, data):
            try:
                leased_files[filename].islocked = True
                self.open_file(self, filename, data)
            except timeout_decorator.timeout as timeout_exception:
                answer = input("Need to extend the lease?")
                if answer == 'YES' or answer == 'Yes' or answer == 'yes':
                    #we'll increase timeout period here
                else:
                    leased_files[filename].islocked = False
                    clients_in_queue[filename].pop(0)


        def exposed_write(self, filename, data, clientip=None, clientsocket=None):
            s_ClientAdress, s_ClientPort = self._conn._config['endpoints'][1]
            index = clients_in_queue[filename].index(clients_in_queue[filename].append([s_ClientAdress, s_ClientPort,data]))
            # we are using lamport's clock logic, checking if the just now appended call is at index 0
            if index == 0:
                #case1 : Current handler is primary owner
                if filename in self.files_owned:
                    #checking if file is already leased:
                    if files_owned[filename].islocked is False:
                        self.writefile(filename,data)
                    #if the file is leased
                    elif files_owned[filename].islocked:
                        while leased_files[filename].islocked:
                            time.sleep(10)
                        self.writefile(filename,data)
                #case2 : Current handler is not the primary owner
                elif filename not in files_owned:
                    #replicating file in below code along with status
                    self.exposed_get_primary_and_replicate(filename)
                    if con1.filestatus






                    if




if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)

    startup()
