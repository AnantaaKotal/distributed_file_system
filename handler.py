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
# Directory to keep track of leased files with structure as {filename : file status} Holds the islocked flag
leased_files = {}
# a list of lists to keep track of servers requesting a particular file,
# data structure will hold another list consisting of [clientip not necessarily socket, timestamp, data to be written] and so on..
clients_in_queue = {}
# this should be global because otherwise every new thread of slave server will have empty structure for files_owned
files_owned = []
files_replicated = {}


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

        def exposed_Delete(self, filename):
            if filename in files_owned:
                os.remove(DATA_DIR + str(filename))
            elif filename in files_replicated:
                primary_handler_addr = files_replicated[filename]
                con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
                primary_handler = con.root.Handler()
                primary_handler.Delete(filename)

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
# Replicating the file locally, it updates the existing replica as
        def exposed_get_primary_and_replicate(self,filename):
            con1 = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
            directory = con1.root.Directory()

            primary_handler_addr = directory.get_primary_for_file(filename)
            con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
            primary_handler = con.root.Handler()
            file_obj = primary_handler.replicated_read(filename)
            # adding reference in primary owner about replicas
            primary_handler.files_owned[filename] =[self._conn._config['endpoints'][1]]
            #Here replica is getting updated if it exists locally on handler
            if filename in files_replicated:
                with open(DATA_DIR + str(filename), 'w') as file:
                    file.write(file_obj)
            #if replica is not already there on handler
            else:
                with open(DATA_DIR + str(filename), 'w') as file:
                    file.write(file_obj)
                files_replicated[filename] = primary_handler


# *****************************************************Timer******************************
        # Timer to track leasing period
        @timeout_decorator.timeout(30, timeout_exception="time ended for the lease")
        def open_file(self, filename, data):
            with open(DATA_DIR + str(filename), 'a+') as file:
                file.write(data)
            popped = leased_files.pop(0)
            print("Write complete for client: ", popped)

        @timeout_decorator.timeout(60, timeout_exception="time ended for the lease")
        def open_file1(self, filename, data):
            with open(DATA_DIR + str(filename), 'a+') as file:
                file.write(data)
            popped = leased_files.pop(0)
            print("Write complete for client: ", popped)

        def writefile(self, filename, data):
            try:
                leased_files[filename]= True
                self.open_file(self, filename, data)
            except timeout_decorator.timeout as timeout_exception:
                answer = input("Need to extend the lease?")
                if answer == 'YES' or answer == 'Yes' or answer == 'yes':
                    try:
                        self.open_file1(filename,data)
                    except timeout_decorator.timeout as timeout_exception:
                        print("write not finished in given time, no changes committed to the file.")
                else:
                    leased_files[filename] = False
                    clients_in_queue[filename].pop(0)

# **********************Initiate the write based on whether the file is present on current handler***************************
        def Initiate_Write_RequestOnTop(self,filename,data):
            # case1 : Current handler is primary owner
            if filename in self.files_owned:
                # checking if file is already leased:
                if leased_files[filename] is False:
                    self.writefile(filename, data)
                # if the file is leased
                elif leased_files[filename] is True:
                    while leased_files[filename] is True:
                        time.sleep(10)
                    self.writefile(filename, data)
            # case2 : Current handler is not the primary owner
            elif filename not in files_owned:
                # replicating file in below code along with status
                self.exposed_get_primary_and_replicate(filename)
                if leased_files[filename] is False:
                    self.writefile(filename, data)
                elif leased_files[filename] is True:
                    while leased_files[filename].islocked:
                        time.sleep(10)
                    self.writefile(filename, data)

        def exposed_write(self, filename, data, clientip=None, clientsocket=None):
            s_ClientAdress, s_ClientPort = self._conn._config['endpoints'][1]
            index = clients_in_queue[filename].index(clients_in_queue[filename].append([s_ClientAdress, s_ClientPort,data]))
            # we are using lamport's clock logic, checking if the just now appended request is at index 0
            if index == 0:
                #we call Initiate_Write_RequestOnTop function
                self.Initiate_Write_RequestOnTop(self,filename,data)
            else:
                time.sleep(10)
                # we check if request is on top after 10secs sleep
                if clients_in_queue[filename].index([s_ClientAdress, s_ClientPort,data]) == 0:
                    self.Initiate_Write_RequestOnTop(self, filename, data)



if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)

    startup()
