import rpyc
import os
import socket
import pathlib
import time
import timeout_decorator
from collections import deque
from configparser import ConfigParser
from datetime import datetime
import math

from rpyc.utils.server import ThreadedServer
import socket


#Directory Address
DIRECTORY_ADDR = 'localhost'
DIRECTORY_PORT = 12345
PORT = 8888


# a list of lists to keep track of servers requesting a particular file,
# data structure will hold another list consisting of [clientip not necessarily socket, timestamp, data to be written] and so on..
clients_in_queue = {}


# this should be global because otherwise every new thread of slave server will have empty structure for files_owned
files_owned = "FILES_OWNED"
files_replicated = "FILES_REPLICATED"
on_lease = "ON_LEASE"

UUID = ""

# main directory for files
METADATA_DIR = str(pathlib.Path().absolute()) + "/config/metadata/"
FILES_DIR = str(pathlib.Path().absolute()) + "/files/"
OWNED = "/owned/"
REPLICATED = "/replicated/"


def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

# Report to Directory upon start
def report_self_to_directory(port):
    global PORT
    PORT = port

    con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
    directory = con.root.Directory()
    isRegistered, fname = directory.add_handler_address(get_ip_address(), port)

    global UUID
    UUID = fname
    print("Identifier: " + UUID)

    if isRegistered:
        f = open(METADATA_DIR + fname + ".conf", "r")
        print(f.read())
    else:
        config_object = ConfigParser()
        config_object[files_owned] = {}
        config_object[files_replicated] = {}
        config_object[on_lease] = {}
        with open(METADATA_DIR + fname + ".conf", 'w') as conf:
            config_object.write(conf)

        os.mkdir(FILES_DIR + UUID)
        os.mkdir(FILES_DIR + UUID + OWNED)
        os.mkdir(FILES_DIR + UUID + REPLICATED)

    print("Reported to Directory")


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

    report_self_to_directory(port)
    t.start()


class HandlerService(rpyc.Service):
    class exposed_Handler():
        def exposed_temp(self):
            time.sleep(10)
            raise TimeoutError

        def exposed_create(self, filename, data):
            con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
            directory = con.root.Directory()

            fileNameExists = directory.add_file(filename, get_ip_address(), PORT)

            global UUID

            if fileNameExists:
                raise ValueError("File Name Exists; Try another File Name")
            else:
                self.local_file_create(filename, data)

        def exposed_replicated_read(self, filename):
            files_owned_dir = FILES_DIR + UUID + OWNED
            if os.path.exists(files_owned_dir + str(filename)):
                with open(files_owned_dir + str(filename), 'r') as f:
                    data = f.read()
                return data
            else:
                raise ValueError("FILE NOT FOUND")

        def exposed_read(self, filename):
            if self.local_is_file_owned(filename):
                files_owned_dir = FILES_DIR + UUID + OWNED

                with open(files_owned_dir + str(filename), 'r') as f:
                    data = f.read()
                return data
            elif self.local_is_file_replicated(filename):
                config_object = ConfigParser()
                config_object.read_file(open(METADATA_DIR + UUID + '.conf'))

                files_replicated_dir = FILES_DIR + UUID + REPLICATED

                with open(files_replicated_dir + str(filename), 'r') as f:
                    data = f.read()
                return data
            else:
                try:
                    data = self.replicate_file_for_read(filename)
                    return data
                except ValueError:
                    raise ValueError("FILE NOT FOUND")

        def exposed_delete(self, filename):
            if self.local_is_file_owned(filename):
                # delete file
                files_owned_dir = FILES_DIR + UUID + OWNED
                os.remove(files_owned_dir + str(filename))

                # update metadata config for handler
                config_object = ConfigParser()
                config_object.read(METADATA_DIR + UUID + '.conf')

                config_object.remove_option('FILES_OWNED', filename)
                with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                    config_object.write(conf)

                self.print_on_update("Deleted")

                # Tell Directory to delete file from list
                con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
                directory = con.root.Directory()
                directory.delete_file_from_record(filename)
            else:
                # Find the Primary Handler for file
                con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
                directory = con.root.Directory()

                primary_handler_addr = directory.get_primary_for_file(filename)

                if primary_handler_addr != "None":
                    con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
                    primary_handler = con.root.Handler()

                    primary_handler.delete(filename)

                files_replicated_dir = FILES_DIR + UUID + REPLICATED
                if os.path.exists(files_replicated_dir + filename):
                    os.remove(files_replicated_dir + filename)

                config_object = ConfigParser()
                config_object.read(METADATA_DIR + UUID + '.conf')
                config_object.remove_option('FILES_REPLICATED', filename)

                with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                    config_object.write(conf)

                self.print_on_update("Deleted")


        # Exposed Function, for Directory Service to check if file has been replicated locally
        def exposed_is_file_replicated(self, filename):
            return self.local_is_file_replicated(filename)

        # Called by Directory Service to Reassign Primary for Node Resilience
        def exposed_make_primary(self, filename):
            files_replicated_dir = FILES_DIR + UUID + REPLICATED

            with open(files_replicated_dir + str(filename), 'r') as f:
                data = f.read()

            # Add to files owned
            self.local_file_create(filename, data)

            # Remove from replicated
            os.remove(files_replicated_dir + filename)

            config_object = ConfigParser()
            config_object.read(METADATA_DIR + UUID + '.conf')

            config_object.remove_option('FILES_REPLICATED', filename)

            with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                config_object.write(conf)

            self.print_on_update("Added")

        # Checks if owner of file
        def local_is_file_owned(self, filename):
            config_object = ConfigParser()
            config_object.read_file(open(METADATA_DIR + UUID + '.conf'))

            files_owned_list = list(config_object.items('FILES_OWNED'))

            for key, value in files_owned_list:
                if key == filename:
                    return True
            return False

        # Checks if file has been replicated locally
        def local_is_file_replicated(self, filename):
            config_object = ConfigParser()
            config_object.read_file(open(METADATA_DIR + UUID + '.conf'))

            files_replicated_list = list(config_object.items('FILES_REPLICATED'))

            for key, value in files_replicated_list:
                if key == filename:
                    replicated_time = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
                    current_time = datetime.now()
                    diff = math.floor((current_time - replicated_time).total_seconds())

                    if diff <= 30:
                        return True
            return False

        def local_file_create(self, filename, data):
            config_object = ConfigParser()
            config_object.read_file(open(METADATA_DIR + UUID + '.conf'))


            # Update Metadata Config
            files = config_object["FILES_OWNED"]
            files[filename] = "NO"

            with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                config_object.write(conf)

            # Create file locally
            files_owned_dir = FILES_DIR + UUID + OWNED
            with open(files_owned_dir + str(filename), 'w') as f:
                f.write(data)

            self.print_on_update("Created")

        def replicate_file_for_read(self, filename):
            files_replicated_dir = FILES_DIR + UUID + REPLICATED

            con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
            directory = con.root.Directory()

            primary_handler_addr = directory.get_primary_for_file(filename)

            if primary_handler_addr == "None":
                # file has been removed or does not exist
                if os.path.exists(files_replicated_dir + str(filename)):
                    os.remove(files_replicated_dir + str(filename))

                raise ValueError("FILE NOT FOUND")
            else:
                con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
                primary_handler = con.root.Handler()

                file_obj = primary_handler.replicated_read(filename)

                with open(files_replicated_dir + str(filename), 'w') as f:
                    f.write(file_obj)

                # Add timestamp for replication
                current_time = datetime.now()
                current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")

                config_object = ConfigParser()
                config_object.read_file(open(METADATA_DIR + UUID + '.conf'))

                files_replicated_info = config_object["FILES_REPLICATED"]
                files_replicated_info[filename] = current_time_str

                with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                    config_object.write(conf)

                self.print_on_update("Replicated")

                with open(files_replicated_dir + str(filename), 'r') as f:
                    data = f.read()

                return data

        def print_on_update(self, func):
            print("=====================================================")
            print("A file has been %s" % (func))
            print("Updated File List for this Server")
            f = open(METADATA_DIR + UUID + ".conf", "r")
            print(f.read())

        # *****************************************************Timer******************************

        # Replicating the file locally, it updates the existing replica as
        def exposed_get_primary_and_replicate(self,filename):
            con1 = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
            directory = con1.root.Directory()

            primary_handler_addr = directory.get_primary_for_file(filename)
            con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
            primary_handler = con.root.Handler()
            file_obj = primary_handler.replicated_read(filename)
            # adding reference in primary owner about replicas
            #ANANTAA: what is the conn attribute?

            #list of handlers that have replica and need to be updated
            primary_handler.files_owned[filename] = (socket.gethostbyname('localhost'), PORT)

            #Here replica is getting updated if it exists locally on handler
            if filename in files_replicated:
                with open(DATA_DIR + str(filename), 'w') as file:
                    file.write(file_obj)
            #if replica is not already there on handler
            else:
                with open(DATA_DIR + str(filename), 'w') as file:
                    file.write(file_obj)
                files_replicated[filename] = primary_handler

# broadcast from primary


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

        #make leased file a config
        def Initiate_Write_RequestOnTop(self,filename,data):
            global files_owned
            # case1 : Current handler is primary owner
            if filename in files_owned:
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

                    # push to primary

        def exposed_write(self, filename, data, clientip=None, clientsocket=None):
            #ANANTAA: what is the conn attribute?
            s_ClientAdress, s_ClientPort = HandlerService.stuff()

            index = clients_in_queue[filename].index(clients_in_queue[filename].append([s_ClientAdress, s_ClientPort,data]))
            # we are using lamport's clock logic, checking if the just now appended request is at index 0
            if index == 0:
                #we call Initiate_Write_RequestOnTop function
                self.Initiate_Write_RequestOnTop(self,filename,data)
            else:
                # we check if request is on top after 10secs sleep
                while(clients_in_queue[filename].index([s_ClientAdress, s_ClientPort,data]) != 0):
                    time.sleep(10)

                self.Initiate_Write_RequestOnTop(self, filename, data)

# *****************************************************Timer******************************


if __name__ == "__main__":
    startup()
