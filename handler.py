import rpyc
import os
import pathlib
import time
from configparser import ConfigParser
from datetime import datetime
import math

from rpyc.utils.server import ThreadedServer
import socket


#Directory Address
DIRECTORY_ADDR = 'localhost'
DIRECTORY_PORT = 12345

#DEFAULT PORT
PORT = 8888

# global variables
files_owned = "FILES_OWNED"
files_replicated = "FILES_REPLICATED"
on_lease = "ON_LEASE"

UUID = ""

# main directory for files
METADATA_DIR = str(pathlib.Path().absolute()) + "/config/metadata/"
FILES_DIR = str(pathlib.Path().absolute()) + "/files/"
OWNED = "/owned/"
REPLICATED = "/replicated/"

LEASE_TIME = 60
ALLOWED_EXTENSION_TIME = 2 * LEASE_TIME


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
            # !!!! Add queueing logic
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

        # ***********************************************************************************

        # Pessimistic write protocol including leasing logic
        def exposed_write_request(self, filename, commit_id):
            timestamp = datetime.now()
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

            if self.local_is_file_owned(filename):
                self.local_primary_write_queue(filename, commit_id, timestamp_str)
            else:
                con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
                directory = con.root.Directory()

                primary_handler_addr = directory.get_primary_for_file(filename)

                if primary_handler_addr != "None":
                    con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
                    primary_handler = con.root.Handler()

                    write_info = primary_handler.primary_write_queue(filename, commit_id, timestamp_str)

                    if write_info is None:
                        return None
                    ready_to_write = write_info[0]

                    while not ready_to_write:
                        queue_number = write_info[1]
                        sleep_time = (30 * int(queue_number))
                        time.sleep(sleep_time)
                        write_info = primary_handler.primary_write_queue(filename, commit_id, timestamp_str)

                        # In case file is deleted by previous action on queue
                        if write_info is None:
                            return None
                        ready_to_write = write_info[0]

                    lease_time = write_info[1]
                    file_data = write_info[2]

                    return lease_time, file_data
                else:
                    return None


        def exposed_extend_lease(self, filename, commit_id):
            if self.local_is_file_owned(filename):
                return self.local_extend_lease(filename, commit_id)
            else:
                con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
                directory = con.root.Directory()

                primary_handler_addr = directory.get_primary_for_file(filename)

                if primary_handler_addr != "None":
                    con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
                    primary_handler = con.root.Handler()

                    return primary_handler.primary_extend_lease(filename, commit_id)


        def exposed_write(self, filename, commit_id, data):
            if self.local_is_file_owned(filename):
                try:
                    return self.local_primary_write_commit(filename, commit_id, data)
                except ValueError as e:
                    raise e

            else:
                con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
                directory = con.root.Directory()

                primary_handler_addr = directory.get_primary_for_file(filename)

                if primary_handler_addr != "None":
                    con = rpyc.connect(host=primary_handler_addr[0], port=primary_handler_addr[1])
                    primary_handler = con.root.Handler()

                    try:
                        primary_handler.primary_write_commit(filename, commit_id, data)
                    except ValueError as e:
                        raise e

                # update local replica
                files_replicated_dir = FILES_DIR + UUID + REPLICATED
                with open(files_replicated_dir + str(filename), 'w') as f:
                    f.write(data)

        def exposed_primary_write_queue(self, filename, commit_id, timestamp):
            return self.local_primary_write_queue(filename, commit_id, timestamp)

        def exposed_primary_extend_lease(self, filename, commit_id):
            return self.local_extend_lease(filename, commit_id)

        def exposed_primary_write_commit(self, filename, commit_id, data):
            try:
                return self.local_primary_write_commit(filename, commit_id, data)
            except ValueError as e:
                raise e

        def local_primary_write_queue(self, filename, commit_id, timestamp):
            global LEASE_TIME

            config_object = ConfigParser()
            config_object.read_file(open(METADATA_DIR + UUID + '.conf'))
            on_lease_info = config_object["ON_LEASE"]

            file_exits = False
            for key, value in config_object.items('FILES_OWNED'):
                if key == filename:
                    file_exits = True

            lease_queue_entry = commit_id + "," + timestamp

            # If File not found; might have been deleted
            if not file_exits:
                return None
            else:
                # check if there is existing queue for file
                for key, value in config_object.items('ON_LEASE'):
                    if key == filename:
                        request_queue = list(value.split(';'))

                        # check if top can be removed?
                        top_request = request_queue[0]
                        request_info = top_request.split(',')
                        timestamp_str = request_info[1]
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')

                        current_time = datetime.now()
                        diff = math.floor((current_time - timestamp).total_seconds())

                        # Max Time that a request can be on top of queue
                        if diff >= 3000:
                            request_queue.remove(top_request)

                        new_value = ';'.join(map(str, request_queue))
                        on_lease_info[key] = new_value

                        with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                            config_object.write(conf)

                        # If commitId is on top
                        if request_queue[0] == lease_queue_entry:
                            request_info = request_queue[0].split(',')

                            self.insert_new_time(filename, commit_id)
                            files_owned_dir = FILES_DIR + UUID + OWNED
                            with open(files_owned_dir + str(filename), 'r') as f:
                                data = f.read()

                            return True, LEASE_TIME, data

                        # if commitId in on queue
                        elif lease_queue_entry in request_queue:
                            queue_position = request_queue.index(lease_queue_entry) + 1

                            return False, queue_position

                        # add commitId to queue
                        else:
                            request_queue.append(lease_queue_entry)
                            queue_position = request_queue.index(lease_queue_entry) + 1

                            new_value = ';'.join(map(str, request_queue))
                            on_lease_info[key] = new_value

                            with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                                config_object.write(conf)

                            return False, queue_position


                # No Lease Queue for fileName; create new; return true
                request_queue = list()
                request_queue.append(lease_queue_entry)

                new_value = ';'.join(map(str, request_queue))
                on_lease_info[filename] = new_value

                with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                    config_object.write(conf)

                files_owned_dir = FILES_DIR + UUID + OWNED
                with open(files_owned_dir + str(filename), 'r') as f:
                    data = f.read()

                return True, LEASE_TIME, data

        def local_extend_lease(self, filename, commit_id):
            global ALLOWED_EXTENSION_TIME

            config_object = ConfigParser()
            config_object.read_file(open(METADATA_DIR + UUID + '.conf'))
            on_lease_info = config_object["ON_LEASE"]

            for key, value in config_object.items('ON_LEASE'):
                if key == filename:
                    request_queue = list(value.split(';'))

                    top_request = request_queue[0]
                    request_info = top_request.split(',')

                    if commit_id != request_info[0]:
                        return False

                    # Only request on queue
                    if len(request_queue) == 1:
                        new_timestamp = datetime.now()
                        new_timestamp_str = new_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

                        new_top_request = commit_id + "," + new_timestamp_str

                        request_queue.remove(top_request)
                        request_queue.append(new_top_request)

                        new_value = ';'.join(map(str, request_queue))
                        on_lease_info[key] = new_value
                        with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                            config_object.write(conf)

                        return True

                    # It can still request to extend lease if it is less than 3 min old
                    else:
                        timestamp_str = request_info[1]
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')

                        current_time = datetime.now()
                        time_on_top = math.floor((current_time - timestamp).total_seconds())

                        if time_on_top <= ALLOWED_EXTENSION_TIME:
                            self.insert_new_time(filename, commit_id)
                            return True

                    return False

        def local_primary_write_commit(self, filename, commit_id, data):
            config_object = ConfigParser()
            config_object.read_file(open(METADATA_DIR + UUID + '.conf'))
            on_lease_info = config_object["ON_LEASE"]

            for key, value in config_object.items('ON_LEASE'):
                if key == filename:
                    request_queue = list(value.split(';'))

                    top_request = request_queue[0]
                    request_info = top_request.split(',')

                    # Commit Id does not Match Top request for file
                    if commit_id != request_info[0]:
                        raise ValueError("Commit Id does not match")

                    # rewrite file on primary
                    files_owned_dir = FILES_DIR + UUID + OWNED
                    with open(files_owned_dir + str(filename), 'w') as f:
                        f.write(data)

                    # remove request from queue
                    request_queue.remove(top_request)
                    new_value = ';'.join(map(str, request_queue))
                    on_lease_info[key] = new_value

                    with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                        config_object.write(conf)

        def insert_new_time(self, filename, commit_id):
            config_object = ConfigParser()
            config_object.read_file(open(METADATA_DIR + UUID + '.conf'))

            on_lease_info = config_object["ON_LEASE"]

            value = on_lease_info[filename]
            request_queue = list(value.split(';'))
            top_request = request_queue[0]

            new_timestamp = datetime.now()
            new_timestamp_str = new_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
            new_top_request = commit_id + "," + new_timestamp_str

            request_queue.remove(top_request)
            request_queue.insert(new_top_request)

            new_value = ';'.join(map(str, request_queue))
            on_lease_info[filename] = new_value

            with open(METADATA_DIR + UUID + '.conf', 'w') as conf:
                config_object.write(conf)

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
            files[filename] = ""

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

if __name__ == "__main__":
    startup()
