import rpyc
import random
import uuid
import threading
import math
import random
from configparser import ConfigParser
import signal
import pickle
import sys
import os
import pathlib
import string
import apscheduler
from apscheduler.schedulers import SchedulerAlreadyRunningError


from rpyc.utils.server import ThreadedServer

import sched, time

#Directory Address
DIRECTORY_ADDR = 'localhost'
DIRECTORY_PORT = 12345

BACKUP_CONFIG_DIR = str(pathlib.Path().absolute()) + "/config/backup/"

backup_scheduler = sched.scheduler(time.time, time.sleep)

def get_random_string():
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(8))
    return result_str


class BackupDirectoryService(rpyc.Service):
    class exposed_Directory():

            def exposed_close(self):
                print("Closing Service")
                server.close()

            # Adds Handles Address to list upon registration
            def exposed_add_handler_address(self, handler_host, handler_port):
                config_object = ConfigParser()
                config_object.read_file(open(BACKUP_CONFIG_DIR + 'handlr_addr.conf'))
                handler_addr = str(handler_host) + "," + str(handler_port)

                for key, value in config_object.items('HANDLERS'):
                    if key == handler_addr:
                        fname, isLive = value.split(',')
                        value = fname + ",Y"

                        handler_info = config_object["HANDLERS"]
                        handler_info[handler_addr] = value
                        with open(BACKUP_CONFIG_DIR + 'handlr_addr.conf', 'w') as conf:
                            config_object.write(conf)

                        return True, fname

                handler_info = config_object["HANDLERS"]
                uuid = get_random_string()
                handler_info[handler_addr] = uuid + ",Y"
                with open(BACKUP_CONFIG_DIR + 'handlr_addr.conf', 'w') as conf:
                    config_object.write(conf)

                return False, uuid

            # Redirect client connection request to handler
            def exposed_connect_request_client(self):
                handler_addr = self.get_live_handler()

                if handler_addr is None:
                    return None

                try:
                    con = rpyc.connect(handler_addr[0], handler_addr[1])
                    return handler_addr
                except ConnectionError:
                    # Mark handler as inactive
                    self.mark_handler_as_inactive(handler_addr[0], handler_addr[1])

                return self.exposed_connect_request_client()

            # update file_list.conf on create
            def exposed_add_file(self, filename, handler_host, handler_port):
                config_object = ConfigParser()
                config_object.read_file(open(BACKUP_CONFIG_DIR + 'file_list.conf'))

                handler_addr = str(handler_host) + "," + str(handler_port)
                for key, value in config_object.items('FILE_INFO'):
                    if key == filename:
                        return True

                file_info = config_object["FILE_INFO"]
                file_info[filename] = handler_addr
                with open(BACKUP_CONFIG_DIR + 'file_list.conf', 'w') as conf:
                    config_object.write(conf)
                return False

            def exposed_get_primary_from_dict(self, filename):
                return self.local_get_primary_from_dict(filename)

            # Exposed Function for Clients to get primary
            def exposed_get_primary_for_file(self, filename):
                while True:
                    addr = self.local_get_primary_from_dict(filename)

                    if addr == "None":
                        return "None"
                    host = addr[0]
                    port = addr[1]

                    # Check if Primary is active
                    if self.is_Handler_live(host, port):
                        # print("Handler Live")
                        try:
                            con = rpyc.connect(host, port)
                            con.close()
                            return host, port
                        except ConnectionError:
                            # print("Connection Error")
                            addr = self.reassign_primary(host, port, filename)
                            if addr is None:
                                return None
                            return addr
                    else:
                        # print("Handler Not live")
                        addr = self.reassign_primary(host, port, filename)
                        if addr is None:
                            return None
                        return addr

            # Removes File from record upon deletion
            def exposed_delete_file_from_record(self, filename):
                config_object = ConfigParser()
                config_object.read_file(open(BACKUP_CONFIG_DIR + 'file_list.conf'))

                config_object.remove_option('FILE_INFO', filename)

                with open(BACKUP_CONFIG_DIR + 'file_list.conf', 'w') as conf:
                    config_object.write(conf)

            # Returns Primary Handler Address from file_list.conf
            def local_get_primary_from_dict(self, filename):
                conf = ConfigParser()
                conf.read_file(open(BACKUP_CONFIG_DIR + 'file_list.conf'))

                file_list = list(conf.items('FILE_INFO'))

                for key, value in file_list:
                    if key == filename:
                        host, port = value.split(',')
                        return host, port

                return "None"

            def is_Handler_live(self, host, port):
                addr = host + "," + port

                conf_live = ConfigParser()
                conf_live.read_file(open(BACKUP_CONFIG_DIR + 'handlr_addr.conf'))
                handler_addr_list = list(conf_live.items('HANDLERS'))

                for key, value in handler_addr_list:
                    if key == addr:
                        fname, isLive = value.split(',')
                        if isLive == "Y":
                            return True

                return False

            # Returns a List of Live handlers
            def get_live_handler(self):
                conf_live = ConfigParser()
                conf_live.read_file(open(BACKUP_CONFIG_DIR + 'handlr_addr.conf'))
                handler_addr_list = list(conf_live.items('HANDLERS'))

                live_handlrs = []

                for key, value in handler_addr_list:
                    fname, isLive = value.split(',')
                    if isLive == "Y":
                        live_handlrs.append(key)

                if len(live_handlrs) == 0:
                    return None
                else:
                    handler_addr = random.choice(live_handlrs)
                    # print("Live handler" + str(handler_addr))

                    host, port = handler_addr.split(',')
                    return host, port

            # Reassigns Primary when Handler is inactive
            def reassign_primary(self, handler_host, handler_port, filename):
                # print("Reassigning primary")
                # Mark handler as inactive
                self.mark_handler_as_inactive(handler_host, handler_port)

                new_addr = self.reassign_file(filename)
                if new_addr is None:
                    return None

                config_primary = ConfigParser()
                config_primary.read_file(open(BACKUP_CONFIG_DIR + 'file_list.conf'))

                file_info = config_primary["FILE_INFO"]

                new_addr_str = str(new_addr[0]) + "," + str(new_addr[1])
                for fname, addr in config_primary.items('FILE_INFO'):
                    if fname == filename:
                        file_info[fname] = new_addr_str

                # Finally update config file with new primary
                with open(BACKUP_CONFIG_DIR + 'file_list.conf', 'w') as conf:
                    config_primary.write(conf)

                return new_addr

            # Finds a new primary Handler for file
            def reassign_file(self, filename):
                is_file_replicated = False
                i = 0

                # Tries 3 times
                while not is_file_replicated and i < 3:
                    host, port = self.get_live_handler()
                    con = rpyc.connect(host, port)
                    handler = con.root.Handler()
                    is_file_replicated = handler.is_file_replicated(filename)
                    # print("file replicated at:" + str(host) + str(port))
                    con.close()
                    i += 1

                if is_file_replicated:
                    con = rpyc.connect(host, port)
                    handler = con.root.Handler()
                    # print("Making Handler primary" + str(host) + str(port))
                    handler.make_primary(filename)
                    con.close()
                    return host, port
                else:
                    return None

            # Marks handler as inactive
            def mark_handler_as_inactive(self, handler_host, handler_port):
                handler_addr = str(handler_host) + "," + str(handler_port)

                config_object_inactive = ConfigParser()
                config_object_inactive.read_file(open(BACKUP_CONFIG_DIR + 'handlr_addr.conf'))

                for key, value in config_object_inactive.items('HANDLERS'):
                    if key == handler_addr:
                        fname, isLive = value.split(',')
                        value = fname + ",N"

                        handler_info = config_object_inactive["HANDLERS"]
                        handler_info[handler_addr] = value
                        with open(BACKUP_CONFIG_DIR + 'handlr_addr.conf', 'w') as conf:
                            config_object_inactive.write(conf)

            def exposed_get_from_backup(self):
                conf_backup_1 = ConfigParser()
                conf_backup_1.read_file(open(BACKUP_CONFIG_DIR + 'handlr_addr.conf'))
                handler_version_bk = int(conf_backup_1.get('VERSION', 'v'))

                conf_backup_2 = ConfigParser()
                conf_backup_2.read_file(open(BACKUP_CONFIG_DIR + 'file_list.conf'))
                file_list_version_bk = int(conf_backup_2.get('VERSION', 'v'))

                with open(BACKUP_CONFIG_DIR + 'handlr_addr.conf', 'r') as f:
                    data_1 = f.read()

                with open(BACKUP_CONFIG_DIR + 'file_list.conf', 'r') as f:
                    data_2 = f.read()

                return handler_version_bk, file_list_version_bk, data_1, data_2

            '''Code to append ip and socket details of the client
            s_ClientAdress, s_ClientPort=self._conn._config['endpoints'][1]
            '''

def get_data_from_main(sc):
    con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
    directory = con.root.Directory()
    data_1, data_2 = directory.backup()
    con.close()

    with open(BACKUP_CONFIG_DIR + 'handlr_addr.conf', 'w') as f:
        f.write(data_1)

    with open(BACKUP_CONFIG_DIR + 'file_list.conf', 'w') as f:
        f.write(data_2)

    print("Backed up..")

    # do your stuff
    s.enter(5, 1, get_data_from_main, (sc,))


if __name__ == "__main__":
    i = 0
    s = sched.scheduler(time.time, time.sleep)

    do_backup = True
    server = ThreadedServer(BackupDirectoryService, port=12346)

    while do_backup:
        try:
            print("Backing up..")
            s.enter(5, 1, get_data_from_main, (s,))
            s.run()
        except ConnectionError:
            print("Starting Service..")
            server.start()
        except KeyboardInterrupt:
            server.close()
            print("Bye")
            do_backup = False



