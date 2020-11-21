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

from rpyc.utils.server import ThreadedServer

CONFIG_DIR = str(pathlib.Path().absolute()) + "/config/"

file_table = set()


def get_random_string():
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(8))
    return result_str


class DirectoryService(rpyc.Service):
    class exposed_Directory():
        # Adds Handles Address to list upon registration
        def exposed_add_handler_address(self, handler_host, handler_port):
            config_object = ConfigParser()
            config_object.read_file(open(CONFIG_DIR + 'handlr_addr.conf'))
            handler_addr = str(handler_host) + "," + str(handler_port)

            for key, value in config_object.items('HANDLERS'):
                if key == handler_addr:
                    fname, isLive = value.split(',')
                    value = fname + ",Y"

                    handler_info = config_object["HANDLERS"]
                    handler_info[handler_addr] = value
                    with open(CONFIG_DIR + 'handlr_addr.conf', 'w') as conf:
                        config_object.write(conf)

                    return True, fname

            handler_info = config_object["HANDLERS"]
            uuid = get_random_string()
            handler_info[handler_addr] = uuid + ",Y"
            with open(CONFIG_DIR + 'handlr_addr.conf', 'w') as conf:
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
            config_object.read_file(open(CONFIG_DIR + 'file_list.conf'))

            handler_addr = str(handler_host) + "," + str(handler_port)
            for key, value in config_object.items('FILE_INFO'):
                if key == filename:
                    return True

            file_info = config_object["FILE_INFO"]
            file_info[filename] = handler_addr
            with open(CONFIG_DIR + 'file_list.conf', 'w') as conf:
                config_object.write(conf)
            return False

        # Exposed Function for Clients to get primary
        def exposed_get_primary_for_file(self, filename):
            return self.local_get_primary_for_file(filename)

        #Removes File from record upon deletion
        def exposed_delete_file_from_record(self,filename):
            config_object = ConfigParser()
            config_object.read_file(open(CONFIG_DIR + 'file_list.conf'))

            config_object.remove_option('FILE_INFO', filename)

            with open(CONFIG_DIR + 'file_list.conf', 'w') as conf:
                config_object.write(conf)

        # returns the primary for file; reassigns inactive primary if required.
        def local_get_primary_for_file(self, filename):
            handlr_addr = self.get_primary_from_dict(filename)

            # FILE NOT FOUND
            if handlr_addr == "None":
                return "None"

            host = handlr_addr[0]
            port = handlr_addr[1]

            # Check if Primary is active
            try:
                con = rpyc.connect(host, port)
                return host, port
            except ConnectionError:
                self.reassign_primary(host, port)
                return self.local_get_primary_for_file(filename)

        # Returns Primary Handler Address from file_list.conf
        def get_primary_from_dict(self, filename):
            conf = ConfigParser()
            conf.read_file(open(CONFIG_DIR + 'file_list.conf'))

            file_list = list(conf.items('FILE_INFO'))

            for key,value in file_list:
                if key == filename:
                    host, port = value.split(',')
                    return host, port

            return "None"

        # Returns a List of Live handlers
        def get_live_handler(self):
            conf = ConfigParser()
            conf.read_file(open(CONFIG_DIR + 'handlr_addr.conf'))
            handler_addr_list = list(conf.items('HANDLERS'))

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
        def reassign_primary(self, handler_host, handler_port):
            # Mark handler as inactive
            self.mark_handler_as_inactive(handler_host, handler_port)

            # Reassign the files owned
            handler_addr = str(handler_host) + "," + str(handler_port)

            config = ConfigParser()
            config.read_file(open(CONFIG_DIR + 'file_list.conf'))

            file_info = config["FILE_INFO"]

            for fname, addr in config.items('FILE_INFO'):
                if addr == handler_addr:
                    new_addr = self.reassign_file(fname)
                    file_info[fname] = new_addr

            # Finally update config file with new primary
            with open(CONFIG_DIR + 'file_list.conf', 'w') as conf:
                config.write(conf)


        # Finds a new primary Handler for file
        def reassign_file(self, filename):
            host, port = self.get_live_handler()
            con = rpyc.connect(host, port)
            handler = con.root.Handler()

            if handler.is_file_replicated(filename):
                handler.make_primary(filename)
                return host, port
            else:
                return self.reassign_file(filename)

        # Marks handler as inactive
        def mark_handler_as_inactive(self, handler_host, handler_port):
            handler_addr = str(handler_host) + "," + str(handler_port)

            config_object = ConfigParser()
            config_object.read_file(open(CONFIG_DIR + 'handlr_addr.conf'))

            for key, value in config_object.items('HANDLERS'):
                if key == handler_addr:
                    fname, isLive = value.split(',')
                    value = fname + ",N"

                    handler_info = config_object["HANDLERS"]
                    handler_info[handler_addr] = value
                    with open(CONFIG_DIR + 'handlr_addr.conf', 'w') as conf:
                        config_object.write(conf)


        '''Code to append ip and socket details of the client
        s_ClientAdress, s_ClientPort=self._conn._config['endpoints'][1]
        '''

if __name__ == "__main__":
    server = ThreadedServer(DirectoryService, port=12345)
    server.start()

