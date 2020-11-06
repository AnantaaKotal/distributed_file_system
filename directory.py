import rpyc
import random
import uuid
import threading
import math
import random
import configparser
import signal
import pickle
import sys
import os

from rpyc.utils.server import ThreadedServer


class DirectoryService(rpyc.Service):
    class exposed_Directory():
        file_table = set()
        handler_addr = []

        # Adds Handles Address to list upon registration
        def exposed_add_handler_address(self, handler_host, handler_port):
            try:
                self.handler_addr.index((handler_host, handler_port))
            except ValueError:
                self.handler_addr.append((handler_host, handler_port))

            print("Live handlers:")
            print(self.handler_addr)

        # Redirect client connection request to handler
        def exposed_connect_request_client(self):
            return random.choice(self.handler_addr)

        # update file_table on create
        def exposed_add_file(self, filename, handler_host, handler_port):
            self.file_table.add((filename, (handler_host, handler_port)))
            print(self.file_table)


if __name__ == "__main__":
    server = ThreadedServer(DirectoryService, port=12345)
    server.start()

