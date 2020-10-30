import rpyc
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

def set_conf():
  conf=configparser.ConfigParser()
  conf.read('dfs.conf')
  PrimaryService.exposed_Primary.block_size = int(conf.get('master','block_size'))


class PrimaryService(rpyc.Service):
    class exposed_Primary():
        file_table = {}
        block_mapping = {}
        handlers = {}

        block_size = 0
        replication_factor = 0

        def exposed_read(self, fname):
            mapping = self.__class__.file_table[fname]
            return mapping

        def exposed_write(self,dest,size):
            if self.exists(dest):
                pass # ignoring for now, will write it later

            self.__class__.file_table[dest] =[]

            num_blocks = self.calc_num_blocks(size)
            blocks = self.alloc_blocks(dest,num_blocks)

            return blocks

        def calc_num_blocks(self, size):
            return int(math.ceil(float(size) / self.__class__.block_size))

        def exists(self, file):
            return file in self.__class__.file_table

        def alloc_blocks(self, dest, num):
            blocks = []
            for i in range(0, num):
                block_uuid = uuid.uuid1()
                nodes_ids = random.sample(self.__class__.handlers.keys(), self.__class__.replication_factor)
                blocks.append((block_uuid, nodes_ids))

                self.__class__.file_table[dest].append((block_uuid, nodes_ids))
            return blocks

