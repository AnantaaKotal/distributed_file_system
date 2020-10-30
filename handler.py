import rpyc
import os

from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/handler/"


class HandlerService(rpyc.Service):
    class exposed_Handler():
        blocks = {}

        def exposed_write(self,block_uuid,data):
            with open(DATA_DIR + str(block_uuid), 'w') as f:
                f.write(data)

        def exposed_read(self,block_uuid):
            block_addr = DATA_DIR + str(block_uuid)
            if not os.path.isfile(block_addr):
                return None
            with open(block_addr) as f:
                return f.read()


if __name__ == "__main__":
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    t = ThreadedServer(HandlerService, port=8888)
    t.start()
