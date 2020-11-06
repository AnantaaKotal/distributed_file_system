import rpyc
import sys
import os
import logging

log = logging.getLogger(__name__)


def connect_to_handler(block_uuid, handler):
    handler_addr = handler[0]
    host, port = handler_addr

    con = rpyc.connect(host, port=port)
    handler = con.root.Handler()

    return handler

def main():
    con = rpyc.connect("localhost", port=12345)
    primary = con.root.Primary(1, 2)

    print(primary.foo())
    print(".")


if __name__ == "__main__":
  main()




