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



