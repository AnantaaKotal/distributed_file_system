import rpyc
import string
import random
import pathlib
import time
import timeout_decorator
import os
from datetime import datetime

"""
This is an automated script to test read, write and append rates.

The rates can be tested with a byte counter or time counter
"""


#Directory Address
DIRECTORY_ADDR = 'localhost'
DIRECTORY_PORT = 12345

# backup_directory_address
BACKUP_DIRECTORY_ADDR = 'localhost'
BACKUP_DIRECTORY_PORT = 12346

FILENAME = "file1"
TEMP_DIR = str(pathlib.Path().absolute()) + "/tmp/"

COUNT = 0
BYTES = 0

def get_random_string():
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(9))
    return result_str

# Connect to Handler
def try_handler_connect():
    # Request Connection to Directory
    con_primary = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
    directory = con_primary.root.Directory()
    handler_addr = directory.connect_request_client()

    if handler_addr is None:
        return None
    else:
        print(handler_addr)
        return handler_addr

def read(handler):
    global BYTES
    try:
        data = handler.read(FILENAME)
        BYTES += len(data.encode('utf-16'))
        # print(data)
    except ValueError as e:
        print(e)


def write(handler):
    global COUNT
    global BYTES

    commit_id = get_random_string()

    # data to write
    new_data = "\nLine" + str(COUNT)
    bytes_size = len(new_data.encode('utf-16'))
    BYTES += bytes_size

    # Request to be put on queue
    request_info = handler.write_request(FILENAME, commit_id)
    write_info = request_info[0]
    ready_to_write = write_info[0]
    time_stamp = request_info[1]

    # Wait while not on top
    while not ready_to_write:
        queue_number = write_info[1]
        sleep_time = (10 * int(queue_number))
        time.sleep(sleep_time)
        request_info = handler.write_request(FILENAME, commit_id, timestamp_str=time_stamp)

        write_info = request_info[0]
        ready_to_write = write_info[0]
        time_stamp = request_info[1]

    file_data = write_info[2]

    if not os.path.exists(TEMP_DIR):
        os.mkdir(TEMP_DIR)

    with open(TEMP_DIR + str(FILENAME), 'w') as f:
        f.write(file_data)

    with open(TEMP_DIR + str(FILENAME), 'a') as f:
        f.write(new_data)

    with open(TEMP_DIR + str(FILENAME), 'r') as f:
        data = f.read()

    try:
        handler.write(FILENAME, commit_id, data)
    except ValueError as e:
        print("Error Writing")
        print(e)
        print("Try again later")


def append(handler):
    global COUNT
    global BYTES
    try:
        data = "\nLine" + str(COUNT)
        new_data = handler.append(FILENAME, data)

        bytes_size = len(data.encode('utf-16'))
        BYTES += bytes_size
    except ValueError as e:
        print(e)


@timeout_decorator.timeout(30)
def automate_read(handler):
    global COUNT

    while True:
        read(handler)
        COUNT += 1

def automate_write(handler):
    global COUNT

    for i in range(0, 500):
        write(handler)
        COUNT += 1

@timeout_decorator.timeout(30)
def automate_append(handler):
    global COUNT

    while True:
        append(handler)
        COUNT += 1


def time_counter():
    handler_addr = try_handler_connect()
    con_handler = rpyc.connect(host=handler_addr[0], port=handler_addr[1])
    handler = con_handler.root.Handler()

    start = datetime.now()
    print(start)
    automate_write(handler)
    stop = datetime.now()
    print(stop)
    con_handler.close()

    diff = (stop - start).total_seconds()
    print(diff)
    bytes_per_sec = BYTES/diff
    print(bytes_per_sec)
    kb_per_sec = bytes_per_sec * 0.001
    print(kb_per_sec)


def byte_counter():
    handler_addr = try_handler_connect()
    con_handler = rpyc.connect(host=handler_addr[0], port=handler_addr[1])
    handler = con_handler.root.Handler()

    try:
        # automate_read(handler)
        automate_append(handler)
    except timeout_decorator.timeout_decorator.TimeoutError:
        con_handler.close()
        print(BYTES)
        print(COUNT)
        byte = BYTES/10
        mb = byte * 0.001
        print(mb)

byte_counter()
# time_counter()