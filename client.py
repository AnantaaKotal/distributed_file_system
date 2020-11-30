import rpyc
import sys
import string
import random
import os
import pathlib
import subprocess
import time
import signal
import timeout_decorator

#Directory Address
DIRECTORY_ADDR = 'localhost'
DIRECTORY_PORT = 12346

# backup_directory_address
BACKUP_DIRECTORY_ADDR = 'localhost'
BACKUP_DIRECTORY_PORT = 12346

TEMP_DIR = str(pathlib.Path().absolute()) + "/tmp/"

USER_INPUT_TIMEOUT = 5
LEASE_TIME = 30


def get_random_string():
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(9))
    return result_str

def create_file(handler, filename, data):
    try:
        handler.create(filename, data)
    except ValueError as e:
        print("File Name Exists; Try another File Name")


def read_file(handler, filename):
    try:
        data = handler.read(filename)
        print(data)
    except ValueError as e:
        print(e)


def write_file(handler, filename, commit_id):
    with open(TEMP_DIR + str(filename), 'r') as f:
        data = f.read()
    try:
        handler.write(filename, commit_id, data)
    except ValueError as e:
        print("Error Writing")
        print(e)
        print("Try again later")


@timeout_decorator.timeout(5)
def timed_input(input_str=None):
    if input_str is not None:
        print(input_str)

    s = input("Would you like to extend lease? ")
    return s

@timeout_decorator.timeout(LEASE_TIME)
def timed_write():
    s = input("Press any key when done..")
    return


def timed_commit(handler, filename, commit_id, input_str=None):
    try:
        user_input = timed_input(input_str)

        if user_input[0] == "Y":
            print("Asking to extend lease..")
            can_extend, lease_time = handler.extend_lease(filename, commit_id)
            print(can_extend, lease_time)
            if can_extend:
                print("Request to extend lease granted..")
                print("Continue writing..")
                try:
                    timed_write()
                    timed_commit(handler, filename, commit_id)
                except:
                    timed_commit(handler, filename, commit_id, input_str="Your time is up")
            else:
                print("Lease extension was denied..")
                print("Committing..")
                write_file(handler, filename, commit_id)
        else:
            print("User didn't ask for lease..")
            print("Committing..")
            write_file(handler, filename, commit_id)
    except timeout_decorator.timeout_decorator.TimeoutError:
        print("User response not found..")
        print("Committing..")
        write_file(handler, filename, commit_id)


def write(handler, filename):
    global LEASE_TIME
    print("Please wait while others finish writing...")
    commit_id = get_random_string()

    request_info = handler.write_request(filename, commit_id)
    if request_info is None:
        print("File not Found")
        return

    write_info = request_info[0]
    time_stamp = request_info[1]
    lease_time = LEASE_TIME

    ready_to_write = write_info[0]
    while not ready_to_write:
        queue_number = write_info[1]
        sleep_time = (LEASE_TIME * int(queue_number))
        time.sleep(sleep_time)
        request_info = handler.write_request(filename, commit_id, timestamp_str=time_stamp)

        # In case file is deleted by previous action on queue
        if request_info is None:
            print("File not Found")
            return

        write_info = request_info[0]
        ready_to_write = write_info[0]
        time_stamp = request_info[1]

    LEASE_TIME = write_info[1] - 5
    file_data = write_info[2]

    if not os.path.exists(TEMP_DIR):
        os.mkdir(TEMP_DIR)

    with open(TEMP_DIR + str(filename), 'w') as f:
        f.write(file_data)

    print("*****************WRITE***********************")
    print("Opening file for write...")
    print("File will auto commit after %s seconds; unless you ask for extension." % (str(LEASE_TIME)))

    p = subprocess.call(['open', '-a', 'TextEdit', TEMP_DIR + str(filename)])

    try:
        timed_write()
        timed_commit(handler, filename, commit_id)
    except:
        timed_commit(handler, filename, commit_id, input_str="\nYour time is up")


def delete(handler, filename):
    global LEASE_TIME
    print("Please wait while others finish writing...")
    commit_id = get_random_string()

    request_info = handler.write_request(filename, commit_id)
    if request_info is None:
        print("File not Found")
        return

    write_info = request_info[0]
    time_stamp = request_info[1]
    lease_time = LEASE_TIME

    ready_to_write = write_info[0]
    while not ready_to_write:
        queue_number = write_info[1]
        sleep_time = (LEASE_TIME * int(queue_number))
        time.sleep(sleep_time)
        request_info = handler.write_request(filename, commit_id, timestamp_str=time_stamp)

        # In case file is deleted by previous action on queue
        if request_info is None:
            print("File not Found")
            return

        write_info = request_info[0]
        ready_to_write = write_info[0]
        time_stamp = request_info[1]

    try:
        handler.delete(filename, commit_id)
        print("Deleted %s" % (filename))

        if os.path.exists(TEMP_DIR + str(filename)):
            os.remove(TEMP_DIR + str(filename))
    except ValueError:
        print("File Not found")

# Append to file
def append(handler, filename, data):
    try:
        new_data = handler.append(filename, data)
        print(new_data)
    except ValueError as e:
        print("File not found")

# Optimistic write
def overwrite(handler, filename):
    try:
        data, version_id = handler.optimistic_write_request(filename)

        # Creating temp file on Client machine
        if not os.path.exists(TEMP_DIR):
            os.mkdir(TEMP_DIR)

        with open(TEMP_DIR + str(filename), 'w') as f:
            f.write(data)

        print("*****************OPTIMISTIC WRITE***********************")
        p = subprocess.call(['open', '-a', 'TextEdit', TEMP_DIR + str(filename)])

        keyboard_interrupt = input("Press any key when done..")

        with open(TEMP_DIR + str(filename), 'r') as f:
            data = f.read()

        new_version_id = version_id + 1

        try:
            can_write = handler.optimistic_write_commit(filename, new_version_id, data)

            if can_write:
                print("Write completed successfully")
            else:
                print("Your file version is out of data")
                print("Please sync before writing")
        except ValueError as e:
            print("Error Writing")
            print(e)
            print("Try again later")
    except ValueError as e:
        print("Error writing")

# Connect to Directory
def directory_connect():
    try:
        con = rpyc.connect(DIRECTORY_ADDR, port=DIRECTORY_PORT)
        return con
    except ConnectionError:
        con = rpyc.connect(BACKUP_DIRECTORY_ADDR, port=BACKUP_DIRECTORY_PORT)
        return con

# Connect to Handler
def try_handler_connect():
    # Request Connection to Directory
    con_primary = directory_connect()
    directory = con_primary.root.Directory()
    handler_addr = directory.connect_request_client()

    if handler_addr is None:
        return None
    else:
        print(handler_addr)
        con_handler = rpyc.connect(host=handler_addr[0], port=handler_addr[1])
        handler = con_handler.root.Handler()
        return handler, handler_addr




def main():
    handler_obj = try_handler_connect()

    if handler_obj is None:
        print("No Live Server Found")
    else:
        handler = handler_obj[0]
        handler_addr = handler_obj[1]

        print("Connected to" + str(handler_addr[0]) + ":" + str(handler_addr[1]))
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        take_input = True
        while (take_input):
            arg = input("What would you like to do next? ")

            args = arg.split(" ")
            if args[0] == "exit":
                take_input = False
            # Handle Client operation
            elif args[0] == "create":
                create_file(handler, filename=args[1], data=args[2])
            elif args[0] == "read":
                read_file(handler, filename=args[1])
            elif args[0] == "write":
                write(handler, filename=args[1])
            elif args[0] == "delete":
                delete(handler, filename=args[1])
            elif args[0] == "append":
                append(handler, filename=args[1], data=args[2])
            elif args[0] == "overwrite":
                overwrite(handler, filename=args[1])
            else:
                print("Error reading client request")


if __name__ == "__main__":
    #main(sys.argv[1:])
    main()

