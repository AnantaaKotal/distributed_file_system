from datetime import datetime
import time
import math
from configparser import ConfigParser
import webbrowser
import os
import subprocess
import timeout_decorator

path = '/Users/anantaa/Desktop/python/dist_sys/config/metadata/iyyjzmzz.conf'
file_section = 'FILES_OWNED'
file_name = 'file9'

def date():
    old_time = datetime.now()
    old_time_str = old_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    print(old_time_str)
    time.sleep(10)
    replicated_time = datetime.now()
    old_time_fmt = datetime.strptime(old_time_str, '%Y-%m-%d %H:%M:%S.%f')

    diff = math.floor((replicated_time - old_time_fmt).total_seconds())
    if diff <= 10:
        print(diff)

def config_delete():
    config_object = ConfigParser()
    config_object.read(path)

    config_object.remove_option(file_section, file_name)

    with open(path, 'w') as conf:
        config_object.write(conf)

def file_open():
    p = subprocess.call(['open', '-a', 'TextEdit', "/Users/anantaa/Desktop/python/dist_sys/config/handlr_addr.conf"])
    print("You done?")

def list_concat():
    my_lst = [1]
    my_lst.insert(0, 6)
    my_lst_str = ';'.join(map(str, my_lst))

    print(my_lst_str)


import signal
USER_INPUT_TIMEOUT = 5 # number of seconds your want for timeout


def interrupt(signum, frame):
    print("Didn't recieve user input.")


@timeout_decorator.timeout(5, timeout_exception="time ended for the lease")
def timed_input():
    s = input('Ready to commit?')
    return s



def input_sleep_try():
    try:
        input = timed_input()
        print(input)
    except:
        print("\ncommitting..")

"""import sched, time
s = sched.scheduler(time.time, time.sleep)

def do_something(sc):
    print("Doing stuff...")
    # do your stuff
    s.enter(10, 1, do_something, (sc,))

s.enter(10, 1, do_something, (s,))
s.run()

# input_sleep_try()"""

def append():
    with open("temp.txt", "a") as file: # append mode
        file.write(" data")

def write():
    with open("temp.txt", "w") as file: # append mode
        file.write(" data")


def has_option():
    config_object = ConfigParser()
    config_object.read(path)

    if config_object.has_option(file_section, file_name):
        file_version = config_object.get(file_section, file_name)

        if file_version == "":
            file_version = 1
        else:
            file_version = int(file_version)
            file_version += 1

        file_info = config_object[file_section]
        file_info[file_name] = str(file_version)

        with open(path, 'w') as conf:
            config_object.write(conf)
    else:
        raise ValueError

has_option()