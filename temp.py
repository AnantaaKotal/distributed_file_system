from datetime import datetime
import time
import math
from configparser import ConfigParser
import webbrowser
import os
import subprocess

path = '/Users/anantaa/Desktop/python/dist_sys/config/metadata/iyyjzmzz.conf'
file_section = 'FILES_REPLICATED'
file_name = 'file1'

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


list_concat()
