from core import env
import flopy
import numpy as np
import pandas as pd
import timeit
import time
import os
import users.MH.Waimak_modeling.model_tools as mt
from users.MH.Waimak_modeling.supporting_data_path import base_mod_dir
import multiprocessing
import logging
import sys


def mp_func(kwargs):
    temp = test_fun(**kwargs)
    return temp


def test_fun(name, day=1, method=None):
    return '{} {} {}'.format(name,day,method)

if __name__ == '__main__':
    runs = []
    for i in range(8):
        temp = {'name': 'test',
                'day' : i,
                'method': 'trial'}
        runs.append(temp)
    multiprocessing.log_to_stderr(logging.DEBUG)
    pool_size = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=pool_size)
    pool_outputs = pool.map(mp_func, runs)
    print(pool_outputs)
    pool.close()  # no more tasks
    pool.join()
