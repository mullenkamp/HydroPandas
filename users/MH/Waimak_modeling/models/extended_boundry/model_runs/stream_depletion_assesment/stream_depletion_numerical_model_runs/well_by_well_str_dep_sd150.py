"""
Author: matth
Date Created: 07/09/2017 4:27 PM
"""

from __future__ import division
from users.MH.Waimak_modeling.supporting_data_path import base_mod_dir
import os
import multiprocessing
import logging
from stream_depletion_model_setup import setup_and_run_stream_dep, setup_and_run_stream_dep_multip
from copy import deepcopy
import time
from starting_hds_ss_sy import get_starting_heads_sd150, get_ss_sy, get_sd_well_list
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import psutil
import datetime
from future.builtins import input


def setup_runs_sd150(model_id, well_list, base_path, ss, sy, start_heads):
    """
    sets up the model runs for stream depletion 150 day assessment
    :param model_id: the NSMC realisation to use
    :param well_list: the list of wells to assess stream depletion one model will be run for each well
    :param base_path: the path to put all of the folders containing each well model
    :param ss: specific storage for the model (either integer or k,i,j array)
    :param sy: specific yield for the model (either integer or k,i,j array)
    :param start_heads: the starting heads for the model (k,i,j array)
    :return:
    """
    spv = {'nper': 5,
           'perlen': 30,
           'nstp': 2,
           'steady': [False, False, False, False, False],
           'tsmult': 1.1}

    if not os.path.exists(base_path):
        os.makedirs(base_path)
    log_dir = '{}/logging'.format(base_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if os.path.exists('{}/error_log.txt'.format(base_path)):
        os.remove('{}/error_log.txt'.format(base_path))

    base_kwargs = {
        'model_id': model_id,
        'name': None,
        'base_dir': base_path,
        'stress_vals': spv,
        'wells_to_turn_on': {0: []},
        'ss': ss,
        'sy': sy,
        'silent': True,
        'start_heads': start_heads,
        'sd_7_150': 'sd7'}

    out_runs = []
    for well in well_list:
        temp_kwargs = deepcopy(base_kwargs)
        temp_kwargs['wells_to_turn_on'][1] = [well]
        temp_kwargs['name'] = 'turn_on_{}_sd30'.format(well.replace('/', '_'))
        out_runs.append(temp_kwargs)

    return out_runs


def start_process():
    """
    ids which model is running
    :return:
    """
    print('Starting', multiprocessing.current_process().name)
    p = psutil.Process(os.getpid())
    # set to lowest priority, this is windows only, on Unix use ps.nice(19)
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)


def well_by_well_depletion_sd150(model_id, well_list, base_path, notes):
    """
    run the well by well depletion for the 150 day stream depletion
    :param model_id: the NSMC realisation to use
    :param well_list: the list of wells to assess stream depletion one model will be run for each well
    :param base_path: the path to put all of the folders containing each well model
    :return:
    """
    if os.path.exists(base_path):
        cont = input("the base path already exists: \n {}\n do you want to continue y/n\n".format(base_path))
        if cont.lower() != 'y':
            raise KeyboardInterrupt('run  stopped to prevent overwrite of {}'.format(base_path))

    t = time.time()
    ss,sy = get_ss_sy()
    start_heads = get_starting_heads_sd150()
    multiprocessing.log_to_stderr(logging.DEBUG)
    runs = setup_runs_sd150(model_id, well_list, base_path, ss, sy, start_heads)
    pool_size = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=pool_size,
                                initializer=start_process,
                                )
    pool_outputs = pool.map(setup_and_run_stream_dep_multip, runs)
    pool.close()  # no more tasks
    pool.join()
    now = datetime.datetime.now()
    with open("{}/forward_run_log/SD7_run_status_{}_{:02d}_{:02d}_{:02d}_{:02d}.txt".format(smt.sdp,now.year,now.month,now.day,now.hour,now.minute), 'w') as f:
        f.write(str(notes) + '\n')
        wr = ['{}: {}\n'.format(e[0], e[1]) for e in pool_outputs]
        f.writelines(wr)
    print('{} runs completed in {} minutes'.format(len(well_list), ((time.time() - t) / 60)))


if __name__ == '__main__':
    print('done')  # todo this needs debugging
