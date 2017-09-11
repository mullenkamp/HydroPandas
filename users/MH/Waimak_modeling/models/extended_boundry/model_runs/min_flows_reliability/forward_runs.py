# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 8/09/2017 11:54 AM
"""

from __future__ import division
from core import env
from base_forward_runs import setup_run_forward_run_mp
import os
import multiprocessing
import logging
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import time
from copy import deepcopy
import itertools


def run_cc_senarios(base_kwargs):
    runs = []
    base_kwargs = deepcopy(base_kwargs)
    periods = range(2010, 2100, 10)  # todo how many itterations is this! 720! talk to zeb about this
    rcms = ['BCC-CSM1.1', 'CESM1-CAM5', 'GFDL-CM3', 'GISS-EL-R', 'HadGEM2-ES', 'NorESM1-M']
    rcps = ['RCP2.6', 'RCP4.5', 'RCP6.0', 'RCP8.5']
    amalg_types = ['tym', 'min', 'low_3_m']
    for per, rcm, rcp, at in itertools.product(periods, rcms, rcps, amalg_types):
        temp = deepcopy(base_kwargs)
        temp['cc_inputs'] = {'rmc': rcm, 'rcp': rcp, 'period': per, 'amag_type': at}
        temp['name'] = '{}_{}_{}_{}_{}'.format(temp['name'], rcm, rcp, per, at)
        runs.append(temp)
    return runs


def setup_run_args(model_id, forward_run_dir):
    runs = []
    # naturalised
    nat = {
        'model_id': model_id,
        'name': 'naturalised',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 1,
        'naturalised': True,
        'full_abs': False,
        'pumping_well_scale': 1
    }
    runs.append(nat)

    # nat + cc
    runs.extend(run_cc_senarios(nat))

    # full abstration
    full_abs = {
        'model_id': model_id,
        'name': 'full_abs',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': True,
        'pumping_well_scale': 1
    }
    runs.append(full_abs)

    # full allocation (full abstraction)
    full_abs_allo = {
        'model_id': model_id,
        'name': 'full_abs_allo',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': True,
        'pumping_well_scale': 1,
        'full_allo': True
    }
    runs.append(full_abs_allo)

    # base model run
    current = {
        'model_id': model_id,
        'name': 'current',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1
    }
    runs.append(current)

    # climate change
    runs.extend(run_cc_senarios(current))

    # pc5
    pc5 = { #todo should pumping be altered by PC5?
        'model_id': model_id,
        'name': 'pc5',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': True,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1
    }
    runs.append(pc5)

    # WIL efficiency
    will_eff = {
        'model_id': model_id,
        'name': 'wil_eff',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 0,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1
    }
    runs.append(will_eff)

    # pc5 + will efficency
    pc5_will_eff = {
        'model_id': model_id,
        'name': 'pc5_wil_eff',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': True,
        'wil_eff': 0,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1
    }
    runs.append(pc5_will_eff)

    # climate change + pc5 + will efficieny
    runs.extend(run_cc_senarios(pc5_will_eff))

    for i in runs:
        i['base_dir'] = '{}/{}'.format(forward_run_dir, i['name'])
    # todo check everything carefully
    return runs


def start_process():
    print('Starting', multiprocessing.current_process().name)


def run_forward_runs():
    t = time.time()
    multiprocessing.log_to_stderr(logging.DEBUG)
    runs = setup_run_args()
    pool_size = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=pool_size,
                                initializer=start_process,
                                )
    pool_outputs = pool.map(setup_run_forward_run_mp, runs)
    pool.close()  # no more tasks
    pool.join()
    with open("{}/forward_run_log/forward_run_status.txt".format(smt.sdp), 'w') as f:
        wr = ['{}: {}\n'.format(e[0], e[1]) for e in pool_outputs]
        f.writelines(wr)
    print('{} runs completed in {} minutes'.format(len(runs), ((time.time() - t) / 60)))


    # todo debug

if __name__ == '__main__':
    runs = setup_run_args('test','test')
    print('done')