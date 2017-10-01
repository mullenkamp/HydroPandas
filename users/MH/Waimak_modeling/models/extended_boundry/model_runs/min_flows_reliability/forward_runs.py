# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 8/09/2017 11:54 AM
"""

from __future__ import division
from core import env
from base_forward_runs import setup_run_forward_run_mp, setup_run_forward_run
import os
import multiprocessing
import logging
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import time
from copy import deepcopy
import itertools
from future.builtins import input
import datetime
import psutil

def run_cc_senarios(base_kwargs, cc_to_waimak_only=False):
    runs = []
    base_kwargs = deepcopy(base_kwargs)
    periods = range(2010, 2100, 20)
    rcms = ['BCC-CSM1.1', 'CESM1-CAM5', 'GFDL-CM3', 'GISS-EL-R', 'HadGEM2-ES', 'NorESM1-M']
    rcps = ['RCP4.5', 'RCP8.5']
    amalg_types = ['tym', 'low_3_m'] # removed min as most low_3_yr were not converging
    for per, rcm, rcp, at in itertools.product(periods, rcms, rcps, amalg_types):
        temp = deepcopy(base_kwargs)
        temp['cc_inputs'] = {'rcm': rcm, 'rcp': rcp, 'period': per, 'amag_type': at, 'cc_to_waimak_only':cc_to_waimak_only}
        temp['name'] = '{}_{}_{}_{}_{}'.format(temp['name'], rcm, rcp, per, at)
        runs.append(temp)
    for rcm, at in itertools.product(rcms, amalg_types):
        per = 1980
        rcp = 'RCPpast'
        temp = deepcopy(base_kwargs)
        temp['cc_inputs'] = {'rcm': rcm, 'rcp': rcp, 'period': per, 'amag_type': at}
        temp['name'] = '{}_{}_{}_{}_{}'.format(temp['name'], rcm, rcp, per, at)
        runs.append(temp)
    return runs


def setup_run_args(model_id, forward_run_dir, cc_to_waimak_only=False):
    """
    set up the forward runs
    :param model_id: which MC realisation to use
    :param forward_run_dir: the dir that all of teh model directories will be placed
    :return:
    """
    runs = []
    # base model run
    mod_per = {
        'model_id': model_id,
        'name': 'mod_period',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'org_pumping_wells': True
    }
    runs.append(mod_per)
    # base model run with 2015-2016 pumping
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

    # full allocation current usage
    full_allo = {
        'model_id': model_id,
        'name': 'full_allo_cur_use',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'full_allo': True
    }
    runs.append(full_allo)


    # pc5
    pc5_80 = {
        'model_id': model_id,
        'name': 'pc5_80',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': True,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'org_efficency': 80
    }
    runs.append(pc5_80)

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
    pc5_80_will_eff = {
        'model_id': model_id,
        'name': 'pc5_80_wil_eff',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': True,
        'wil_eff': 0,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'org_efficency': 80
    }
    runs.append(pc5_80_will_eff)

    # climate change senarios (lots of runs)
    # nat + cc
    runs.extend(run_cc_senarios(nat, cc_to_waimak_only=cc_to_waimak_only))

    # climate change
    runs.extend(run_cc_senarios(current, cc_to_waimak_only=cc_to_waimak_only))

    # climate change + pc5 + will efficieny
    runs.extend(run_cc_senarios(pc5_80_will_eff, cc_to_waimak_only=cc_to_waimak_only))

    if not os.path.exists(forward_run_dir):
        os.makedirs(forward_run_dir)

    for i in runs:
        i['base_dir'] = '{}/{}'.format(forward_run_dir, i['name'])
    return runs


def start_process():
    print('Starting', multiprocessing.current_process().name)
    p = psutil.Process(os.getpid())
    # set to lowest priority, this is windows only, on Unix use ps.nice(19)
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)


def run_forward_runs(runs,forward_run_dir, notes=None):
    with open(os.path.join(forward_run_dir,'READ_ME.txt'),'w') as f:
        f.write(str(notes) + '\n')

    t = time.time()
    multiprocessing.log_to_stderr(logging.DEBUG)
    pool_size = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=pool_size,
                                initializer=start_process,
                                )
    pool_outputs = pool.map(setup_run_forward_run_mp, runs)
    pool.close()  # no more tasks
    pool.join()
    now = datetime.datetime.now()
    with open("{}/forward_run_log/forward_run_status_{}_{:02d}_{:02d}_{:02d}_{:02d}.txt".format(smt.sdp,now.year,now.month,now.day,now.hour,now.minute), 'w') as f:
        f.write(str(notes) + '\n')
        wr = ['{}: {}\n'.format(e[0], e[1]) for e in pool_outputs]
        f.writelines(wr)
    print('{} runs completed in {} minutes'.format(len(runs), ((time.time() - t) / 60)))



if __name__ == '__main__':
    safemode = True
    #todo define the two below before each run
    dir_path = r"D:\mh_model_runs\forward_runs_2017_09_30" # path on rdsprod03
    notes = """ 
    LSR senario changes applied to full domain, CC component of LSR changes applied to only waimakariri
    pumping changes only applied to Waimakariri,  trying again with some runs that didn't complete [36,56,84]
    run in {}
    """.format(dir_path)
    if safemode:
        if os.path.exists(dir_path):
            cont = input(
                'run all forward runs, this could overwrite item in :\n {} \n continue y/n\n'.format(dir_path)).lower()
            if cont != 'y':
                raise ValueError('script aborted so as not to potentially overwrite {}'.format(dir_path))
    runs = setup_run_args('opt',dir_path,cc_to_waimak_only=True)
    import time
    t = time.time()
    missing_runs = [35,36,49, 56,77,78,84,233,240]
    missing_runs = [36,56]
    runs = [runs[e] for e in missing_runs]
    for run in runs:
        print run
        setup_run_forward_run(**run)
    #run_forward_runs(runs,dir_path,notes)
    print('{} runs in __ min'.format(len(runs)))
    print (time.time() - t)/60
    print('done')