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
    """
    set up a sweet of climate change runs from a base run
    :param base_kwargs: the base run to run through climate change senairos
    :param cc_to_waimak_only: boolean if True then only apply the LSR changes to the waimakariri esle apply to whole
                              model domain pumping is only applied to waimakariri as there is too much uncertainty in
                              teh selwyn due to central plains
    :return: list of runs
    """
    runs = []
    base_kwargs = deepcopy(base_kwargs)
    periods = range(2010, 2100, 20)
    rcms = ['BCC-CSM1.1', 'CESM1-CAM5', 'GFDL-CM3', 'GISS-EL-R', 'HadGEM2-ES', 'NorESM1-M']
    rcps = ['RCP4.5', 'RCP8.5']
    amalg_types = ['tym', 'low_3_m']  # removed min as most low_3_yr were not converging
    for per, rcm, rcp, at in itertools.product(periods, rcms, rcps, amalg_types):
        temp = deepcopy(base_kwargs)
        temp['cc_inputs'] = {'rcm': rcm, 'rcp': rcp, 'period': per, 'amag_type': at,
                             'cc_to_waimak_only': cc_to_waimak_only}
        temp['name'] = '{}_{}_{}_{}_{}'.format(temp['name'], rcm, rcp, per, at)
        runs.append(temp)
    for rcm, at in itertools.product(rcms, amalg_types):
        per = 1980
        rcp = 'RCPpast'
        temp = deepcopy(base_kwargs)
        temp['cc_inputs'] = {'rcm': rcm, 'rcp': rcp, 'period': per, 'amag_type': at,
                             'cc_to_waimak_only': cc_to_waimak_only}
        temp['name'] = '{}_{}_{}_{}_{}'.format(temp['name'], rcm, rcp, per, at)
        runs.append(temp)
    return runs


def setup_run_args(model_id, forward_run_dir, cc_to_waimak_only=False, cc_runs=True):
    """
    set up the forward runs
    :param model_id: which MC realisation to use
    :param forward_run_dir: the dir that all of teh model directories will be placed
    :param cc_to_waimak_only: boolean if true only apply the climate changes stuff to waimakariri
    :param cc_runs: boolean if True set up the runs for the climate change senarios else just the normal senarios
    :return: list of runs
    """
    runs = []
    # base model run handled separately in base forward runs with identified by the name 'mod_period'
    mod_per_rm_car = {
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
    runs.append(mod_per_rm_car)

    mod_per = {
        'model_id': model_id,
        'name': 'mod_period_w_ncar',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'org_pumping_wells': True,
        'rm_ncarpet': False
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

    current_w_ncar = {
        'model_id': model_id,
        'name': 'current_w_ncar',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': False,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'rm_ncarpet': False
    }
    runs.append(current_w_ncar)

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
        'pc5_well_reduction': True,
        'pc5_to_waimak_only': True,
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
        'pc5_well_reduction': True,
        'pc5_to_waimak_only': True,
        'wil_eff': 0,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'org_efficency': 80
    }
    runs.append(pc5_80_will_eff)

    # pc5
    pc5_no_pump_red = {
        'model_id': model_id,
        'name': 'pc5_no_pump_reduc',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': True,
        'pc5_well_reduction': False,
        'pc5_to_waimak_only': True,
        'wil_eff': 1,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'org_efficency': 80
    }
    runs.append(pc5_no_pump_red)

    # pc5 + will efficency
    pc5_no_pump_red_will_eff = {
        'model_id': model_id,
        'name': 'pc5_no_pump_reduc_wil_eff',
        'base_dir': None,
        'cc_inputs': None,
        'pc5': True,
        'pc5_well_reduction': False,
        'pc5_to_waimak_only': True,
        'wil_eff': 0,
        'naturalised': False,
        'full_abs': False,
        'pumping_well_scale': 1,
        'org_efficency': 80
    }
    runs.append(pc5_no_pump_red_will_eff)

    if cc_runs:
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
    """
    function to run at the start of each multiprocess sets the priority lower
    :return:
    """
    print('Starting', multiprocessing.current_process().name)
    p = psutil.Process(os.getpid())
    # set to lowest priority, this is windows only, on Unix use ps.nice(19)
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)


def run_forward_runs(runs, forward_run_dir, notes=None):
    """
    run the runs via multiprocessing
    :param runs: runs to run
    :param forward_run_dir: dir to put all runs in
    :param notes: notes to save as a readme file
    :return:
    """
    with open(os.path.join(forward_run_dir, 'READ_ME.txt'), 'w') as f:
        f.write(str(notes) + '\n')
    model_id = runs[0]['model_id']
    t = time.time()
    multiprocessing.log_to_stderr(logging.DEBUG)
    pool_size = psutil.cpu_count(logical=False)
    pool = multiprocessing.Pool(processes=pool_size,
                                initializer=start_process,
                                )
    results = pool.map_async(setup_run_forward_run_mp, runs)
    while not results.ready():
        print('{} runs left of {}'.format(results._number_left, len(runs)))
        time.sleep(60 * 5)  # sleep 5 min between printing
    pool_outputs = results.get()
    pool.close()  # no more tasks
    pool.join()
    now = datetime.datetime.now()
    with open("{}/forward_run_log/{}_forward_run_status_{}_{:02d}_{:02d}_{:02d}_{:02d}.txt".format(smt.sdp, model_id,
                                                                                                   now.year, now.month,
                                                                                                   now.day, now.hour,
                                                                                                   now.minute),
              'w') as f:
        f.write(str(notes) + '\n')
        wr = ['{}: {}\n'.format(e[0], e[1]) for e in pool_outputs]
        f.writelines(wr)
    print('{} runs completed in {} minutes'.format(len(runs), ((time.time() - t) / 60)))


if __name__ == '__main__':
    # tests run in the run script for forward runs
    safemode = True
    # define the two below before each run
    dir_path = r"D:\mh_model_runs\forward_runs_2017_10_10"  # path on rdsprod03
    notes = """ 
    LSR senario changes applied to full domain, CC component of LSR changes applied to whole domain, but ccmult and missing h20 is waimak only
    pumping changes only applied to Waimakariri with the exception of the pc5 adjustment which is applied in the full domain,
    run in {}
    """.format(dir_path)
    if safemode:
        if os.path.exists(dir_path):
            cont = input(
                'run all forward runs, this could overwrite item in :\n {} \n continue y/n\n'.format(dir_path)).lower()
            if cont != 'y':
                raise ValueError('script aborted so as not to potentially overwrite {}'.format(dir_path))
    runs = setup_run_args('opt', dir_path, cc_to_waimak_only=False)
    import time

    t = time.time()
    run_forward_runs(runs, dir_path, notes)
    print('{} runs in __ min'.format(len(runs)))
    print (time.time() - t) / 60
    print('done')
