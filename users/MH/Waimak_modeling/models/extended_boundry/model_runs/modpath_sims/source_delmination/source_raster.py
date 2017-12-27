# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 11/12/2017 9:56 AM
"""

from __future__ import division
from core import env
from time import time, sleep
import numpy as np
import pandas as pd
import os
import multiprocessing
import logging
import psutil
import datetime
from traceback import format_exc
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.modpath_sims.setup_reverse_modpath import \
    setup_run_backward_modpath
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.modpath_sims.extract_data import extract_back_data, \
    save_forward_data
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.modpath_sims.setup_forward_modpath import \
    setup_run_forward_modpath
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.modpath_wrapper import \
    get_cbc, get_cbc_mp
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.convergance_check import modpath_converged


def define_source_from_forward(emulator_path, bd_type, index):
    """
    defines the source area for a given integer array
    :param emulator_path: path to the emulator (hdf)
    :param index: a integer array of areas of interest 0 delneates no interest
    :return: array of shape (smt.layers, rows, cols) with a rough percentage of water from source
    """
    # run some checks on inputs
    assert isinstance(index, np.ndarray), 'index must be a nd array'
    assert index.shape == (smt.layers, smt.rows, smt.cols), 'index must be 3d'
    assert issubclass(index.dtype, np.integer), 'index must be some sort of integer array'
    assert index.min() >= 0, 'index should be positive'

    # load emulator and initialize outdata
    print('loading emulator')
    t = time()
    emulator = pd.read_hdf(emulator_path)  # this keeps the structure of everything

    print('took {} s to load emulator'.format(time() - t))
    t = time()

    # get general area of interest
    print('calculating area of interest')
    idx = index > 0
    layers, rows, cols = np.meshgrid(range(smt.layers), range(smt.rows), range(smt.cols), indexing='ij')
    ids = ['{:02d}_{:03d}_{:03d}'.format(k, i, j) for k, i, j in zip(layers[idx], rows[idx], cols[idx])]
    temp = np.in1d(emulator.index.values, ids)
    emulator = emulator.loc[temp]
    print('took {} s to general identify area'.format(time() - t))

    # calculate source percentage
    all_outdata = {}
    for g in set(index.flatten()) - {0}:
        idx = index == g
        ids = ['{:02d}_{:03d}_{:03d}'.format(k, i, j) for k, i, j in zip(layers[idx], rows[idx], cols[idx])]
        temp = np.in1d(emulator.index.values, ids)
        temp_emulator = emulator.loc[temp]
        temp_emulator = temp_emulator.reset_index()
        temp = temp_emulator.groupby('Particle_Group').aggregate({'fraction': np.sum})
        temp *= 1 / temp.sum()

        # populate array
        outdata = smt.get_empty_model_grid(False)
        idx = bd_type.flatten() != -1
        outdata = outdata.flatten()
        temp_array = outdata[idx]
        temp_array[temp.index.values - 1] = temp.fraction.values
        outdata[idx] = temp_array
        outdata = outdata.reshape((smt.rows, smt.cols))
        all_outdata[g] = outdata

    return all_outdata


def define_source_from_backward(index, mp_ws, mp_name, cbc_file, root3_num_part=1, capt_weak_s=False, recalc=False):
    """
    define the source area for an integer index
    :param index: a integer array of groups of areas of interest 0 delneates no interest
    :param mp_ws: path for the modpath model
    :param mp_name: name of the modpath model
    :param cbc_file: cbc_file for the base modflow model of interest
    :param root3_num_part: the cubic root of the number of particles (placed evenly in the cell) e.g.
                           root3_num_part of 2 places 8 particles in each cell
    :param capt_weak_s: bool if True terminate particles at weak sources
    :param recalc: bool if True rerun the model even if it exists
    :return:
    """
    assert isinstance(index, np.ndarray), 'index must be a nd array'
    assert index.shape == (smt.layers, smt.rows, smt.cols), 'index must be 3d'
    assert issubclass(index.dtype, np.integer), 'index must be some sort of integer array'
    assert index.min() >= 0, 'index should be positive'

    path_path = os.path.join(mp_ws, '{}.mppth'.format(mp_name))
    if not os.path.exists(path_path) or recalc:
        # set up and run model
        part_index = index > 0
        setup_run_backward_modpath(mp_ws, mp_name, cbc_file, index=part_index, group=index,
                                   root3_num_part=root3_num_part, capt_weak_s=capt_weak_s)

    mapper_path = os.path.join(mp_ws, '{}_group_mapper.csv'.format(mp_name))
    outdata = extract_back_data(path_path, mapper_path)
    return outdata


def _run_forward_em_one_mp(kwargs):  # todo debug
    model_id = kwargs['model_id']
    try:
        needed_keys = ['model_id', 'mp_runs_dir', 'emulator_dir', 'modflow_dir', 'min_part', 'max_part', 'capt_weak_s',
                       'keep_org_files']
        assert np.in1d(needed_keys, kwargs.keys()).all(), 'missing keys {}'.format(
            set(needed_keys) - set(kwargs.keys()))
        mp_ws = os.path.join(kwargs['mp_runs_dir'], model_id)
        outpath = os.path.join(kwargs['emulator_dir'], model_id)
        mp_name = '{}_forward'.format(model_id)
        cbc_path = get_cbc(model_id, kwargs['modflow_dir'])
        setup_run_forward_modpath(cbc_path, mp_ws, mp_name,
                                  min_part=kwargs['min_part'], max_part=kwargs['max_part'],
                                  capt_weak_s=kwargs['capt_weak_s'])
        path_path = os.path.join(mp_ws, '{}.mppth'.format(mp_name))
        save_forward_data(path_path, outpath)

        if not kwargs['keep_org_files']:
            # delete large files to save memory
            for end in ['.mpend', '.mppth', '.loc', '.mpbas']:  # all others are either needed or really tiny
                os.remove(os.path.join(mp_ws, '{}{}'.format(mp_name, end)))
        success = 'converge' if modpath_converged(os.path.join(mp_ws, '{}.mplst'.format(mp_name))) else 'did not converge'
    except Exception as val:
        mp_name = '{}_forward'.format(model_id)
        success = format_exc().replace('\n', '')

    return mp_name, success


def run_forward_emulators(model_ids, results_dir, modflow_dir, keep_org_files=True, min_part=1, max_part=None,
                          capt_weak_s=False, notes=''):
    """

    :param model_ids: a list of model ids to run
    :param results_dir: the dir to put the modpath models and the results
    :param modflow_dir: the dir to put the necissary modflow models for modpath
    :param keep_org_files: bool if True keep all modpath files else delete the big ones
    :param min_part: the minimum number of particles for each cell
    :param max_part: the maximum number of particles for each cell
    :param capt_weak_s: bool if True terminate particles at weak sink
    :param notes: str any notes to be saves as a readme
    :return:
    """
    # set up a function to run all the emulators for the forward runs
    model_ids = np.atleast_1d(model_ids)
    emulator_dir = os.path.join(results_dir, 'forward_data')
    mp_runs_dir = os.path.join(results_dir, 'forward_runs')

    for path in [modflow_dir, results_dir, emulator_dir, mp_runs_dir]:
        if not os.path.exists(path):
            os.makedirs(path)

    with open(os.path.join(results_dir, 'README.txt'), 'w') as f:
        f.write(notes)

    input_kwargs = []
    for model_id in model_ids:
        temp = {'model_id': model_id,
                'mp_runs_dir': mp_runs_dir,
                'emulator_dir': emulator_dir,
                'modflow_dir': modflow_dir,
                'min_part': min_part,
                'max_part': max_part,
                'capt_weak_s': capt_weak_s,
                'keep_org_files': keep_org_files}

        input_kwargs.append(temp)
    t = time()
    # multiprocess the running of things
    outputs = []
    for kwarg in input_kwargs:
        outputs.append(_run_forward_em_one_mp(kwarg))
    now = datetime.datetime.now()
    with open(
            "{}/forward_run_log/{}_forward_modpath_{:02d}_{:02d}_{:02d}_{:02d}.txt".format(smt.sdp, now.year, now.month,
                                                                                           now.day, now.hour,
                                                                                           now.minute), 'w') as f:
        f.write(str(notes) + '\n')
        wr = ['{}: {}\n'.format(e[0], e[1]) for e in outputs]
        f.writelines(wr)
    with open("{}/metadata.txt".format(results_dir), 'w') as f:
        f.write(str(notes) + '\n')
        wr = ['{}: {}\n'.format(e[0], e[1]) for e in outputs]
        f.writelines(wr)
    print('{} runs completed in {} minutes'.format(len(model_ids), ((time() - t) / 60)))


def get_all_cbcs(model_ids, modflow_dir, sleep_time=5):
    """
    a quick multiprocessing wrapper to run all the NSMC realisations I need
    :param model_ids: list of model ids to pass to the get cbc
    :param modflow_dir: overall directory to save everything
    :param sleep_time: the sleep time between printing number of runs left (mostly for debugging purposes)
    :return:
    """
    input_kwargs = []
    for model_id in model_ids:
        input_kwargs.append({'model_id': model_id,
                             'base_dir': modflow_dir})

    multiprocessing.log_to_stderr(logging.DEBUG)
    pool_size = psutil.cpu_count(logical=False)
    pool = multiprocessing.Pool(processes=pool_size,
                                initializer=start_process,
                                )
    results = pool.map_async(get_cbc_mp, input_kwargs)
    while not results.ready():
        print('{} runs left of {}'.format(results._number_left, len(input_kwargs)))
        sleep(60 * sleep_time)  # sleep 5 min between printing
    pool_outputs = results.get()
    pool.close()  # no more tasks
    pool.join()
    with open(os.path.join(modflow_dir, 'metadata.txt'), 'w') as f:
        wr = ['{}: {}\n'.format(e[0], e[1]) for e in pool_outputs]
        f.writelines(wr)


def get_both_source_areas():  # todo
    # todo add weak source/sink so I can interpret all four plots qualativly
    # a wrapper to setup and run all of the source delineation
    # some how save the number data as well as just the boolean data as a netcdf
    raise NotImplementedError


def start_process():
    print('Starting', multiprocessing.current_process().name, 'pid: {}'.format(os.getpid()))
    p = psutil.Process(os.getpid())
    # set to lowest priority, this is windows only, on Unix use ps.nice(19)
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)


def get_modeflow_dir_for_source(version=1):
    """
    somethign to keep the modflow dirs for the modpath sernarios straight
    :param version: which version
    :return:
    """
    if version == 1:
        path = env.gw_met_data("mh_modeling/modflow_dir_for_source")
    else:
        raise NotImplementedError('version {} has not been set up'.format(version))
    return path

if __name__ == '__main__':
    # this looks good
    # check layer 7 in the area of no data
    import matplotlib.pyplot as plt
    from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.cwms_index import \
        get_zone_array_index

    temp_index = smt.shape_file_to_model_array(r"C:\Users\MattH\Downloads\test_area.shp", 'Id', True)
    index = smt.get_empty_model_grid(True).astype(bool)
    index[1] = np.isfinite(temp_index)
    index = smt.get_empty_model_grid(True).astype(bool)
    temp = smt.shape_file_to_model_array(r"{}\m_ex_bd_inputs\shp\rough_chch.shp".format(smt.sdp), 'Id', True)
    index[5][np.isfinite(temp)] = True
    bd_type = np.loadtxt(r"T:\Temp\temp_gw_files\NsmcBase_first_try_bnd_type.txt")
    outdata = define_source_from_forward(r"T:\Temp\temp_gw_files\first_try.hdf", bd_type, index.astype(bool))
    smt.plt_matrix(outdata != 0, base_map=True)
    plt.show()
