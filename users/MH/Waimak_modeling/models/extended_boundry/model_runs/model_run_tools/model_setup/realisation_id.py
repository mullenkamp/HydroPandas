# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/09/2017 2:23 PM
"""

from __future__ import division
from core import env
import flopy
import pickle
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import os
from warnings import warn
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import _get_wel_spd_v1
import pandas as pd
import numpy as np
import subprocess

temp_pickle_dir = '{}/temp_pickle_dir'.format(smt.pickle_dir)
if not os.path.exists(temp_pickle_dir):
    os.makedirs(temp_pickle_dir)


def get_base_well(model_id, recalc=False):
    pickle_path = "{}/model_{}_base_wells".format(temp_pickle_dir, model_id)
    if (os.path.exists(pickle_path)) and (not recalc):
        outdata = pickle.load(open(pickle_path))
        return outdata

    all_wells = _get_wel_spd_v1()
    all_wells.loc[:, 'nsmc_type'] = ''

    # pumping wells
    all_wells.loc[(all_wells.type == 'well') & (all_wells.cwms == 'chch'), 'nsmc_type'] = 'pump_c'
    all_wells.loc[(all_wells.type == 'well') & (all_wells.cwms == 'selwyn'), 'nsmc_type'] = 'pump_s'
    all_wells.loc[(all_wells.type == 'well') & (all_wells.cwms == 'waimak'), 'nsmc_type'] = 'pump_w'

    # selwyn_hillfeds
    all_wells.loc[all_wells.type == 'river', 'nsmc_type'] = 'sriv'

    # races
    all_wells.loc[(all_wells.type == 'race') & (all_wells.zone == 'n_wai'), 'nsmc_type'] = 'n_race'
    all_wells.loc[(all_wells.type == 'race') & (all_wells.zone == 's_wai'), 'nsmc_type'] = 's_race'

    # boundary fluxes
    all_wells.loc[all_wells.type == 'llr_boundry_flux', 'nsmc_type'] = 'llrzf'
    all_wells.loc[all_wells.type == 'ulr_boundry_flux', 'nsmc_type'] = 'ulrzf'
    all_wells.loc[all_wells.type == 'boundry_flux', 'nsmc_type'] = 'nbndf'

    base_dir = os.path.dirname(get_model_name_path(model_id))
    mult_path = '{}/wel_adj.txt'.format(base_dir)  # figure out what the pest control file will populate
    multipliers = pd.read_table(mult_path, index_col=0, delim_whitespace=True, names=['value'])

    mult_groups = ['pump_c', 'pump_s', 'pump_w', 'sriv', 'n_race', 's_race', 'nbndf']
    for group in mult_groups:
        all_wells.loc[all_wells.nsmc_type == group, 'flux'] *= multipliers.loc[group, 'value']

    add_groups = ['llrzf', 'ulrzf']
    for group in add_groups:
        all_wells.loc[all_wells.nsmc_type == group, 'flux'] = multipliers.loc[group, 'value'] / (
        all_wells.nsmc_type == group).sum()
    pickle.dump(all_wells, open(pickle_path, 'w'))
    return all_wells


def get_base_rch(model_id, recalc=False):
    """
    loads the rch from the model and saves as a pickle for faster use
    :param model_id: the model realisation to use
    :param recalc: False
    :return:
    """
    pickle_path = "{}/model_{}_base_rch.p".format(temp_pickle_dir, model_id)
    if (os.path.exists(pickle_path)) and (not recalc):
        rch = pickle.load(open(pickle_path))
        return rch

    m = get_model(model_id)
    rch = m.rch.rech.array[0, 0, :, :]
    pickle.dump(rch, open(pickle_path, 'w'))
    return rch


def get_rch_multipler(model_id):
    model_ws = os.path.dirname(get_model_name_path(model_id))  # for now assuming that SCI is mapped to P drive could fix in future
    model_ws = os.path.splitunc(model_ws)[1].replace('/SCI','P:/')
    exe_path = r"P:/Groundwater/Matt_Hanson/model_exes/fac2real.exe"
    rch_mult_path = os.path.join(model_ws, 'recharge_mul.ref')
    if not os.path.exists(rch_mult_path):
        test = subprocess.call(exe_path + ' < fac2real_rech.in',cwd=model_ws,shell=True)
        print(test)


    # to read in the possibly wrapped array use:
    outdata = flopy.utils.Util2d.load_txt((smt.rows, smt.cols), rch_mult_path, float, '(FREE)')
    no_flow = smt.get_no_flow(1)
    no_flow[no_flow < 0] = 0
    outdata[~no_flow.astype(bool)] = 1
    return outdata


def get_model_name_path(model_id):
    # model id needs to be non-numeric (start with a letter)
    model_dict = {
        'test': r"C:\Users\MattH\Desktop\Waimak_modeling\ex_bd_tester\test_import_gns_mod\mf_aw_ex.nam",
    # a place holder to test the scripts
        'opt': "{}/from_gns/optimised/mf_aw_ex/mf_aw_ex.nam".format(smt.sdp)  # the optimized model as of 26/09/2017
    }
    if model_id not in model_dict.keys():
        raise NotImplementedError('model {} has not yet been defined'.format(model_id))
    return model_dict[model_id]


def get_model(model_id):
    """
    load a flopy model instance of the model id
    :param model_id:
    :return: m flopy model instance
    """
    # check well packaged loads appropriately! yep this is a problem because we define the iface value,
    #  but it isn't used, remove iface manually?
    # if it doesn't exist I will need to add the options flags to SFR package manually?

    name_file_path = get_model_name_path(model_id)
    well_path = name_file_path.replace('.nam', '.wel')
    with open(well_path) as f:
        counter = 0
        while counter < 10:
            line = f.readline()
            counter += 1
            if 'aux' in line.lower():
                raise ValueError('AUX in well package will cause reading error {}'.format(well_path))

    # check SFR package is correct
    sfr_path = name_file_path.replace('.nam', '.sfr')
    with open(sfr_path) as f:
        lines = [next(f).lower().strip() for x in range(10)]

    if any(~np.in1d(['options', 'reachinput', 'end'], lines)):
        raise ValueError('options needed in sfr package {}'.format(sfr_path))

    m = flopy.modflow.Modflow.load(name_file_path, model_ws=os.path.dirname(name_file_path), forgive=False)
    return m


if __name__ == '__main__':
    test = get_rch_multipler('opt')
    m = get_model('opt')
    well = get_base_well('test')
    print m
