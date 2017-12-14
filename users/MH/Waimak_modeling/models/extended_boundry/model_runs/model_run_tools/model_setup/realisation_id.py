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
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import _get_wel_spd_v1, _get_wel_spd_v2
import pandas as pd
import numpy as np
import subprocess
import netCDF4 as nc

temp_pickle_dir = '{}/temp_pickle_dir'.format(smt.pickle_dir)
if not os.path.exists(temp_pickle_dir):
    os.makedirs(temp_pickle_dir)


def get_base_well(model_id, org_pumping_wells, recalc=False):
    """
    applies the NSMC pumping mulitpliers
    :param model_id: the NSMC realisation
    :param org_pumping_wells: if True use the model peiod wells if false use the 2014-2015 usage
    :param recalc: usual recalc
    :return:
    """
    if org_pumping_wells:
        org = 'model_period_pumping'
    else:
        org = '14-15_pumping_waimak'
    pickle_path = "{}/model_{}_base_wells_{}.p".format(temp_pickle_dir, model_id, org)
    if (os.path.exists(pickle_path)) and (not recalc):
        outdata = pickle.load(open(pickle_path))
        return outdata

    if org_pumping_wells:
        all_wells = _get_wel_spd_v1()  # usage for the model period as passed to the optimisation
    else:
        all_wells = _get_wel_spd_v2()  # usage for 2014/2015 period in waimak zone

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
    loads the rch from the model and saves as a pickle for faster use # depreciated
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
    """
    get the recharge multipler if it does not exist in the file then create it with fac2real
    :param model_id: the NSMC realisation
    :return:
    """
    model_ws = os.path.dirname(get_model_name_path(model_id))
    rch_mult_path = os.path.join(model_ws, 'recharge_mul.ref')
    if not os.path.exists(rch_mult_path):
        model_ws = os.path.splitunc(model_ws)[1].replace('/SCI',
                                                         'P:/')  # for now assuming that SCI is mapped to P drive could fix in future
        exe_path = r"P:/Groundwater/Matt_Hanson/model_exes/fac2real.exe"
        test = subprocess.call(exe_path + ' < fac2real_rech.in', cwd=model_ws, shell=True)
        print(test)

    # to read in the possibly wrapped array use:
    outdata = flopy.utils.Util2d.load_txt((smt.rows, smt.cols), rch_mult_path, float, '(FREE)')
    no_flow = smt.get_no_flow(1)
    no_flow[no_flow < 0] = 0
    outdata[~no_flow.astype(bool)] = 1
    return outdata


def get_model_name_path(model_id):
    """
    get the path to a model_id base model
    # !!!model id needs to be non-numeric (start with a letter) and does not contain an '_' !!!!
    :param model_id:
    :return:
    """
    # new model check the well package, check the sfr package and run new_model_run

    if model_id in ['test', 'opt', 'NsmcBaseB', 'VertUnstabA', 'VertUnstabB']:
        warn('model {} is depreciated'.format(model_id))

    model_dict = {
        # a place holder to test the scripts
        'test': r"C:\Users\MattH\Desktop\Waimak_modeling\ex_bd_tester\test_import_gns_mod\mf_aw_ex.nam",

        # the optimized model as of 26/09/2017; depreciated due to concerns in eyrewell forrest area and changes
        # in the recharge and well packages in the waimakariri Zone
        'opt': "{}/from_gns/optimised/mf_aw_ex/mf_aw_ex.nam".format(smt.sdp),

        # an opitimised model recieved 20/10/2017 that has the new priors, well and rch packages
        # which fits the heads, streams, ofshore flows, but not the deep heads, and not the verticle targets
        'StrOpt': "{}/from_gns/StrOpt/AW20171019_i3_optver/mf_aw_ex.nam".format(smt.sdp),

        # an optimised model revieved 24/10/2017 which fits all targets, but is rather unstable
        'VertUnstabB': "{}/from_gns/VertUnstabB/AW20171022_i4_optver/i4/mf_aw_ex.nam".format(smt.sdp),

        # the previous optimisation recived 24/10/2017 iterattion to VertUnstabB which does not fit the targets as well,
        # but is more stable
        'VertUnstabA': "{}/from_gns/VertUnstabA/AW20171022_i3_optver/i3/mf_aw_ex.nam".format(smt.sdp),

        # optimisation recieved on 24/10/2017 which is more stable and better hits the targets than either of the
        # VertUnstab models.  if the jacobian runs fine then this will be the base for the NSMC this ended up the base for the NSMC
        'NsmcBase': "{}/from_gns/NsmcBase/AW20171024_2_i2_optver/i2/mf_aw_ex.nam".format(smt.sdp),

        # optimisation revieved on 25/10/2017 which is the previous iteration to NsmcBase.  NsmcBase was quite unstable,
        # which is why we are considering the previous optimisation
        'NsmcBaseB': "{}/from_gns/NsmcBaseB/AW20171024_2_i1_optver/i1/mf_aw_ex.nam".format(smt.sdp)
    }
    if '_' in model_id:
        raise ValueError(
            '_ in model id: {}, model_id cannot include an "_" as this is a splitting character'.format(model_id))
    if model_id not in model_dict.keys():
        raise NotImplementedError('model {} has not yet been defined'.format(model_id))
    return model_dict[model_id]


def _get_nsmc_realisation(model_id, save_to_dir=False):
    """
    wrapper to get model from a NSMC realisation
    :param model_id: identifier 'NsmcReal{nsmc_num:06d}'
    :param save_to_dir: boolean if true save a copy of the model for quicker reteval in the dir specified below
    :return:
    """
    assert 'NsmcReal' in model_id, 'unknown model id: {}, expected NsmcReal(nsmc_num:06d)'.format(model_id)
    assert len(model_id) == 14, 'unknown model id: {}, expected NsmcReal(nsmc_num:06d)'.format(model_id)

    nsmc_num = int(model_id[-6:])
    save_dir = None  # todo set up
    # todo allow the model to be opened from a saved version....
    converter_dir = "{}/base_for_nsmc_real".format(smt.sdp)
    param_data = nc.Dataset(env.gw_met_data(r"mh_modeling\netcdfs_of_key_modeling_data\nsmc_params_obs_metadata.nc"))
    param_idx = np.where(np.array(param_data.variables['nsmc_num']) == nsmc_num)[0][0]

    # write well paraemeters to wel_adj.txt #todo check
    with open(os.path.join(converter_dir, 'wel_adj.txt'), 'w') as f:
        for param in ['pump_c', 'pump_s', 'pump_w', 'sriv', 'n_race', 's_race', 'nbndf', 'llrzf', 'ulrzf']:
            val = param_data.variables[param][param_idx]
            f.write('{} {}\n'.format(param, val))

    # write rch parameters to rch_ppts.txt #todo check
    with open(os.path.join(converter_dir, 'rch_ppts.txt'), 'w') as f:
        keys = np.array(param_data.variables['rch_ppt'])
        x = np.array(param_data.variables['rch_ppt_x'])
        y = np.array(param_data.variables['rch_ppt_y'])
        group = np.array(param_data.variables['rch_ppt_group'])
        val = np.array(param_data.variables['rch_mult'][param_idx])
        for k, _x, _y, g, v in zip(keys, x, y, group, val):
            f.write('{} {} {} {} {}\n'.format(k, _x, _y, g, v))

    # write kh and kv #todo check
    all_kv = np.array(param_data.variables['kv'][param_idx])  # shape = (11, 178)
    all_kh = np.array(param_data.variables['kh'][param_idx])  # shape = (11, 178)
    all_names = np.array(param_data.variables['khv_ppt'])
    all_x = np.array(param_data.variables['khv_ppt_x'])
    all_y = np.array(param_data.variables['khv_ppt_y'])
    for layer in range(smt.layers):
        khv_idx = np.isfinite(all_kh[layer])
        layer_names = all_names[khv_idx]
        layer_x = all_x[khv_idx]
        layer_y = all_y[khv_idx]
        layer_kv = all_kv[layer][khv_idx]
        layer_kh = all_kh[layer][khv_idx]

        # parameters to kh_kkp_{layer one indexed}.txt
        with open(os.path.join(converter_dir, 'KH_ppk_{:02d}.txt'.format(layer + 1)), 'w') as f:  # one indexed
            for n, x, y, kh in zip(layer_names, layer_x, layer_y, layer_kh):
                f.write('{} {} {} {}\n'.format(n, x, y, kh))

        # write kv parameters to kv_ppk_{layer one indexed}.txt
        with open(os.path.join(converter_dir, 'KV_ppk_{:02d}.txt'.format(layer + 1)), 'w') as f:  # one indexed
            for n, x, y, kv in zip(layer_names, layer_x, layer_y, layer_kv):
                f.write('{} {} {} {}\n'.format(n, x, y, kv))

    # write sfr parameters to segfile.txt and/or segfile.tpl #todo

    # write fault parameters to fault_ks.txt #todo check
    with open(os.path.join(converter_dir, 'fault_ks.txt'), 'w') as f:
        for param in ['fkh_mult', 'fkv_mult']:
            val = param_data.variables[param][param_idx]
            f.write('{}\n'.format(val))

    # write drain package from parameters and mf_aw_ex_drn.tpl #todo

    # run model.bat #todo

    # load model #todo

    # if save then save to a new folder and run the model #todo move up a level, nope



    raise NotImplementedError


def get_model(model_id, save_to_dir=False):
    """
    load a flopy model instance of the model id
    :param model_id:
    :param save_to_dir: boolean only for nsmc realisations save the model to dir?
    :return: m flopy model instance
    """
    # check well packaged loads appropriately! yep this is a problem because we define the iface value,
    #  but it isn't used, remove iface manually
    # if it doesn't exist I will need to add the options flags to SFR package manually

    if 'NsmcReal' in model_id:  # model id should be 06d
        assert len(model_id) == 14, 'model id for realsiation is "NsmcReal{nsmc_num:06d}"'
        m = _get_nsmc_realisation(model_id, save_to_dir)
        return m

    name_file_path = get_model_name_path(model_id)
    well_path = name_file_path.replace('.nam', '.wel')
    with open(well_path) as f:
        counter = 0
        while counter < 10:
            line = f.readline()
            counter += 1
            if 'aux' in line.lower():
                raise ValueError(
                    'AUX in well package will cause reading error {} please remove manually'.format(well_path))

    # check SFR package is correct
    sfr_path = name_file_path.replace('.nam', '.sfr')
    with open(sfr_path) as f:
        lines = [next(f).lower().strip() for x in range(10)]

    if any(~np.in1d(['options', 'reachinput', 'end'], lines)):
        raise ValueError('options needed in sfr package {}, please add manually'.format(sfr_path))

    m = flopy.modflow.Modflow.load(name_file_path, model_ws=os.path.dirname(name_file_path), forgive=False)
    return m


if __name__ == '__main__':
    # tests
    test = get_rch_multipler('opt')
    m = get_model('opt')
    well = get_base_well('test')
    print m
