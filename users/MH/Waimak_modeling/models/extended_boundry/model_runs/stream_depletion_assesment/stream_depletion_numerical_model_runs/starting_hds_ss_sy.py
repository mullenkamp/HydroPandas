"""
Author: matth
Date Created: 7/09/2017 4:19 PM
"""

from __future__ import division
from core import env
import pickle
import os
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.base_modflow_wrapper import \
    mod_gns_model
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.data_extraction.data_at_wells import \
    get_hds_file_path, hds_no_data
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_bc_data.wells import \
    get_race_data, get_full_consent
import flopy
import numpy as np

def get_sd_starting_hds(model_id, sd_version):
    if sd_version == 'sd7':
        hds = get_starting_heads_sd7(model_id)
    elif sd_version == 'sd30':
        hds = get_starting_heads_sd30(model_id)
    elif sd_version == 'sd150':
        hds = get_starting_heads_sd150(model_id)
    elif sd_version == 'grid':
        hds = _get_no_pumping_ss_hds(model_id)
    else:
        raise ValueError('unexpected argument for sd_version: {}'.format(sd_version))

    return hds

def get_sd_well_list(model_id):
    cav = get_full_consent(model_id) # note this is identical to max rate well list
    cav = cav.loc[(cav.type=='well') & (cav.zone == 'n_wai') & (cav.flux < 0)]
    well_list = list(cav.index)
    return well_list

def get_starting_heads_sd150(model_id):
    hds = _get_no_pumping_ss_hds(model_id)
    return hds


def get_starting_heads_sd30(model_id):
    hds = _get_no_pumping_ss_hds(model_id)
    return hds


def get_starting_heads_sd7(model_id):
    hds = _get_no_pumping_ss_hds(model_id)
    return hds


def _get_no_pumping_ss_hds(model_id, recalc=False):
    pickle_path = "{}/model_{}_sd_starting_hds.p".format(smt.pickle_dir, model_id)
    if (os.path.exists(pickle_path)) and (not recalc):
        hds = pickle.load(open(pickle_path))
        if hds.shape != (smt.layers, smt.rows, smt.cols):
            raise ValueError('unexpected shape for hds {}, expected {}'.format(hds.shape, (smt.layers, smt.rows, smt.cols)))
        return hds
    dirpath = "{}/forward_supporting_models/base_str_dep".format(smt.sdp)  # model Id is added in import gns model
    well = {0: smt.convert_well_data_to_stresspd(get_race_data(model_id))}
    m = mod_gns_model(model_id, 'base_for_str_dep', dir_path=dirpath, safe_mode=False, well=well)
    m.write_name_file()
    m.write_input()
    suc, buff = m.run_model()
    if not suc:
        raise ValueError('starting heads model for model_id: {} did not run'.format(model_id))
    hds_path = get_hds_file_path(m=m)
    hds = flopy.utils.HeadFile(hds_path).get_alldata(nodata=hds_no_data)[-1, :, :, :]
    if hds.shape != (smt.layers, smt.rows, smt.cols):
        raise ValueError('unexpected shape for hds {}, expected {}'.format(hds.shape, (smt.layers, smt.rows, smt.cols)))
    pickle.dump(hds, open(pickle_path, 'w'))
    return hds


def get_ss_sy():
    sy = np.zeros(smt.model_array_shape) + 0.1
    ss = np.zeros(smt.model_array_shape)
    ss[0, :, :] = 1.6E-3
    ss[1:, :, :] = 5.6E-5
    return ss, sy


if __name__ == '__main__':
    test = get_sd_starting_hds('opt','sd150')
    test2 = get_starting_heads_sd150('opt')
    print('done')