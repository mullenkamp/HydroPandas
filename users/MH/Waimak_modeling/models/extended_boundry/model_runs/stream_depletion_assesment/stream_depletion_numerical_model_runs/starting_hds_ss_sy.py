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
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_bc_data.wells import get_race_data
import flopy


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
    pickle_path = "{}/model_well_full_abstraction.p".format(smt.pickle_dir)
    if (os.path.exists(pickle_path)) and (not recalc):
        hds = pickle.load(open(pickle_path))
        return hds
    dirpath = "{}/forward_supporting_models/base_str_dep".format(smt.sdp)  # model Id is added in import gns model
    well = {0:smt.convert_well_data_to_stresspd(get_race_data())}
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
    # todo check this function/debug
    pickle.dump(hds, open(pickle_path, 'w'))
    return hds


def get_ss_sy():  # todo
    sy = 0.1
    ss = None #todo
    raise NotImplementedError
