# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 8/09/2017 8:28 AM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.realisation_id import \
    get_rch_multipler
from rch_support.map_rch_to_model_array import map_rch_to_array
import os
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt

lsrm_rch_base_dir = env.gw_met_data('niwa_netcdf/lsrm/lsrm_results/water_year_means')
rch_idx_shp_path = env.gw_met_data("niwa_netcdf/lsrm/lsrm_results/test/output_test2.shp")

def get_forward_rch(model_id, naturalised, pc5=False, rcm=None, rcp=None, period=None, amag_type=None):
    """
    get the rch for the forward runs
    :param model_id: which NSMC realisation to use
    :param naturalised: boolean if True then get rch for
    :param rcm: regional Climate model identifier
    :param rcp: representetive carbon pathway identifier
    :param period: e.g. 2010, 2020, ect
    :param amag_type: the amalgamation type one of: 'tym': ten year mean,
                                                    'min': minimum annual average,
                                                    'low_3_m': average of the 3 lowest consecutive years
                                                    'mean': full data mean
                                                    None: then use 'mean'
    :param pc5: boolean if true use assumed PC5 efficency (only applied to the WILS and {something} areas)
    :return: rch array (11,364,365)
    """
    # I think I need to apply everything as a percent change or somehow normalise the rch so that I do not get any big
    # changes associated with changes in models which created the recharge array.

    hdf_path = _get_rch_hdf_path(lsrm_rch_base_dir, naturalised, pc5, rcm, rcp)
    # get rch array from LSRM
    amalg_dict = {None: 'mean', 'mean': 'mean', 'tym': 'period_mean', 'low_3_m': '3_lowest_con_mean',
                  'min': 'lowest_year'}

    method = amalg_dict[amag_type]
    rch_array = map_rch_to_array(hdf=hdf_path,
                                 method=method,
                                 period_center=period,
                                 mapping_shp=rch_idx_shp_path,
                                 period_length=10,
                                 return_irr_demand=False)

    # apply multiplier array from pest parameraterisation
    rch_array *= get_rch_multipler(model_id)

    # handle weirdness from the arrays (e.g. ibound ignore the weirdness from chch/te waihora paw)
    no_flow = smt.get_no_flow(0)
    no_flow[no_flow < 0] = 0
    rch_array[~no_flow.astype(bool)] = 0
    return rch_array #todo check


def _get_rch_hdf_path(base_dir, naturalised, pc5, rcm, rcp):  # todo check
    """
    get the path for the rch
    :param base_dir: the directory containing all forward runs
    :param naturalised: boolean if true use array with no irrigation
    :param pc5: boolean if true use 100% effcient irrigation
    :param rcm: None or the RCM of the model
    :param rcp: None or the RCP of the model
    :return:
    """
    if rcp is None and rcm is not None:
        raise ValueError('rcm and rcp must either both be none or be defined')
    elif rcm is None and rcp is not None:
        raise ValueError('rcm and rcp must either both be none or be defined')
    elif rcp is None and rcp is None:
        if naturalised:
            outpath = os.path.join(base_dir, 'vcsn_no_irr.h5')
        elif pc5:
            outpath = os.path.join(base_dir, 'vcsn_100perc.h5')
        else:
            outpath = os.path.join(base_dir, 'vcsn_80perc.h5')
    else:
        if naturalised:
            outpath = os.path.join(base_dir, "{}_{}_no_irr.h5".format(rcp, rcm))
        elif pc5:
            outpath = os.path.join(base_dir, "{}_{}_100perc.h5".format(rcp, rcm))
        else:
            outpath = os.path.join(base_dir, "{}_{}_80perc.h5".format(rcp, rcm))

    return outpath
