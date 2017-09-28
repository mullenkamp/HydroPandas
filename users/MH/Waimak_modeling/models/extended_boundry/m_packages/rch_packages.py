# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 20/06/2017 11:59 AM
"""

from __future__ import division
from core import env
import flopy
import numpy as np
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.model_tools import get_base_rch, no_flow as old_no_flow
from copy import deepcopy
import pickle
import os


def create_rch_package(m):
    rch = flopy.modflow.mfrch.ModflowRch(m,
                                         nrchop=3,
                                         ipakcb=740,
                                         rech=_get_rch(),
                                         irch=0,
                                         unitnumber=716)



def _get_rch(recalc=False):
    pickle_path = '{}/org_rch.p'.format(smt.pickle_dir)
    if os.path.exists(pickle_path) and not recalc:
        rch = pickle.load(open(pickle_path))
        return rch

    new_no_flow = smt.get_no_flow()
    zones = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/cwms_zones.shp".format(smt.sdp),'ZONE_CODE')
    zones[~new_no_flow[0].astype(bool)] = 0
    # waimak = 4, chch_wm = 7, selwyn=8 , chch_wm chch_formation = 9
    confin_rch_zone = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/chch_wm_rch_split_chch_form.shp".format(smt.sdp),'ID',True)
    zones[(zones==7) & (np.isfinite(confin_rch_zone))] = 9
    part_zones = deepcopy(zones[:190,:])
    part_zones[old_no_flow] = np.nan # here this is the no flow for layer 1 I created hence why i don't invert it

    old_rch = get_base_rch()
    scaled_old_rch = np.zeros((190,365))*np.nan
    scaled_old_rch[part_zones==4] = old_rch[part_zones==4] # do not scale waimak
    new_old_top = np.nanpercentile(scaled_old_rch,99)
    scaled_old_rch[scaled_old_rch> new_old_top] = new_old_top
    # create rch values for south wai
    """use homogeneous rate of 270 mm/year based on Williams 2010 (modified from White 2008)
    estimate of 23.8 m³/s for 276,000 ha Te Waihora catchment area.
    Use 190 mm/year for Christchurch WM zone. scaled by an implementation of the david scott model"""

    ds_rch = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/dave_scott_rch.shp".format(smt.sdp), 'rch', True)/1000
    ds_paw = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/dave_scott_rch.shp".format(smt.sdp), 'paw', True)
    #do a bit of cleaning for the DS model
    ds_rch[ds_paw <= 600000] = np.nan  # get rid or rch in streams, te wai, and chch urban area
    new_top = np.nanpercentile(ds_rch,99)
    ds_rch[ds_rch>new_top] = new_top
    new_bot = np.nanpercentile(ds_rch,1)
    ds_rch[ds_rch<new_bot] = new_bot
    ds_rch[np.isnan(ds_rch)] = new_bot

    rch = np.zeros((smt.rows, smt.cols))
    rch[zones == 7] = ds_rch[zones==7] * 175/1000/365/ds_rch[zones==7].mean()
    rch[zones == 9] = ds_rch[zones==9] * 100/1000/365/ds_rch[zones==9].mean()
    rch[zones == 8] = ds_rch[zones==8] * 195/1000/365/ds_rch[zones==8].mean()
    rch[zones == 4] = ds_rch[zones==4] * 290/1000/365/ds_rch[zones==4].mean()
    idx = np.where(np.isfinite(scaled_old_rch))
    rch[idx]= scaled_old_rch[idx]
    # get new rch values for nwai
    pickle.dump(rch, open(pickle_path, 'w'))
    return rch
if __name__ == '__main__':
    rch=_get_rch()
    smt.plt_matrix(rch)
    print('done')