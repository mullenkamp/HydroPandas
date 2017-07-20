# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 20/07/2017 12:55 PM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.drn_packages import _get_drn_spd
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt, _get_constant_heads
import geopandas as gpd

def gen_drn_target_array():
    drn_data = _get_drn_spd(smt.reach_v,smt.wel_version)
    out_dict = {}
    out_array = np.zeros((smt.rows,smt.cols))
    for i, group in enumerate(set(drn_data['target_group']),1):
        temp = drn_data.loc[drn_data.target_group==group]
        temp_array = smt.df_to_array(temp,'k')
        out_array[np.isfinite(temp_array)] = i
        out_dict[i] = group

    return out_array, out_dict

def gen_sfr_flow_array():
    raise NotImplementedError

def  gen_sfr_flux_array():
    raise NotImplementedError

def gen_constant_head_targets(): # watch if we have constant heads in the sw boundary also note that this is 3d
    chbs = _get_constant_heads()
    shp_path = "{}/m_ex_bd_inputs/shp/coastal_head_target_zones.shp".format(smt.sdp)
    zones = np.repeat(smt.shape_file_to_model_array(shp_path,'Id',alltouched=True)[np.newaxis,:,:],smt.layers,axis=0)
    zones[np.isnan(chbs)| (chbs < -900)] = 0
    zones[np.isnan(zones)] = 0
    zone_data = gpd.read_file(shp_path)
    zone_data = zone_data.set_index('Id')
    zone_data = zone_data.loc[:,'name'].to_dict()

    return zones,zone_data

if __name__ == '__main__':
    #zones,zone_data = gen_constant_head_targets()
    drn_array, drn_dict = gen_drn_target_array()
    print 'done'
