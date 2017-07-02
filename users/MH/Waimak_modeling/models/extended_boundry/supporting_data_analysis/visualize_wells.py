"""
coding=utf-8
Author: matth
Date Created: 2/07/2017 4:46 PM
"""

from __future__ import division
from core import env
import pandas as pd
import matplotlib.pyplot as plt
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import get_nwai_wells, _get_s_wai_wells
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import _mt
from mpl_toolkits.mplot3d import Axes3D
from osgeo.gdal import Open as gdalOpen
import numpy as np
from users.MH.Waimak_modeling.supporting_data_path import sdp

def well_depth_base_layer_1(): # TODO DO THE SAME FOR THE 5+ READINGS
    targets = pd.read_csv(env.sci("Groundwater/Waimakariri/Groundwater/Numerical GW model/Model build and optimisation/water_levels_for_wells_2_plus_readings.csv"))
    targets = targets.loc[:,['nztmx', 'nztmy', 'mid_screen_elv','readings']]
    targets = targets.rename(columns={'nztmx': 'x', 'nztmy': 'y', 'mid_screen_elv': 'z'})
    targets.index.names = ['well']
    targets['group'] = 'targets'
    top = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/tops.tif".format(sdp)).ReadAsArray()
    top[np.isclose(top, -3.40282306074e+038)] = 0

    bottom_layer1 = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/layering/base_layer_1.tif".format(sdp)).ReadAsArray()
    idx = np.isclose(bottom_layer1, -3.40282306074e+038)
    bottom_layer1[idx] = top[idx] - 10 #set nan values to 10 m thick.  all these are out of the no-flow bound

    for well in targets.index:
        try:
            row, col = _mt.convert_coords_to_matix(targets.loc[well,'x'],targets.loc[well,'y'])
            targets.loc[well, 'row'], targets.loc[well, 'col'] = row, col
            targets.loc[well,'layer1_elv'] = bottom_layer1[row, col]
        except AssertionError:
            targets.loc[well, 'row'], targets.loc[well, 'col'] = np.nan, np.nan
            targets.loc[well, 'layer1_elv'] = np.nan

    targets.loc[:,'depth_from_l1'] = targets.loc[:, 'layer1_elv'] - targets.loc[:,'z']
    return targets



if __name__ == '__main__':
    targets = well_depth_base_layer_1()
    targets.to_csv(env.sci(r"Groundwater\Waimakariri\Groundwater\Numerical GW model\Model build and optimisation\wells_2_plus_from_layer1.csv"))
    print 'done'