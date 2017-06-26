"""
coding=utf-8
Author: matth
Date Created: 26/06/2017 10:52 AM
"""

from __future__ import division
from users.MH.Waimak_modeling.supporting_data_path import sdp
from osgeo.gdal import Open as gdalOpen
import numpy as np

def _elvdb_calc(): #todo
    top = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/tops.tif".format(sdp)).ReadAsArray()
    top[np.isclose(top,-3.40282306074e+038)] = 0

    basement = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/basement2.tif".format(sdp)).ReadAsArray()
    basement[np.isclose(basement, 9999999)] = np.nan
    basement2 = np.concatenate((basement, np.repeat(basement[:, -1][:, np.newaxis], 33, axis=1)), axis=1)

    bottom_layer1 = None #todo define this

    #todo build up the layers based on the above


    raise NotImplementedError

def _no_flow_calc():
    raise NotImplementedError