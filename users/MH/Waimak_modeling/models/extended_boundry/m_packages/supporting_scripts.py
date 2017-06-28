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

    basement = _get_basement()

    bottom_layer1 = None #todo define this

    #todo build up the layers based on the above


    raise NotImplementedError

def _get_basement():
    basement = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/basement2.tif".format(sdp)).ReadAsArray()
    basement[np.isclose(basement, 9999999)] = np.nan
    basement2 = np.concatenate((basement, np.repeat(basement[:, -1][:, np.newaxis], 33, axis=1)), axis=1)
    return basement2


def _no_flow_calc(): #todo
    elv_db = _elvdb_calc()
    basement = _get_basement()
    constant_heads = _get_constant_heads()

    org_no_flow = None # todo

    #load original
    #add on rows via repetition of zeros
    # convert shapefile to array and set all to 1 then add to the active boundry (set all <0 to 1)
    # set all of those greater than 1 to 1
    # load constant head cell lines/polygons and set those to -1 load polygons as -1


    raise NotImplementedError

def _get_constant_heads(): #todo
    # todo think about where to put the constant head boundries
    # sea surface (north/south wai
    # te waihuro
    # model boundry... how many layers for this
    #return a 3d array (layer, col, row) of constant heads values and np.nan for all others.
    raise NotImplementedError

if __name__ == '__main__':
    _elvdb_calc()