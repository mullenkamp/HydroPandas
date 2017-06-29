"""
Author: matth
Date Created: 22/06/2017 5:05 PM
"""

from __future__ import division
from core import env
from users.MH.Model_Tools.ModelTools import ModelTools
from users.MH.Waimak_modeling.supporting_data_path import sdp, temp_file_dir
from osgeo.gdal import Open as gdalOpen
import numpy as np
from users.MH.Waimak_modeling.model_tools.get_str_rch_values import get_ibound as gib

layers, rows, cols = 17, 364, 365

_mt = ModelTools('ex_bd_va', sdp='{}/ex_bd_va_sdp'.format(sdp), ulx=1512162.53275, uly=5215083.5772, layers=layers,
                 rows=rows, cols=cols, grid_space=200, temp_file_dir=temp_file_dir, base_mod_path=None)


def _elvdb_calc():  # todo, add pickle
    top = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/tops.tif".format(sdp)).ReadAsArray()
    top[np.isclose(top, -3.40282306074e+038)] = 0

    basement = _get_basement()

    bottom_layer1 = None  # todo define this

    # todo build up the layers based on the above
    outdata = None #todo

    #todo check that no layer is less than 50% of it's neighbor #todo implement in model tools

    waihora = _mt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/te_waihora.shp".format(_mt.sdp), 'ID', True)
    idx = np.isfinite(waihora)
    outdata[0][idx] = 1.5 #todo check... set DEM to lake level


    raise NotImplementedError

def _get_basement():
    basement = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/basement2.tif".format(sdp)).ReadAsArray()
    basement[np.isclose(basement, 9999999)] = np.nan
    basement = basement[1:,:]
    basement2 = np.concatenate((basement[:,:], np.repeat(basement[:, -1][:, np.newaxis], 33, axis=1)), axis=1)
    return basement2


def _no_flow_calc():  # todo, add pickle
    elv_db = _elvdb_calc()
    basement = _get_basement()
    constant_heads = _get_constant_heads()

    #todo watch the pockets of no-flow inside the model
    org_no_flow = gib() #todo is this wise given layering may change
    no_flow = np.zeros(_mt.layers,_mt.rows,_mt.cols)
    no_flow[:,:,]


    # load original
    # add on rows via repetition of zeros
    # convert shapefile to array and set all to 1 then add to the active boundry (set all <0 to 1)
    # set all of those greater than 1 to 1
    # load constant head cell lines/polygons and set those to -1 load polygons as -1


    raise NotImplementedError


def _get_constant_heads():

    outdata = np.zeros((_mt.layers, _mt.rows, _mt.cols))*np.nan
    # sea surface (north/south wai
    sea_val = None #todo
    sea = _mt.shape_file_to_model_array(None,'ID',True) #todo build shapefile
    idx = np.isfinite(sea)
    outdata[0][idx] = sea_val

    # te waihuro
    wai_val = 1.5 #todo check
    waihora = _mt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/te_waihora.shp".format(_mt.sdp), 'ID', True)
    idx = np.isfinite(waihora)
    outdata[0][idx] = wai_val

    # todo model boundry (sw)... how many layers for this
    # return a 3d array (layer, col, row) of constant heads values and np.nan for all others.


    raise NotImplementedError


smt = ModelTools(
    'ex_bd_va', sdp='{}/ex_bd_va_sdp'.format(sdp), ulx=1512162.53275, uly=5215083.5772, layers=layers, rows=rows,
    cols=cols, grid_space=200, no_flow_calc=_no_flow_calc, temp_file_dir=temp_file_dir, elv_calculator=_elvdb_calc,
    base_mod_path=None
)







if __name__ == '__main__':
   _get_basement()

   print 'done'