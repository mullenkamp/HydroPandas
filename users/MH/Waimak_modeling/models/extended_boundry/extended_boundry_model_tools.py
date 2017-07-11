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
import pandas as pd
import time
from pykrige.ok import OrdinaryKriging

layers, rows, cols = 11, 364, 365

_mt = ModelTools('ex_bd_va', sdp='{}/ex_bd_va_sdp'.format(sdp), ulx=1512162.53275, uly=5215083.5772, layers=layers,
                 rows=rows, cols=cols, grid_space=200, temp_file_dir=temp_file_dir, base_mod_path=None)


def _elvdb_calc():
    top = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/tops.tif".format(sdp)).ReadAsArray()
    top[np.isclose(top, -3.40282306074e+038)] = 0


    # bottoms
    # layer 0
    bot0 = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/layering/base_layer_1.tif".format(sdp)).ReadAsArray()
    idx = np.isclose(bot0, -3.40282306074e+038)
    bot0[idx] = top[idx] - 10 #set nan values to 10 m thick.  all these are out of the no-flow bound

    num_layers = 9
    layer_names =[
        'ricc',
        'brom',
        'lin',
        'heath',
        'bur',
        'shirl',
        'wainoni',
        'deep_divide1',
        'deep_divide2'
    ]
    thicknesses = {  #todo check verticle offset of targets, particularly vertical gradient targets
        # these values came from the median thickness from the GNS model in the confined zone
        # (defined by the extent of the ashely and chch formation)  this is a bit loose but decent given time
        'ricc': 15, #todo may need to smooth this layer inland? it may be worth making this layer non-uniform
        'brom': 15,
        'lin': 25,
        'heath': 15,
        'bur': 15, #todo this could pose a problem for the steep sections of the model layering
        'shirl': 15,
        'wainoni': 20,
        # seems to be target (though with minimal data) in two groups 1 from 20-30 the other from 40-80
        'deep_divide1': 40, #this one was carefully thought about to ensure that there was the potential to grab targets (assuming no changes in the above layers)
        'deep_divide2': 40 #todo this captures our deepest wells but means no targets in teh bottom of the model.
    }
    outdata = np.zeros((len(layer_names)+3,_mt.rows,_mt.cols))
    outdata[0] = top
    outdata[1] = bot0

    for i,name in enumerate(layer_names):
        layer = i+2
        thickness = thicknesses[name]
        outdata[layer] = outdata[layer-1] - thickness

    # bottom of the model
    basement = _get_basement()
    thickness = outdata[-2] - basement
    thickness[thickness < 20] = 20
    outdata[-1] = outdata[-2] - thickness

    waihora = _mt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/te_waihora.shp".format(_mt.sdp), 'ID', True)
    idx = np.isfinite(waihora)
    outdata[0][idx] = 1.5

    return outdata

def _get_basement():
    basement = gdalOpen("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/basement2.tif".format(sdp)).ReadAsArray()
    basement[np.isclose(basement, 9999999)] = np.nan
    basement = basement[1:,:]
    basement2 = np.concatenate((basement[:,:], np.repeat(basement[:, -1][:, np.newaxis], 33, axis=1)), axis=1)
    return basement2


def _no_flow_calc(): #todo check the intersection with ibounds...
    no_flow = np.zeros((_mt.rows,_mt.cols))
    outline = _mt.shape_file_to_model_array("{}/ex_bd_va_sdp/m_ex_bd_inputs/shp/new_active_domain.shp".format(sdp),'DN',True)
    no_flow[np.isfinite(outline)] = 1

    no_flow = np.repeat(no_flow[np.newaxis,:,:],_mt.layers, axis=0)
    # convert shapefile to array and set all to 1 then add to the active boundry (set all <0 to 1)


    # propogate constant head of the sea down as active cells (handle mis-match of outline and sea values
    sea = _mt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/coastal_constant_heads.shp".format(_mt.sdp),'ID',True)
    for i in range(1,_mt.layers):
        no_flow[i][np.isfinite(sea)] = 1

    # set no flow from boundry
    tops = _elvdb_calc()[0:_mt.layers]
    if len(tops) != _mt.layers:
        raise ValueError('wrong number of layers returned')
    basement = np.repeat(_get_basement()[np.newaxis,:,:], _mt.layers, axis=0)
    no_flow[basement >= tops] = 0

    #todo check the pockets of no-flow inside the model, and general domain

    #set constant heads  to -1
    constant_heads = _get_constant_heads()
    no_flow[np.isfinite(constant_heads)] = -1




    return no_flow



def _get_constant_heads():

    outdata = np.zeros((_mt.layers, _mt.rows, _mt.cols))*np.nan
    # te waihura
    # te waihora is defined at the 2m contour from the DEM around the lake to ensure that no weirdness happens when we
    # set the DEM to 1.5 m MSL
    wai_val = 1.5
    waihora = _mt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/te_waihora.shp".format(_mt.sdp), 'ID', True)
    idx = np.isfinite(waihora)
    outdata[0][idx] = wai_val

    # sea surface (north/south wai
    first_sea_val = 0.5
    rest_sea_val = 0
    sea = _mt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/coastal_constant_heads.shp".format(_mt.sdp),'ID',True)
    sea[np.isfinite(sea)] = rest_sea_val

    # set east most value to first sea for the north part of the model
    for i in range(0,300):
        if all(np.isnan(sea[i])):
            continue
        j = np.where(np.isfinite(sea[i]))[0].min()
        sea[i,j] = first_sea_val

    temp = sea[300:,:]
    for j in range(309):
        if all(np.isnan(temp[:,j])):
            continue

        i = np.where(np.isfinite(temp[:,j]))[0].min()
        sea[i+300,j] = first_sea_val

    outdata[0][np.isfinite(sea)] = sea[np.isfinite(sea)]
    # propogate constant head down as active cells
    for i in range(1,_mt.layers):
        outdata[i][np.isfinite(sea)] = 1

    #todo handle the basement.... so that it is defined and not overwritten by constant heads...

    # todo model boundry? (sw)... how many layers for this
    # return a 3d array (layer, col, row) of constant heads values and np.nan for all others.

    return outdata


def create_sw_boundry(): #todo definetly add pickle to this one
    #todo I'm not sure if this is teh best option for our model.

    # 0 identify the sampling points for the boundry (x,y,z) space
    # the bottom layer should probably be no-flow as I do not have any basis for interpolation

    boundry = _mt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/s_boundry_line.shp".format(_mt.sdp),'ID',True)
    boundry = boundry[np.newaxis,:,:].repeat(_mt.layers, axis = 0)
    elv = _elvdb_calc()
    basement = _get_basement()
    tops = elv[0:_mt.layers]
    if len(tops) != _mt.layers:
        raise ValueError('wrong number of layers returned')
    basement = np.repeat(_get_basement()[np.newaxis,:,:], _mt.layers, axis=0)
    boundry[basement >= tops] = np.nan

    # set bottom layer to nan we have no values to interpolate off of #todo do we have data for the other layers?
    boundry[-1,:,:] = np.nan
    locations = pd.DataFrame(_mt.model_where(np.isfinite(boundry)), columns=['k','i','j'])
    for cell in locations.index:
        k, i, j = locations.loc[cell,['k','i','j']].astype(int)
        x,y,z = _mt.convert_matrix_to_coords(i, j, k, elv)
        locations.loc[cell,'x'] = x
        locations.loc[cell,'y'] = y
        locations.loc[cell,'z'] = z


    # 2 Id the radius of influance (or include in montecarlo approximations) other script looking at the data I chose 10km
    # 3 Identify which points to use (and get good coverage) and create PDFs for each of the input points
    # the below wells were selected based on a querry of all targets that were within 10 km of the boundry line and
    # those that had at least 20 readings.  an exception was made for teh deeper wells layers (7-10) where all wells
    # were included regardless of of the number of readings.  this was done to increase the spatial coverage of deep data

    with open("{}/m_ex_bd_inputs/wells_to_use_for_s_boundry.txt".format(_mt.sdp)) as f:
        wells = f.readlines()
    wells = [e.strip() for e in wells]

    all_targets = pd.read_csv(env.sci(
        "Groundwater/Waimakariri/Groundwater/Numerical GW model/Model build and optimisation/targets/head_targets/head_targets_2008_offsets.csv"),index_col=0)
    data = all_targets.loc[wells]
    data = data.rename(columns={'nztmx':'x', 'nztmy':'y', 'mid_screen_elv':'z', 'h2o_elv_mean':'d', 'total_error_m':'sd'})
    data = data.dropna(subset=['x','y','d'])

    # 4 run monte calo simulation from the point pdfs to create a pdf for each point on the grid
    # 1 id which interpolation technique to use (try just eh default of multinomial?)

    for i in range(10):
        print i
        idx = locations.k == i
        temp = data.loc[data.layer==i]
        print len(temp)
        ok2d = OrdinaryKriging(np.array(temp.x),np.array(temp.y),np.array(temp.d))

        k, s = ok2d.execute('points',np.array(locations.loc[idx,'x']), np.array(locations.loc[idx,'y']))

        locations.loc[idx,'krig_data'] = k.data
    import matplotlib.pyplot as plt
    for i in range(10):
        temp = locations[locations.k==i]
        plt.scatter(range(len(temp)),temp.krig_data,label=i)
    plt.legend()
    return locations #todo check it;s not above top

smt = ModelTools(
    'ex_bd_va', sdp='{}/ex_bd_va_sdp'.format(sdp), ulx=1512162.53275, uly=5215083.5772, layers=layers, rows=rows,
    cols=cols, grid_space=200, no_flow_calc=_no_flow_calc, temp_file_dir=temp_file_dir, elv_calculator=_elvdb_calc,
    base_mod_path=None
)

# quick versioning
model_version = 'a'

if model_version == 'a':
    smt.model_version = 'a'
    smt.sfr_version, smt.seg_v, smt.reach_v = 1, 1, 1
    smt.k_version = 1
    smt.wel_version = 1



if __name__ == '__main__':
   start = time.time()

   test = _no_flow_calc()

   print('took {} seconds'.format((time.time()-start)))
