"""
Author: matth
Date Created: 20/06/2017 11:58 AM
"""

from __future__ import division
from core import env
import flopy
import numpy as np
import pandas as pd
from users.MH.Waimak_modeling.model_tools import get_drn_samp_pts_dict, get_base_drn_cells
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from sfr2_packages import _get_reach_data
from wel_packages import get_wel_spd
import geopandas as gpd


def create_drn_package(m, wel_version, reach_version):
    drn_data = _get_drn_spd(wel_version=wel_version, reach_v=reach_version).loc[:,
               ['k', 'i', 'j', 'elev', 'cond']].to_records(False)
    drn_data = drn_data.astype(flopy.modflow.ModflowDrn.get_default_dtype())

    flopy.modflow.mfdrn.ModflowDrn(m,
                                   ipakcb=740,
                                   stress_period_data={0: drn_data},
                                   unitnumber=710)

def _get_drn_spd(reach_v, wel_version):  # todo add pickle at some point


    # load original drains
    drn_data = pd.DataFrame(get_base_drn_cells())

    # take away the cust ones (duplication to id from model where)
    drn_dict = get_drn_samp_pts_dict()
    drn_to_remove = pd.DataFrame(smt.model_where(drn_dict['num7drain_swaz']), columns=['i', 'j'])

    drn_data = pd.concat((drn_data, drn_to_remove))

    idx = ~drn_data.duplicated(['i', 'j'], keep=False)

    drn_data = drn_data.loc[idx]
    drn_data['group'] = None

    # define grouping
    index_to_pass = drn_data.index
    for key in drn_dict:
        if ('str' in key or 'swaz' not in key or key == 'num7drain_swaz') and key not in ['waimak_drn', '']:
            continue

        cells = smt.model_where(drn_dict[key])
        drn_to_mark = pd.DataFrame(cells, columns=['i', 'j'], index=range(-1, -len(cells) - 1, -1))
        temp = pd.concat((drn_data, drn_to_mark))
        dup = temp.duplicated(['i', 'j'], False)
        drn_data.loc[dup.loc[index_to_pass], 'group'] = key

    carpet_groups = np.array(pd.read_table('{}/m_ex_bd_inputs/drncpt_3group.txt'.format(smt.sdp),
                                           delim_whitespace=True, header=None))
    carpet_groups = carpet_groups[np.isfinite(carpet_groups)].reshape((190,365))
    carpet_groups[np.isclose(carpet_groups, 0)] = np.nan
    for i in drn_data.index:
        row, col = drn_data.loc[i,['i','j']]
        drn_data.loc[i,'temp_carpet_group'] = carpet_groups[row,col]

    # todo do we want to remove the carpet drains in the N part or the model?
    if drn_data.loc[drn_data['temp_carpet_group'].notnull(),'group'].notnull().any():
        raise ValueError('carpet index yeilds non-carpet drain')
    drn_data.loc[np.isclose(drn_data.temp_carpet_group, 98), 'group'] = 'cust_carpet'
    drn_data.loc[np.isclose(drn_data.temp_carpet_group, 97), 'group'] = 'ash_carpet'
    drn_data.loc[np.isclose(drn_data.temp_carpet_group, 99), 'group'] = 'chch_carpet'

    # re-set carpet drains to layer top
    top = smt.calc_elv_db()[0]
    for i in drn_data.loc[np.in1d(drn_data.group,['cust_carpet', 'ash_carpet', 'chch_carpet'])].index:
        row, col = drn_data.loc[i,['i','j']]
        drn_data.loc[i,'elev'] = top[row, col]

    # todo the below is a shitty catch all... fix at somepoint
    drn_data.loc[drn_data[
                     'group'].isnull(), 'group'] = 'other'  # this catches a few drain cells that we're not super interested in

    # todo could also define a target group column (from brioch's shapefile), which would help things
    # define zone
    drn_data['zone'] = 'n_wai'  # here this represents the old model
    drn_data.loc[np.in1d(drn_data['group'], ['chch_swaz', 'chch_carpet']), 'zone'] = 's_wai'

    # te waihura
    # te waihora is defined at the 2m contour from the DEM around the lake to ensure that no weirdness happens when we
    # set the DEM to 1.5 m MSL
    wai_val = 1.5
    waihora = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/te_waihora.shp".format(smt.sdp), 'ID', True)
    temp = smt.get_no_flow(0)
    temp[temp < 0] = 0
    waihora[~temp.astype(bool)] = np.nan
    temp = pd.DataFrame(smt.model_where(np.isfinite(waihora)), columns=['i', 'j'])
    temp['k'] = 0
    temp['zone'] = 's_wai'
    temp['group'] = 'waihora'
    temp['cond'] = 30000
    temp['elev'] = wai_val
    drn_data = pd.concat((drn_data, temp)).reset_index().drop('index',axis=1)

    # add a carpet drain south of the waimakariri to loosely represent the low land streams
    # only add drains where there are not other model conditions
    drain_to_add = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/s_carpet_drns.shp".format(smt.sdp),'group',alltouched=True)
    index = np.zeros((smt.rows, smt.cols))

    # wel
    temp = smt.df_to_array(get_wel_spd(wel_version), 'col', True)[0]
    index[np.isfinite(temp)] = 1

    # current drain
    temp = smt.df_to_array(drn_data, 'j')
    index[np.isfinite(temp)] = 1

    # sfr2
    temp = smt.df_to_array(pd.DataFrame(_get_reach_data(reach_v)), 'j')
    index[np.isfinite(temp)] = 1

    # constant_head/inactive
    temp = smt.get_no_flow(0)
    temp[temp < 0] = 0
    index[~temp.astype(bool)] = 1

    index[np.isnan(index)]=0
    index = index.astype(bool)
    drain_to_add[index] = np.nan

    top = smt.calc_elv_db()[0]
    for val, group in zip([1,3,2,5,4], ['chch_carpet', 'up_lincoln','down_lincoln', 'up_selwyn', 'down_selwyn']):
        temp = pd.DataFrame(smt.model_where(np.isclose(drain_to_add, val)), columns=['i', 'j'])
        temp['k'] = 0
        temp['zone'] = 's_wai'
        temp['group'] = group
        temp['cond'] = 20000
        for i in temp.index:
            row, col = temp.loc[i, ['i', 'j']]
            temp.loc[i, 'elev'] = top[row, col]
        drn_data = pd.concat((drn_data, temp))

    # add the waimakariri drain up above the bridge
    # set a drain at the top of the ground level
    drain_to_add = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/upper_waimak_drn.shp".format(smt.sdp), 'ID',
                                                 alltouched=True)
    temp = pd.DataFrame(smt.model_where(np.isfinite(drain_to_add)), columns=['i', 'j'])
    temp['k'] = 0
    temp['zone'] = 'n_wai'
    temp['group'] = 'up_waimak'
    temp['cond'] = 20000
    for i in temp.index:
        row, col = temp.loc[i,['i', 'j']]
        temp.loc[i, 'elev'] = top[row, col]

    drn_data = pd.concat((drn_data, temp))

    # check for null grouping
    # remove all drains in no-flow boundries or constant head
    no_flow = smt.get_no_flow()[0]
    no_flow[no_flow<0]=0
    idxs = pd.DataFrame(smt.model_where(~no_flow.astype(bool)), columns=['i','j'])
    drn_data.loc[:,'is_drn'] = True
    temp_df = pd.concat((drn_data, idxs))
    drn_data = temp_df.loc[~temp_df.duplicated(['i','j'],False) & (temp_df.loc[:,'is_drn'])].reset_index()
    drn_data = drn_data.drop(['is_drn','index', 'temp_carpet_group'], axis=1)

    # add new shapefile drn elevations keep the lower of the new or the old - 2
    # with the exception of upper waimak drn which is 0 for some reason
    man_drn_data = gpd.read_file(env.sci("Groundwater/Waimakariri/Groundwater/Numerical GW model/Model build and optimisation/stream_drn_values/updated_drains_MH.shp"))

    elv = smt.calc_elv_db()
    for i in man_drn_data.index:
        row, col = man_drn_data.loc[i,['i','j']]
        cell_elv = elv[0,row,col]
        old = man_drn_data.loc[i,'elev']
        new = man_drn_data.loc[i,'Elev_use']

        if man_drn_data.loc[i,'group'] == 'up_waimak': # do not inset upper waimak drains
            el_to_use = new
            if el_to_use > cell_elv:
                el_to_use = cell_elv
        else:
            if old > new - 2: #inset all new drain elv 2 m
                el_to_use = new - 2
            else:
                el_to_use = old

            if el_to_use > cell_elv - 2:
                el_to_use = cell_elv - 2

        man_drn_data.loc[i,'elev_to_use'] = el_to_use

    # set new elevations note there will be acouple of cells that weren't linked due to a labeling problem... just the way it's going to be
    man_drn_data = man_drn_data.set_index(['i','j'])
    drn_data = drn_data.set_index(['i','j'])
    from copy import deepcopy #todo DADB
    for_test = deepcopy(drn_data) #todo DADB
    drn_data.loc[man_drn_data.index,'elev'] = man_drn_data.loc[:,'elev_to_use']
    drn_data = drn_data.reset_index()


    # fix elevation of a couple of chch and other swaz
    for i in drn_data.loc[np.in1d(drn_data.group,['chch_swaz','other'])].index:
        row, col = drn_data.loc[i,['i','j']]
        cell_elv = elv[0,row,col]
        old = drn_data.loc[i, 'elev']
        if cell_elv-2 < old:
            drn_data.loc[i,'elev'] = cell_elv - 2

    # add the ashley estuary and remove any carpet drains that overlap
    temp_path = env.sci("Groundwater/Waimakariri/Groundwater/Numerical GW model/Model build and optimisation/stream_drn_values/ashley estuary.shp")
    drain_to_add = smt.shape_file_to_model_array(temp_path, 'ID', alltouched=True)
    drain_to_add = pd.DataFrame(smt.model_where(np.isfinite(drain_to_add)), columns=['i','j'])
    drain_to_add['k'] = 0
    drain_to_add['zone'] = 'n_wai'
    drain_to_add['group'] = 'ashley_estuary'
    drain_to_add['cond'] = 20000
    drain_to_add['elev'] = 1.14

    drn_data = pd.concat((drn_data,drain_to_add))
    idx = drn_data.duplicated(subset=['i','j'], keep='last')

    if (drn_data.loc[idx,'group'] != 'ash_carpet').any():
        raise ValueError('{} groups overlapping ashley estuary, consider'.format(set(np.array(drn_data.loc[idx,'group']))))
    drn_data = drn_data.loc[~idx]
    drn_data = drn_data.reset_index(drop=True)


    # check that no drain cells are below the bottom of layer 1 or above ground!
    elv = smt.calc_elv_db()
    drn_elv = smt.df_to_array(drn_data,'elev')
    if any((drn_elv > elv[0]).flatten()):
        raise ValueError('drains with elevation above surface')

    if any((drn_elv <= elv[1]).flatten()): #todo some of the ohoka is quite thin (check)
        raise ValueError('drains below layer 1')

    if any(pd.isnull(drn_data['group'])):
        raise ValueError('some groups still null')
    return drn_data


if __name__ == '__main__':
    test = _get_drn_spd(1,1)
    test2 = test.loc[np.in1d(test.group, [
        'ashley_swaz',
        'cam_swaz',
        'courtenay_swaz',
        'greigs_swaz',
        'kaiapoi_swaz',
        'kairaki_swaz',
        'northbrook_swaz',
        'ohoka_swaz',
        'other',
        'saltwater_swaz',
        'southbrook_swaz',
        'taranaki_swaz',
        'up_waimak',
        'waikuku_swaz',
        'waimak_drn'
    ])]
    for i in test2.index:
        row, col = test.loc[i,['i','j']].astype(int)
        x,y = smt.convert_matrix_to_coords(row,col)
        test2.loc[i,'nztmx'] = x
        test2.loc[i,'nztmy'] = y

    smt.plt_matrix(smt.df_to_array(test2,'i'))
    print('done')
