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


def create_drn_package(m, drn_version, wel_version, reach_version):
    drn_data = _get_drn_spd(wel_version=wel_version, reach_v=reach_version).loc[:,
               ['k', 'i', 'j', 'elev', 'cond']].to_records(False)
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

    # define grouping #todo chat with brioch about this/targets... might want to change grouping for current drains
    index_to_pass = drn_data.index
    for key in drn_dict:
        if ('str' in key or 'swaz' not in key or key == 'num7drain_swaz') and key not in ['waimak_drn', '']:
            continue

        cells = smt.model_where(drn_dict[key])
        drn_to_mark = pd.DataFrame(cells, columns=['i', 'j'], index=range(-1, -len(cells) - 1, -1))
        temp = pd.concat((drn_data, drn_to_mark))
        dup = temp.duplicated(['row', 'col'], False)
        drn_data.loc[dup.loc[index_to_pass], 'group'] = key

    drn_data.loc[np.isclose(drn_data.cond, 585.138611), 'group'] = 'cust_carpet'
    drn_data.loc[np.isclose(drn_data.cond, 5241.530762), 'group'] = 'ash_carpet'
    drn_data.loc[np.isclose(drn_data.cond, 20000.000000), 'group'] = 'chch_carpet'

    # todo the below is a shitty catch all... fix at somepoint
    drn_data.loc[drn_data[
                     'group'].isnull(), 'group'] = 'other'  # this catches a few drain cells that we're not super interested in

    # define zone
    drn_data['zone'] = 'n_wai'  # here this represents the old model
    drn_data.loc[np.in1d(drn_data['group'], ['chch_swaz', 'chch_carpet']), 'zone'] = 's_wai'

    # add a carpet drain south of the waimakariri to loosely represent the low land streams
    # only add drains where there are not other model conditions
    drain_to_add = smt.shape_file_to_model_array()  # todo create a shapefile with indexes 1 for the are s of chch and the otehr for selyn tewai
    index = np.zeros(smt.rows, smt.cols)

    # wel
    temp = smt.df_to_array(get_wel_spd(wel_version), 'k', True)[0]
    temp += 1
    index += temp

    # current drain
    temp = smt.df_to_array(drn_data, 'k')
    temp += 1
    index += temp

    # sfr2
    temp = smt.df_to_array((pd.DataFrame(_get_reach_data(reach_v), 'k')))
    temp += 1
    index += temp

    # constant_head/inactive
    temp = smt.get_no_flow(0)
    temp[temp < 0] = 0
    index += ~temp.astype(bool)

    index = index.astype(bool)
    drain_to_add[index] = np.nan

    top = smt.calc_elv_db()[0]
    for val, group in zip([], []):  # todo define groups/vals for the different drains
        temp = pd.DataFrame(smt.model_where(np.isclose(drain_to_add, val)), columns=['i', 'j'])
        temp['k'] = 0
        temp['zone'] = 's_wai'
        temp['group'] = group
        temp['cond'] = 20000
        for i in temp.index:
            row, col = temp.loc['i', 'j']
            temp.loc[i, 'elv'] = top[row, col]

        drn_data = pd.concat(drn_data, temp)

    # add the waimakariri drain up above the bridge # todo

    # check for null grouping
    if any(pd.isnull(drn_data['group'])):
        raise ValueError('some groups still null')
    raise NotImplementedError()


if __name__ == '__main__':
    test = _get_drn_spd()
