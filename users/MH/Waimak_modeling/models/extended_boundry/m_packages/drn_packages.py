"""
Author: matth
Date Created: 20/06/2017 11:58 AM
"""

from __future__ import division
from core import env
import flopy
import numpy as np
import pandas as pd
from users.MH.Waimak_modeling.model_tools import get_drn_samp_pts_dict,get_base_drn_cells
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt

def create_drn_package(m,drn_version):
    drn_data = _get_drn_spd(drn_version).loc[:,['k', 'i', 'j', 'elev', 'cond']].to_records(False)
    flopy.modflow.mfdrn.ModflowDrn(m,
                                   ipakcb=740,
                                   stress_period_data={0:drn_data},
                                   unitnumber=710)



def _get_drn_spd(): #todo


    # load original drains
    drn_data = pd.DataFrame(get_base_drn_cells())
    drn_data = drn_data.rename(columns={'i':'row','j':'col','k':'layer'})

    # take away the cust ones (duplication to id from model where)
    drn_dict = get_drn_samp_pts_dict()
    drn_to_remove = pd.DataFrame(smt.model_where(drn_dict['num7drain_swaz']),columns=['row','col'])

    drn_data = pd.concat((drn_data,drn_to_remove))

    idx = ~drn_data.duplicated(['row','col'], keep=False)

    drn_data = drn_data.loc[idx]
    drn_data['group'] = None

    # define grouping
    index_to_pass = drn_data.index
    for key in drn_dict:
        if ('str' in key or 'swaz' not in key or key == 'num7drain_swaz') and key not in ['waimak_drn','']:
            continue

        cells = smt.model_where(drn_dict[key])
        drn_to_mark = pd.DataFrame(cells, columns=['row', 'col'],index=range(-1,-len(cells)-1,-1))
        temp = pd.concat((drn_data,drn_to_mark))
        dup = temp.duplicated(['row','col'],False)
        drn_data.loc[dup.loc[index_to_pass],'group'] = key

    drn_data.loc[np.isclose(drn_data.cond, 585.138611),'group'] = 'cust_carpet'
    drn_data.loc[np.isclose(drn_data.cond, 5241.530762),'group'] = 'ash_carpet'
    drn_data.loc[np.isclose(drn_data.cond, 20000.000000),'group'] = 'chch_carpet'

    #todo the below is a shitty catch all... fix at somepoint
    drn_data.loc[drn_data['group'].isnull(), 'group'] = 'other'  # this catches a few drain cells that we're not super interested in

    # define zone
    drn_data['zone'] = 'n_wai'  # here this represents the old model
    drn_data.loc[np.in1d(drn_data['group'],['chch_swaz','chch_carpet']),'zone'] = 's_wai'






    # add the waimakariri drain up above the bridge

    # add a carpet drain south of the waimakariri to loosely represent the low land streams?  #todo this package must be last


    # check for null grouping
    if any(pd.isnull(drn_data['group'])):
        raise ValueError('some groups still null')

    raise NotImplementedError()

if __name__ == '__main__':
    test = _get_drn_spd()