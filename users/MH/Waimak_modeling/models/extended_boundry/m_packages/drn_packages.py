"""
Author: matth
Date Created: 20/06/2017 11:58 AM
"""

from __future__ import division
from core import env
import flopy
import pandas as pd
from users.MH.Waimak_modeling.model_tools import get_drn_samp_pts_dict,get_base_drn_cells

def create_drn_package(m,drn_version):
    flopy.modflow.mfdrn.ModflowDrn(m,
                                   ipakcb=740,
                                   stress_period_data=_get_drn_spd(drn_version),
                                   unitnumber=710)



def _get_drn_spd(version): #todo


    # load original drains
    drn_data = pd.DataFrame(get_base_drn_cells())


    # take away the cust ones (duplication to id from model where)
    # add the waimakariri drain up above the bridge
    # add a carpet drain south of the waimakariri to loosely represent the low land streams?  better option?


    raise NotImplementedError()