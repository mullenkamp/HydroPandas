"""
Author: matth
Date Created: 20/06/2017 11:57 AM
"""

from __future__ import division
from core import env
import flopy
from users.MH.Waimak_modeling.model_tools.well_values import get_race_data
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import mt
import pandas as pd

def create_wel_package(m,wel_version):

    wel = flopy.modflow.mfwel.ModflowWel(m,
                                         ipakcb=740,
                                         stress_period_data=_get_wel_spd(wel_version),
                                         options=['AUX IFACE'],
                                         unitnumber=709)

def _get_wel_spd(version): #todo
    races = get_race_data() #todo check this
    n_wai_wells = None #todo
    s_wai_wells = None #todo

    all_wells = pd.concat((races,n_wai_wells,s_wai_wells))
    out_data = {0: mt.convert_well_data_to_stresspd(all_wells)}



    raise NotImplementedError()
    return out_data