# -*- coding: utf-8 -*-
"""
Author: mattH
Date Created: 22/07/2017 9:12 PM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.sfr2_packages import _get_reach_data, _get_segment_data
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import get_wel_spd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.drn_packages import _get_drn_spd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.rch_packages import _get_rch
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import from_levels_and_colors
import os


if __name__ == '__main__':
    sfr_data = pd.DataFrame(_get_reach_data(smt.reach_v))
    sfr_data['bc_type'] = 'sfr'

    well_data = get_wel_spd(smt.wel_version).rename(columns={'row':'i','col':'j', 'layer':'k'})
    well_data['bc_type'] = 'well'

    drn_data = _get_drn_spd(smt.reach_v,smt.wel_version)
    drn_data['bc_type'] = 'drn'
    drn_data = drn_data.loc[~drn_data.group.str.contains('carpet')] # don't worry about carpet drains

    all_data = pd.concat((sfr_data,well_data,drn_data)).reset_index()
    all_data = all_data.loc[(all_data.i<190) & (all_data.j>200)]

    for i in all_data.index:
        row, col = all_data.loc[i,['i','j']]
        x,y = smt.convert_matrix_to_coords(row,col)
        all_data.loc[i,'mx'] = x
        all_data.loc[i,'my'] = y

    all_data.to_csv(r"C:\Users\MattH\Downloads\all_bcs.csv")