# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 16/07/2017 10:00 AM
"""

from __future__ import division
from core import env
import geopandas as gpd
import numpy as np

def id_aquifer():
    data = gpd.read_file(env.sci("Groundwater/Waimakariri/Groundwater/Numerical GW model/Model build and optimisation/targets/head_targets/for_extract.shp"))
    data = data.set_index('well')
    data = data.replace(-9999,np.nan)

    formations = [] #todo
    names = []

    for form, name in zip(formations, names):
        top_key = '{}_t'.format(form)
        bot_key = '{}_b'.format(form)
        idx = (data.loc[:,'elv_z'] > data.loc[:,bot_key]) & (data.loc[:,'elv_z'] < data.loc[:,top_key])


    print 'dope'


if __name__ == '__main__':
    id_aquifer()