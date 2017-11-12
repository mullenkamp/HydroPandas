# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 13/11/2017 10:25 AM
"""

from __future__ import division
from core import env
import pandas as pd
import numpy as np
import netCDF4 as nc
import os

def check_missing(nc_path):
    data = nc.Dataset(nc_path)
    variables = list(set(data.variables.keys()) - {u'layer',
                                                   u'row',
                                                   u'col',
                                                   u'nsmc_num',
                                                   u'crs',
                                                   u'latitude',
                                                   u'longitude'})
    nsmc_nums = np.array(data.variables['nsmc_nums'])

    outdata = pd.DataFrame(index=nsmc_nums, columns=variables, data=False)
    for var in variables:
        temp_data = np.array(data[var])
        for i, num in enumerate(nsmc_nums):
            outdata.loc[num,var] = np.isnan(temp_data[i]).all()

    outdata.to_csv(os.path.join(os.path.dirname(nc_path,'check_nan_{}'.format(os.path.basename(nc_path)))))

if __name__ == '__main__':
    check_missing(env.gw_met_data(r"mh_modeling\netcdfs_of_key_modeling_data\emma_unc.nc"))
    check_missing(env.gw_met_data(r"mh_modeling\netcdfs_of_key_modeling_data\post_filter1_hds.nc"))
    check_missing(env.gw_met_data(r"mh_modeling\netcdfs_of_key_modeling_data\post_filter1_cell_budgets.nc"))