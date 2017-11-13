# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 13/11/2017 12:12 PM
"""

from __future__ import division
from core import env
import numpy as np
import netCDF4 as nc
import flopy
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.data_extraction.data_at_wells import hds_no_data

def spot_check_hds(n):
    data = nc.Dataset(env.gw_met_data(r"mh_modeling\netcdfs_of_key_modeling_data\post_filter1_hds.nc"))
    nsmc_nums = data.variables['nsmc_num']
    check_nums = np.random.random_integers(1,len(nsmc_nums)-2,n)

    for i in check_nums:
        test_num = nsmc_nums[i]
        hds = flopy.utils.HeadFile(r"K:\mh_modeling\data_from_gns\hdsrepo\mf_aw_ex_{}.hds".format(test_num)).get_data((0,0))
        hds[np.isclose(hds,hds_no_data)] = np.nan

        nc_hds = np.array(data['heads'][i])
        out = (np.isclose(hds,nc_hds) | (np.isnan(hds) & np.isnan(nc_hds))).all()
        print('{}: {}'.format(test_num,out))

if __name__ == '__main__':
    spot_check_hds(10)