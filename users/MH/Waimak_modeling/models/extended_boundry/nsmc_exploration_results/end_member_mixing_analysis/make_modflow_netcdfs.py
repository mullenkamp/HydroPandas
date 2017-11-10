# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 11/11/2017 10:54 AM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.nsmc_exploration_results.combine_nsmc_results import *


def make_modflow_netcdfs(hds_nc_path, bud_nc_path):

    hds_paths = [] #todo make from emma_nsmc_numbers
    sfo_paths = [] #todo make from emma_nsmc_numbers
    cbc_paths = [] #todo make from emma_nsmc_numbers

    make_hds_netcdf(nsmc_nums=emma_nsmc_numbers,hds_paths=hds_paths,nc_path=hds_nc_path)
    make_cellbud_netcdf(nsmc_nums=emma_nsmc_numbers,sfo_paths=sfo_paths,cbc_paths=cbc_paths,nc_path=bud_nc_path)

if __name__ == '__main__':
    #todo setup and debug/run
    print'done'