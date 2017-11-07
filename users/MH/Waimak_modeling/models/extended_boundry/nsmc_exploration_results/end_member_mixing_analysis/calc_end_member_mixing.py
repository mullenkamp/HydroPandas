# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/11/2017 5:00 PM
"""

from __future__ import division
from core import env
import os
import netCDF4 as nc
import numpy as np
import pandas as pd

# imports from my set up, done in a try loop to handle passing this to brioch
try:
    from users.MH.Waimak_modeling.models.extended_boundry.supporting_data_analysis.all_well_layer_col_row import get_all_well_row_col
except ImportError:
    def get_all_well_row_col():
        path = ''  # add the path to the csv with all wells
        if not os.path.exists(path):
            raise NotImplementedError('please update the path to all wells')
        out_data = pd.read_csv(path, index_col=0)
        return out_data


def calculate_endmember_mixing(nc_path, wells=(), str_drn_sites=()):
    """
    create a dictionary (keys = runtype) of dataframes(index=nsmc realisation, columns = sites) for each runtype in
    :param nc_path: path to netcdf file created by make_ucn_netcdf
    :param wells: well numbers as a list
    :param str_drn_sites: stream or drain sites
    :return:
    """
    runtypes = ('coastal', 'inland', 'river')

    nc_file = nc.Dataset(nc_path)

    missing = set(runtypes) - set(nc_file.variables.keys())
    if len(missing) > 1:
        raise ValueError('at least two of (coastal, inland, river) '
                         'must be present nc variables: {}'.format(nc_file.variables.keys()))

    outdict = {}
    for runtype in runtypes:
        outdict[runtype] = pd.DataFrame(index=nc_file.variables['nsmc_num'], columns=wells+str_drn_sites)

    for runtype in (set(runtypes) - missing):
        # todo pull data out of
        raise NotImplementedError

    if len(missing) == 1:
        # calculate the percentage of the missing well assuming all sum to 1
        missing_data = outdict[list(missing)[0]]
        missing_data.loc[:] = 1
        for runtype in (set(runtypes) - missing):
            missing_data = missing_data - outdict[runtype]

    return outdict

#todo figure out how to save and add x,y,z