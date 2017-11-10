# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 11/11/2017 8:51 AM
"""

from __future__ import division
from core import env
import netCDF4 as nc
import flopy
import numpy as np
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.data_extraction.data_at_wells import \
    _get_kstkpers, hds_no_data
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from warnings import warn


def make_hds_netcdf(nsmc_nums, hds_paths, nc_path):
    """
    make a cell budget file netcdf for easy use
    :param nsmc_nums: list the unique identifiers for the netcdfs
    :param hds_paths: list the paths for the hds files of a given netcdf number (same order as nsmc_nums)
    :param nc_path: the path to the outfile
    :return:
    """
    nc_file = nc.Dataset(nc_path, 'w')

    # make dimensions
    nc_file.createDimension('nsmc_num', len(nsmc_nums))
    nc_file.createDimension('layer', smt.layers)
    nc_file.createDimension('row', smt.rows)
    nc_file.createDimension('col', smt.cols)

    # set up layer col and row
    layer = nc_file.createVariable('layer', 'i1', ('layer',), fill_value=-9)
    layer.setncatts({'units': 'none',
                     'long_name': 'model layer',
                     'comments': '1 indexed',
                     'missing_value': -9})
    layer[:] = range(1, smt.layers + 1)

    row = nc_file.createVariable('row', 'i1', ('row',), fill_value=-9)
    row.setncatts({'units': 'none',
                   'long_name': 'model row',
                   'comments': '1 indexed',
                   'missing_value': -9})
    row[:] = range(1, smt.rows + 1)

    col = nc_file.createVariable('col', 'i1', ('col',), fill_value=-9)
    col.setncatts({'units': 'none',
                   'long_name': 'model column',
                   'comments': '1 indexed',
                   'missing_value': -9})
    col[:] = range(1, smt.cols + 1)

    nsmc_num = nc_file.createVariable('nsmc_num', 'i2', ('nsmc_num',), fill_value=-9)
    nsmc_num.setncatts({'units': 'none',
                        'long_name': 'Null Space Monte Carlo Realisation Number',
                        'comments': 'unique identifier phi lower and phi upper are -1 and -2, respectively',
                        'missing_value': -9})
    nsmc_num[:] = nsmc_nums

    # get the last kstpkper
    temp = flopy.utils.CellBudgetFile(hds_paths[0])
    kstpkper = _get_kstkpers(temp, rel_kstpkpers=-1)[0]
    if len(temp.get_kstpkper()) > 1:
        warn('more than one kstpkper, using the last kstpkper which is {}'.format(kstpkper))

    # create variable
    dim = ('nsmc_num', 'layer', 'row', 'col')
    all_hds = nc_file.createVariable('heads', 'f8', dim, fill_value=np.nan, zlib=True)
    all_hds.setncatts({'units': 'm',
                       'long_name': 'modelled heads',
                       'missing_value': np.nan})

    # add data
    for (i, nsmc_num), hd_path, in zip(enumerate(nsmc_nums), hds_paths):
        hds = flopy.utils.HeadFile(hd_path)
        temp_data = hds.get_data(kstpkper=kstpkper)
        temp_data[np.isclose(temp_data, hds_no_data)] = np.nan  # todo check no data is still the same
        all_hds[i] = temp_data

# todo debug
