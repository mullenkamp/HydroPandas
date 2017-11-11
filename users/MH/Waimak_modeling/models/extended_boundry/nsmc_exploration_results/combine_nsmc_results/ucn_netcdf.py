# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 11/11/2017 8:54 AM
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
import sys
import datetime


def make_ucn_netcd(nsmc_nums, ucn_paths, units, description, nc_path, ucn_no_value=-1):
    """
    creates as netcdf file for all of the ucn data
    :param nsmc_nums: list of nsmc numbers
    :param ucn_paths: dictionary {variable name:[ucn paths]} ucn paths same order as nsmc_nums
    :param units: either string or dictionary variable name: units) to be passed to the netcdf variable attribute\
    :param description: a description (str) to pass to the netcdf attribute
    :param nc_path: the path to save the netcdf files
    :param ucn_no_value: the no data value for the ucn file (will be convereted to np.nan)
    :return:
    """
    if not isinstance(ucn_paths, dict):
        raise ValueError('expected dictionary for unc_paths, see doc')

    nc_file = nc.Dataset(nc_path, 'w')

    # make dimensions
    nc_file.createDimension('nsmc_num', len(nsmc_nums))
    nc_file.createDimension('layer', smt.layers)
    nc_file.createDimension('row', smt.rows)
    nc_file.createDimension('col', smt.cols)

    # general attributes
    nc_file.description = description
    nc_file.history = 'created {}'.format(datetime.datetime.now().isoformat())
    nc_file.source = 'script: {}'.format(sys.argv[0])

    # set up layer col and row
    layer = nc_file.createVariable('layer', 'i1', ('layer',), fill_value=-9)
    layer.setncatts({'units': 'none',
                     'long_name': 'model layer',
                     'comments': '1 indexed',
                     'missing_value': -9})

    row = nc_file.createVariable('row', 'i1', ('row',), fill_value=-9)
    row.setncatts({'units': 'none',
                   'long_name': 'model row',
                   'comments': '1 indexed',
                   'missing_value': -9})

    col = nc_file.createVariable('col', 'i1', ('col',), fill_value=-9)
    col.setncatts({'units': 'none',
                   'long_name': 'model column',
                   'comments': '1 indexed',
                   'missing_value': -9})

    nsmc_num = nc_file.createVariable('nsmc_num', 'i2', ('nsmc_num',), fill_value=-9)
    nsmc_num.setncatts({'units': 'none',
                        'long_name': 'Null Space Monte Carlo Realisation Number',
                        'comments': 'unique identifier phi lower and phi upper are -1 and -2, respectively',
                        'missing_value': -9})
    nsmc_num[:] = nsmc_nums

    # some checks
    for var, paths in ucn_paths.items:
        if isinstance(units, str):
            addu = units
        elif isinstance(units, dict):
            addu = units[var]
        else:
            raise ValueError('units must be either dict or string, see doc')

        temp = nc_file.createVariable(var, 'f8', ('nsmc_num', 'layer', 'row', 'col'), fill_value=np.nan,
                                      zlib=True, )
        temp.setncatts({'units': addu,
                        'long_name': var,
                        'missing_value': np.nan})

        temp_ucn_file = flopy.utils.UcnFile(paths)
        kstpkper = _get_kstkpers(temp_ucn_file, rel_kstpkpers=-1)[0]  # get the last kstpkper
        if len(temp_ucn_file.get_kstpkper()) > 1:
            warn('more than one kstpkper for {}, using the last kstpkper which is {}'.format(var, kstpkper))
        for i, path in enumerate(paths):
            ucn_file = flopy.utils.UcnFile(path)
            temp_out = ucn_file.get_data(kstpkper=kstpkper)
            temp_out[np.isclose(temp_out, ucn_no_value)] = np.nan
            temp[i] = temp_out

            # todo debug
