# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/11/2017 2:58 PM
"""

from __future__ import division
import netCDF4 as nc
from glob import glob
import flopy
import os
import numpy as np

layers = 11
rows = 364
cols = 365


def _get_kstkpers(bud_file, kstpkpers=None, rel_kstpkpers=None):
    """
    get the kstpkpers to use from either a list of kstpkpers or a list of relative kstpkpers
    :param bud_file: the budget file for the model in question
    :param kstpkpers: the kstkpers
    :param rel_kstpkpers:  the relative kstpkpers to use
    :return: 2d np array
    """
    if kstpkpers is not None and rel_kstpkpers is not None:
        raise ValueError('must define only one of kstpkpers or rel_kstpkpers')
    elif kstpkpers is not None:
        kstpkpers = np.atleast_2d(kstpkpers)
        if kstpkpers.shape[1] != 2:
            raise ValueError('must define both kstp and kper in kstpkpers')
        check = [(e[0], e[1]) not in bud_file.get_kstpkper() for e in kstpkpers]
        if any(check):
            raise ValueError('{} kstpkpers not in bud_file'.format(kstpkpers[check]))
    elif rel_kstpkpers is not None:
        if rel_kstpkpers == 'all':
            kstpkpers = np.atleast_2d(bud_file.get_kstpkper())
        else:
            rel_kstpkpers = np.atleast_1d(rel_kstpkpers)
            if rel_kstpkpers.ndim != 1:
                raise ValueError('too many dimensions for rel_kstpkpers')
            temp = bud_file.get_kstpkper()
            try:
                kstpkpers = np.atleast_2d([temp[e] for e in rel_kstpkpers])
            except IndexError as e_val:
                raise IndexError(e_val.message + ' only {} kstpkpers in bud_file'.format(len(bud_file.get_kstpkper())))
    elif kstpkpers is None and rel_kstpkpers is None:
        raise ValueError('must define one of kstpkpers or rel_kstpkpers')
    return kstpkpers


def make_netcd_endmember_mixing(base_dir, nc_path, run_types=('coastal', 'inland'), ucn_no_value=-1):  # todo chekc runtypes
    """
    creates as netcdf file for all of the ucn data
    :param base_dir: dir that holds all of the ucnfiles
    :param nc_path: the path to save the netcdf files
    :param run_types: they keys to locate the different end_members
    :param ucn_no_value: the no data value for the ucn file (will be convereted to np.nan)
    :return:
    """
    nc_file = nc.Dataset(nc_path, 'w')

    # make dimensions
    nc_file.createDimension('nsmc_num')
    nc_file.createDimension('layer', layers)
    nc_file.createDimension('row', rows)
    nc_file.createDimension('col', cols)

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

    run_paths = {}
    for runtype in run_types:  # this assumes that there are ucns for every model
        run_paths[runtype] = sorted(glob(os.path.join(base_dir, '*_{}_*.ucn'.format(runtype))))
        temp = nc_file.createVariable(runtype, 'f8', ('nsmc_num', 'layer', 'row', 'col'), fill_value=np.nan,
                                      zlib=True,)
        temp.setncatts({'units': 'fraction',
                        'long_name': runtype,
                        'missing_value': np.nan})

    # some checks
    if len(set([len(e) for e in run_paths.values()])) != 1 or len(run_paths.values()[0]) == 0:
        raise ValueError('paths are empty or are different lengths')
    temp = [[os.path.basename(i).split('_')[-1].replace('.ucn', '') for i in e] for e in
            run_paths.values()]
    if not all(e == temp[0] for e in temp):
        raise ValueError('sets are different between lists')

    # populate variables
    layer[:] = range(1, layers + 1)
    row[:] = range(1, rows + 1)
    col[:] = range(1, cols + 1)

    nsmc_nums = []
    for e in temp[0]:
        try:
            nsmc_nums.append(int(e))
        except ValueError:
            if e == 'philow':
                nsmc_nums.append(-1)
            elif e == 'phiupper':
                nsmc_nums.append(-2)
            else:
                raise ValueError('unexpected string: {}'.format(e))
    nsmc_num[:] = nsmc_nums

    for runtype in run_types:
        paths = run_paths[runtype]
        temp_out = np.zeros((len(nsmc_nums), layers, rows, cols)) * np.nan
        for i, path in enumerate(paths):
            ucn_file = flopy.utils.UcnFile(path)
            kstpkpers = _get_kstkpers(ucn_file, rel_kstpkpers=-1)  # get the last kstpkper

            temp_out[i] = ucn_file.get_data(kstpkper=kstpkpers[0])
        temp_out[np.isclose(temp_out, ucn_no_value)] = np.nan
        nc_file.variables[runtype][:] = temp_out

if __name__ == '__main__':
    make_netcd_endmember_mixing(r"C:\Users\MattH\Desktop\mt_aw_ex_coastal_phiupper",
                                r"C:\Users\MattH\Desktop\mt_aw_ex_coastal_phiupper\test3.nc",
                                ['coastal'])