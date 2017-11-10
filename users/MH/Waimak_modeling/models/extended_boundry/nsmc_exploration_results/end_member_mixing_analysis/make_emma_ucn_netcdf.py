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
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.nsmc_exploration_results.combine_nsmc_results.ucn_netcdf import \
    make_ucn_netcd
from users.MH.Waimak_modeling.models.extended_boundry.nsmc_exploration_results.combine_nsmc_results.nc_nums import emma_nsmc_numbers


def make_netcd_endmember_mixing(nc_path):
    """
    make the netcdf for the End member mixing analysis
    :param nc_path: path to save the netcdf
    :return:
    """
    vars = ['coastal', 'inland']
    ucn_paths = {'coastal': [], #todo from emma_nc_numbers
                 'inland': []}  # todo


    make_ucn_netcd(emma_nsmc_numbers, ucn_paths, 'fraction', nc_path)


if __name__ == '__main__':
    make_netcd_endmember_mixing(r"C:\Users\MattH\Desktop\mt_aw_ex_coastal_phiupper",
                                r"C:\Users\MattH\Desktop\mt_aw_ex_coastal_phiupper\test3.nc",
                                ['coastal'])
