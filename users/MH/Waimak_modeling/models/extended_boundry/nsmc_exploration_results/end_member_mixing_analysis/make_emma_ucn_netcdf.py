# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/11/2017 2:58 PM
"""

from __future__ import division
from core import env
import netCDF4 as nc
from glob import glob
import flopy
import os
import numpy as np
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.nsmc_exploration_results.combine_nsmc_results.ucn_netcdf import \
    make_ucn_netcd
from users.MH.Waimak_modeling.models.extended_boundry.nsmc_exploration_results.combine_nsmc_results.nc_nums import \
    emma_nsmc_numbers


def make_netcd_endmember_mixing(nc_path):
    """
    make the netcdf for the End member mixing analysis
    :param nc_path: path to save the netcdf
    :return:
    """
    vars = ['coastal', 'inland']
    vars_base_paths = [
        env.gw_met_data("mh_modeling/data_from_gns/EM_coast_ucnrepo"),
        env.gw_met_data("mh_modeling/data_from_gns/EM_inland_ucnrepo")

    ]
    ucn_paths = {}
    for var, bp in zip(vars, vars_base_paths):
        temp_paths = []
        for i in emma_nsmc_numbers:
            if i > 0:
                temp_paths.append(os.path.join(bp, 'mt_aw_ex_{}_{}.ucn'.format(var, i)))
            elif i == -1:
                temp_paths.append(os.path.join(bp, 'mt_aw_ex_{}_philow.ucn'.format(var)))
            elif i == -2:
                temp_paths.append(os.path.join(bp, 'mt_aw_ex_{}_phiupper.ucn'.format(var)))
            else:
                raise ValueError('unexpected number in emma_nsmc_nums')

    description = """ endmember mixing analysis results for inland and coastal endmembers.  
    initial concentrations for each endmember was 1.  
    the coastal endmember is defined as the LSR from a line approximately drawn through 
    [Greendale, 2km south of Charing Cross, Noalan House, Kirwee, corner of worlingham rd, Arcadia] to the coast
    the inland endmember is defined as the LSR inland of the coastal member, as well as the influxes for the eyre river,
     Ashley tributaries, and cust inflow. we intended to set the selwyn streams and northern boundary flux (lowburn) to 
     inland, but a bug in the mt3d run meant that they were not interpreted as inland.  we did not re-run due to 
     programme time constraints LSR on both sides of the Waimakariri river were set
     we envisage a third endmember (i.e. alpine river water), but due to time constraints we are assuming that the sum 
     of all of these endmembers is 1 for each observation point and did not run the alpine river endmember"""

    make_ucn_netcd(nsmc_nums=emma_nsmc_numbers, ucn_paths=ucn_paths, units='fraction',
                   description=description, nc_path=nc_path)


if __name__ == '__main__':
    #todo debug
    make_netcd_endmember_mixing(env.gw_met_data("mh_modeling/netcdfs_of_key_modeling_data/emma_ucn.nc"))
