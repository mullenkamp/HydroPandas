# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 14/11/2017 7:34 PM
"""

from __future__ import division
from core import env
from obs_2d_mean_sd import plot_sd_mean_multid, no_change
import socket
import numpy as np
import netCDF4 as nc
import os
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import matplotlib.pyplot as plt

def _above_mav(x, **kwargs):
    return x >= 11.3

def _above_half_mav(x, **kwargs):
    return x >= 5.65

def _above_quarter_mav(x, **kwargs):
    return x >= 2.825

def _above_low(x, **kwargs):
    return x >= 1


def plot_all_2d_con(outdir, filter_strs):
    if socket.gethostname() != 'GWATER02':
        raise ValueError('this must be run on GWATER02 as that is where the uncompressed data is stored')

    nc_param_data = nc.Dataset(r"K:\mh_modeling\netcdfs_of_key_modeling_data\nsmc_params_obs_metadata.nc")
    emma = nc.Dataset(r"C:\mh_waimak_model_data\emma_con.nc")
    bestn = nc.Dataset(r"C:\mh_waimak_model_data\mednload_ucn.nc")

    if not os.path.exists(outdir):
        os.makedirs(outdir)
    for l in range(smt.layers):
        title = 'coastal component for layer: {:02d}'.format(l)
        fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=l,
                                       nc_param_data=nc_param_data, nc_obs_data=emma,
                                       data_id='coastal', function_adjust=no_change,
                                       title=title,
                                       basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                       method='mean_sd', contour_color='g')

        fig.savefig(os.path.join(outdir, title))
        plt.close()

    for l in range(smt.layers):
        title = 'inland component for layer: {:02d}'.format(l)
        fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=l,
                                       nc_param_data=nc_param_data, nc_obs_data=emma,
                                       data_id='inland', function_adjust=no_change,
                                       title=title,
                                       basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                       method='mean_sd', contour_color='g')

        fig.savefig(os.path.join(outdir, title))
        plt.close()

    for l in range(smt.layers):
        title = 'river component for layer: {:02d}'.format(l)
        fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=l,
                                       nc_param_data=nc_param_data, nc_obs_data=emma,
                                       data_id='river', function_adjust=no_change,
                                       title=title,
                                       basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                       method='mean_sd', contour_color='g')

        fig.savefig(os.path.join(outdir, title))
        plt.close()

    for l in range(smt.layers):
        title = 'best N estimate (g/m3) for layer: {:02d}'.format(l)
        fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=l,
                                       nc_param_data=nc_param_data, nc_obs_data=bestn,
                                       data_id='inland', function_adjust=no_change,
                                       title=title,
                                       basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                       method='mean_sd', contour_color='g')

        fig.savefig(os.path.join(outdir, title))
        plt.close()

    for limit, ftn in zip([1, 2.85, 5.65, 11.3], [_above_low,_above_quarter_mav, _above_half_mav, _above_mav]):
        for l in range(smt.layers):
            title = 'N above {} (g/m3) for layer: {:02d}'.format(limit, l)
            fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=l,
                                           nc_param_data=nc_param_data, nc_obs_data=bestn,
                                           data_id='inland', function_adjust=ftn,
                                           title=title,
                                           basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                           method='mean_sd', contour_color='g')

            fig.savefig(os.path.join(outdir, title))
            plt.close()


# both 2D spatial as well as point measurements at wells for example
# take for granted that only 2000 something were run when looking through filters

def plot_all_1d_con(outdir):
    # watch nans for boxplots
    raise NotImplementedError