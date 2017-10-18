# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 17/10/2017 11:07 AM
"""

from __future__ import division
from core import env
from pykrige.ok3d import OrdinaryKriging3D
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import pickle
import numpy as np
import os
import netCDF4 as nc
import pandas as pd
import datetime
import sys
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.data_extraction.data_from_streams import \
    get_samp_points_df
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.cwms_index import get_zone_array_index
import matplotlib.pyplot as plt

depths = [5, 10, 15, 25, 30, 40, 50, 75, 100, 150, 200] #todo confirm with zeb

def get_mask(recalc=False):
    """
    define the mask for kriging from noflow array should be k,i,j
    :param recalc:
    :return:
    """
    pickle_path = os.path.join(smt.temp_pickle_dir, 'sd_grid_krig_mask.p')
    if os.path.exists(pickle_path) and not recalc:
        mask = pickle.load(open(pickle_path))
        return mask

    elv_db = smt.calc_elv_db()
    no_flow = smt.get_no_flow()
    xs, ys = smt.get_model_x_y(False)
    y, z, x = np.meshgrid(ys, depths, xs)
    z = elv_db[0] - z
    mask = np.zeros(smt.model_array_shape)
    zidx = np.repeat(get_zone_array_index('waimak')[np.newaxis,:,:],11,axis=0)
    mask[~zidx] = np.nan

    mask = mask.flatten()
    for i, (tx, ty, tz, m) in enumerate(zip(x.flatten(), y.flatten(), z.flatten(), mask)): # todo I could probably speed this up...
        if np.isnan(m):
            continue
        try:
            layer, row, col = smt.convert_coords_to_matix(tx, ty, tz)
            mask[i] = no_flow[layer, row, col]
        except AssertionError:
            mask[i] = 0
    mask = mask.reshape(smt.model_array_shape)
    mask = mask == 1
    pickle.dump(mask, open(pickle_path, 'w'))
    return mask


def krig_stream(data, stream): #todo check speed I may want to re-sample the grid minimally I should mask everything south of waimak
    # 3d krigging on x,y, depth x,y resolution of 200 m ? (e.g. model grid)
    x = data.loc[:, 'mx'].values
    y = data.loc[:, 'my'].values
    z = data.loc[:, 'depth'].values
    val = data.loc[:, stream].values
    mask = get_mask()
    grid_x, grid_y = smt.get_model_x_y(False)
    ok3d = OrdinaryKriging3D(x=x, y=y, z=z, val=val, variogram_model='spherical')
    # this returns a shape of z,y,x
    k3d_temp, ss3d_temp = ok3d.execute('masked', grid_x, grid_y, depths,
                             mask=mask,
                             backend='vectorized')  # this may cause a memory error if so switch to 'loop'

    k3d = k3d_temp.data
    k3d[k3d_temp.mask] = np.nan
    ss3d = ss3d_temp.data
    ss3d[ss3d_temp.mask] = np.nan

    return k3d, ss3d


def extract_all_stream_krig(data_path, outpath):
    """
    extract all streams from a given run and then save as a netcdf
    :param data_path: path to the extracted relative data
    :param outpath: path to save the nc file
    :return:
    """
    samp_points_df = get_samp_points_df()
    sites = list(samp_points_df[samp_points_df.m_type == 'swaz'].index)
    data = pd.read_csv(data_path)
    flux = data.loc[:, 'flux'].iloc[0]

    outfile = nc.Dataset(outpath, 'w')
    x, y = smt.get_model_x_y(False)
    # create dimensions
    outfile.createDimension('latitude', len(y))
    outfile.createDimension('longitude', len(x))
    outfile.createDimension('depth', len(depths))

    # create variables
    depth = outfile.createVariable('depth', 'f8', ('depth',), fill_value=np.nan)
    depth.setncatts({'units': 'm',
                     'long_name': 'depth',
                     'missing_value': np.nan})
    depth[:] = depths

    lat = outfile.createVariable('latitude', 'f8', ('latitude',), fill_value=np.nan)
    lat.setncatts({'units': 'NZTM',
                   'long_name': 'latitude',
                   'missing_value': np.nan})
    lat[:] = y

    lon = outfile.createVariable('longitude', 'f8', ('longitude',), fill_value=np.nan)
    lat.setncatts({'units': 'NZTM',
                   'long_name': 'longitude',
                   'missing_value': np.nan})
    lon[:] = x

    for site in sites:
        k3d, ss3d = krig_stream(data, site)
        site_k3d = outfile.createVariable('var_{}'.format(site), 'f8', ('depth', 'latitude', 'longitude'),
                                          fill_value=np.nan)
        site_k3d.setncatts({'units': 'None',
                            'long_name': 'variance of interpolated stream depletion from {}'.format(site),
                            'missing_value': np.nan})
        site_k3d[:] = k3d
        site_ss3d = outfile.createVariable('sd_{}'.format(site), 'f8', ('depth', 'latitude', 'longitude'),
                                           fill_value=np.nan)
        site_ss3d.setncatts({'units': 'percent of pumping',
                             'long_name': 'stream depletion from {}'.format(site),
                             'missing_value': np.nan})
        site_ss3d[:] = ss3d

    # set global attributes
    outfile.description = (
    'interpolated stream depletion (and variance) at steady state for flux: {} m3/day'.format(flux))
    outfile.history = 'created {}'.format(datetime.datetime.now().isoformat())
    outfile.source = 'original data: {}, script: {}'.format(data_path, sys.argv[0])
    outfile.flux = flux
    outfile.flux_units = 'm3/day'


def plot_all_streams_sd(nc_path, outdir):
    """
    plot all of the k3d and ss3d for each depth extract_all_stream_krig
    :param nc_path: path to the netcdf file created by
    :param outdir: directory to place everything
    :return:
    """
    data = nc.Dataset(nc_path)
    flux = data.flux
    for var in data.variables.keys():
        if var in ['longitude', 'latitude', 'depth']:
            continue
        varoutdir = os.path.join(outdir,var)
        if not os.path.exists(varoutdir):
            os.makedirs(varoutdir)

        temp = np.array(data.variables[var])
        for layer in range(len(depths)):
            fig, ax = smt.plt_matrix(temp[layer], vmin=0, vmax=100, cmap='RdBu',
                                     title='{} for flux: {}'.format(var,flux),base_map=True)  #todo vmin and vmax
            fig.savefig(os.path.join(varoutdir))
            plt.close(fig)



def plot_relationship_3_fluxes():  # todo I really don't know how to visualise this maybe hold off?
    raise NotImplementedError


if __name__ == '__main__':
    mask = get_mask()
    print('done')
