# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 11/10/2017 1:33 PM
"""

from __future__ import division
from core import env
import netCDF4 as nc
import numpy as np
import pandas as pd
from glob import glob
import os
import geopandas as gpd
import seaborn as sns
import matplotlib.pyplot as plt

zone_lats = {}
zone_lons = {}

bounds = gpd.read_file(
    r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\niwa_data_explore\lsr_test_zones_wgs.shp")
for _id, geom, zone, in bounds.itertuples(False, None):
    zone_lats[zone] = geom.bounds[1], geom.bounds[3]
    zone_lons[zone] = geom.bounds[0], geom.bounds[2]


def make_save_rcppast_year_amalg(variable, amalg_type, outpath, groupby=('year')):
    """

    :param variable:
    :param amalg_type: e.g np.sum
    :return:
    """
    if variable == 'pe':
        paths = glob(env.gw_met_data(
            r"niwa_netcdf\climate_projections\RCPpast\*\PE_VCSN_*_RCPpast_1971_2005_south-island_p05_daily_ECan.nc"))
        at = np.nanmean
    elif variable == 'rain':
        paths = glob(env.gw_met_data(
            r"niwa_netcdf\climate_projections\RCPpast\*\TotalPrecipCorr_VCSN_*_RCPpast_1971_2005_south-island_p05_daily_ECan.nc"))
        at = np.nansum
    outdata = []
    paths.append(r"Y:\VirtualClimate\vcsn_precip_et_2016-06-06.nc")
    for path in paths:
        model = os.path.basename(os.path.dirname(path))
        data = nc.Dataset(path)
        lat, lon = np.array(data['latitude']), np.array(data['longitude'])
        for zone in zone_lats.keys():
            lats = zone_lats[zone]
            lons = zone_lons[zone]
            lat_idx = np.where((lat >= lats[0]) & (lat <= lats[1]))
            lon_idx = np.where((lon >= lons[0]) & (lon <= lons[1]))
            temp_var = np.array(data[variable])
            temp_var[np.isclose(temp_var, data.variables[variable]._FillValue)] = np.nan
            print(np.isnan(temp_var).sum() / len(temp_var.flatten()))
            if path == r"Y:\VirtualClimate\vcsn_precip_et_2016-06-06.nc":
                temp_var = amalg_type(temp_var[lon_idx[0], :, :][:, lat_idx[0], :],
                                      axis=(1, 0))  # todo confirm this works both idx and axis
            else:
                temp_var = amalg_type(temp_var[:, lat_idx[0]][:, :, lon_idx[0]],
                                      axis=(2, 1))  # todo confirm this works both idx and axis
            years = [e.year for e in nc.num2date(np.array(data.variables['time']), data.variables['time'].units)]
            month = [e.month for e in nc.num2date(np.array(data.variables['time']), data.variables['time'].units)]
            temp_outdata = pd.DataFrame({'year': years, 'month': month, variable: temp_var})
            temp_outdata.loc[:, 'model'] = model
            temp_outdata.loc[:, 'zone'] = zone
            temp_outdata = temp_outdata.groupby(list(groupby) + ['model', 'zone']).aggregate(
                {variable: at}).reset_index()
            outdata.append(temp_outdata)

    outdata = pd.concat(outdata)
    outdata.to_csv(outpath)
    # todo debug one of these


def plt_data(plt_idx=['year'],title=None):
    base_dir = r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\niwa_data_explore"

    if plt_idx == ['year', 'month']:
        path = os.path.join(base_dir, 'year_month_zonal_pe_comp.csv')
        data = pd.read_csv(path)
        data.loc[:, 'datetime'] = [pd.datetime(y, m, 15) for y, m in
                                   data.loc[:, ['year', 'month']].itertuples(False, None)]
        x = 'datetime'
    elif plt_idx == ['year']:
        path = os.path.join(base_dir, 'year_zonal_pe_comp.csv')
        data = pd.read_csv(path)
        x = 'year'
    elif plt_idx == ['month']:
        path = os.path.join(base_dir, 'month_zonal_pe_comp.csv')
        data = pd.read_csv(path)
        x = 'month'
    else:
        raise ValueError('plt idx not implemented')

    g = sns.FacetGrid(data, col='zone')
    g.map_dataframe(sns.pointplot, x, 'pe', 'model', color=sns.color_palette("Set1", n_colors=8, desat=.5))
    g.add_legend()
    g.fig.suptitle(title)
    return g


def make_data():
    outdir = r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\niwa_data_explore"
    outpath = os.path.join(outdir, 'year_zonal_pe_comp.csv')
    make_save_rcppast_year_amalg('pe', np.nanmean, outpath, ['year'])

    outpath = os.path.join(outdir, 'month_zonal_pe_comp.csv')
    make_save_rcppast_year_amalg('pe', np.nanmean, outpath, ['month'])

    outpath = os.path.join(outdir, 'year_month_zonal_pe_comp.csv')
    make_save_rcppast_year_amalg('pe', np.nanmean, outpath, ['year', 'month'])


if __name__ == '__main__':
    make_data()
    g1 = plt_data(['year'],'year')
    g2 = plt_data(['year','month'],'month-year')
    g3 = plt_data(['month'],'month')
    plt.show()

