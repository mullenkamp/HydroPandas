"""
Author: matth
Date Created: 15/06/2017 1:51 PM
"""

from __future__ import division
from core import env
import numpy as np
import xarray

def export_nc_for_mikes(station_names=None):
    """
    convert vcsn netcdf to a netcdf to input into mike shear
    :param station_names: a list of station names to include if None pulls all of them.
    :return:
    """

    temp = xarray.open_dataset(env.data('VirtualClimate/vcsn_precip_et_2016-06-06.nc'))

    df = temp.to_dataframe().reset_index()
    if station_names is not None:
        df = df[np.in1d(df['site'],station_names)]
    df = df.set_index(['x','y','time'])
    df = df.drop('site',axis=1)
    out = df.to_xarray()
    out = out.fillna(-99)
    out.to_netcdf(env.temp("Patrick/VCSN_x_Y_t.nc"))

if __name__ == '__main__':
    with open(env.temp('Patrick/stations_to_pull_out.txt')) as f:
        stations = f.readlines()
    stations = [e.strip() for e in stations]
    export_nc_for_mikes(stations)