# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 15:34:32 2017

@author: MichaelEK
"""
import pytest
import os
import pandas as pd
import geopandas as gpd
from hydropandas.core.base import Hydro

py_dir = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
csv_files = ['test_long1.csv', 'test_wide1.csv', 'test_wide2.csv']
rd_csv_param = {'test_long1.csv': {'freq_type': 'discrete', 'dformat': 'long', 'times': 'time', 'hydro_id': 'hydro_id', 'sites': 'site', 'values': 'data'}, 'test_wide1.csv': {'dformat': 'wide', 'hydro_id': 'hydro_id', 'freq_type': 'discrete', 'sites': 'site', 'multicolumn': True}, 'test_wide2.csv': {'dformat': 'wide', 'hydro_id': 'river / flow / rec / prime', 'freq_type': 'discrete', 'units': 'm**3/s'}}
extra_csv = 'test_combine.csv'
geo_shp = 'sites_geo.shp'
export_keys = ['mtypes', 'sites']
#netcdf1 = 'test_netcdf1.nc'
hdf1 = 'test_hdf.h5'
hydro_id_units = {'river / flow / mfield / prime': 'm**3/s', 'river / flow / rec / prime': 'l/s', 'river / wl / rec / prime': 'cm'}

#exist_units = h1.units.copy()

py_dir = r'E:\ecan\git\HydroPandas\hydropandas\tests'

#data = pd.read_csv(os.path.join(py_dir, csv_files[1]), parse_dates=True, infer_datetime_format=True, dayfirst=True, header=[0, 1], index_col=0)

#locals().update(add_data_param[csv_files[1]])
#
#data = rd_ts(os.path.join(py_dir, csv_files[2]))
#
#locals().update(add_data_param[csv_files[2]])
#locals().update(add_data_param[csv_files[0]])
#freq_type = {('river / flow / rec / qc', '65101'): 'discrete', ('river / flow / rec / qc', '69505'): 'discrete', ('river / flow / mfield / qc', '66'): 'mean'}
#freq_type = {'river / flow / rec / qc': 'discrete', 'river / flow / mfield / qc': 'discrete', 'river / wl / rec / qc': 'mean'}


@pytest.mark.parametrize('csv', csv_files)
def test_io_csv(csv):
    tparam = rd_csv_param[csv].copy()

    ## Read
    h1 = Hydro().rd_csv(os.path.join(py_dir, csv), **tparam)
    h1._base_stats_fun()
    assert (len(h1._base_stats) > 4)

    ## Write
    dformat = tparam['dformat']
    out_param = {}
    if dformat == 'long':
        out_param.update({'pivot': False})
    else:
        out_param.update({'pivot': True})
    h1.to_csv(os.path.join(py_dir, csv), **out_param)

    ## Read
    h1 = Hydro().rd_csv(os.path.join(py_dir, csv), **tparam)
    h1._base_stats_fun()
    assert (len(h1._base_stats) > 4)


## Base import
tparam = rd_csv_param[csv_files[0]]
h1 = Hydro().rd_csv(os.path.join(py_dir, csv_files[0]), **tparam)
h1._base_stats_fun()
h1_len = len(h1._base_stats)

## Combine test
h2 = Hydro().rd_csv(os.path.join(py_dir, extra_csv), **tparam)
h2._base_stats_fun()
h2_len = len(h2._base_stats)


def test_combine():
    h3 = h1.combine(h2)
    h3._base_stats_fun()
    assert (len(h3._base_stats) == (h1_len + h2_len))


### Test unit conversion
def test_unit_conversion():
    h4 = h1.to_units(hydro_id_units)
    h4._base_stats_fun()
    old_mean = h1._base_stats.loc['river / flow / rec / qc', 69505]['mean']
    new_mean = h4._base_stats.loc['river / flow / rec / qc', 69505]['mean']
    assert (new_mean >= 999 * old_mean)


### Test geo import
geo1 = gpd.read_file(os.path.join(py_dir, geo_shp))[['index', 'geometry']]
geo2 = geo1.set_index('index')


def test_add_geo_loc():
    h1.add_geo_point(geo2)
    assert (len(h1.geo_point) == 7)


def test_io_hdf():
    ## Read
    h4 = Hydro().rd_hdf(os.path.join(py_dir, hdf1))
    h4._base_stats_fun()
    assert (len(h4._base_stats) == 12)

    ## Write
    h4.to_hdf(os.path.join(py_dir, hdf1))

    ## Read
    h4 = Hydro().rd_hdf(os.path.join(py_dir, hdf1))
    h4._base_stats_fun()
    assert (len(h4._base_stats) == 12)



#def test_io_netcdf():
#    ## Read
#    h4 = hydro().rd_netcdf(os.path.join(py_dir, netcdf1))
#    h4._base_stats_fun()
#    assert (len(h4._base_stats) > 4)
#
#    ## Write
#    h4.to_netcdf(os.path.join(py_dir, netcdf1))
#
#    ## Read
#    h4 = hydro().rd_netcdf(os.path.join(py_dir, netcdf1))
#    h4._base_stats_fun()
#    assert (len(h4._base_stats) > 4)









