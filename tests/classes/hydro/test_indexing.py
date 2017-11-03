# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 15:34:32 2017

@author: MichaelEK
"""

from core.classes.hydro import hydro
import pytest
from os import path, getcwd
from geopandas import read_file

py_dir = path.realpath(path.join(getcwd(), path.dirname(__file__)))
netcdf1 = 'test_netcdf.nc'
poly_shp = 'test_poly.shp'

## Read in data
h1 = hydro().rd_netcdf(path.join(py_dir, netcdf1))
h1._base_stats_fun()
stats = h1._base_stats


## Test selection options
poly = read_file(path.join(py_dir, poly_shp)).set_index('site')


@pytest.mark.parametrize('poly_in', [path.join(py_dir, poly_shp), poly])
def test_sel_by_poly(poly_in):
    h2 = h1.sel_ts_by_poly(poly_in, 100, pivot=True)
    assert (len(h2.columns) == 4)
    h3 = h1.sel_by_poly(poly_in, 100)
    h3._base_stats_fun()
    assert (len(h3._base_stats) == 4)
    h4 = h1.sel_ts_by_poly(poly_in, 100, mtypes='flow', pivot=True, resample='W')
    assert (len(h4) == 11)


@pytest.mark.parametrize('mtypes', ['flow', 'flow_m'])
@pytest.mark.parametrize('sites', [[69607, 70105], 66])
@pytest.mark.parametrize('require', [None, [70105]])
@pytest.mark.parametrize('pivot', [True, False])
@pytest.mark.parametrize('resample', [None, 'W'])
def test_sel_ts(mtypes, sites, require, pivot, resample, start='2016-01-22', end='2016-03-01'):
    df1 = h1.sel_ts(mtypes, sites, require, pivot, resample, start, end)
    if ((mtypes == 'flow') & (sites == 66)):
        assert (len(df1) == 0)
    elif ((mtypes == 'flow_m') & (sites == [69607, 70105])):
        assert (len(df1) == 0)
    elif (sites == 66) & (require == [70105]):
        assert (len(df1) == 0)
    elif (mtypes == 'flow') & (sites == [69607, 70105]) & (pivot == False) & (resample == 'W'):
        assert (len(df1) > 0) & (len(h1.data) > len(df1))
    else:
        assert (len(df1) > 0)


def test_sel():
    h6 = h1.sel(mtypes='flow', sites=[70105, 69607], resample='W')
    h6._base_stats_fun()
    assert (len(h6._base_stats) == 2) & (h6._base_stats['count'][0] == 11)









