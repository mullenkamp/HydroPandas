# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 15:34:32 2017

@author: MichaelEK
"""
import os
import pytest
import geopandas as gpd
from hydropandas.core.base import Hydro


py_dir = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
#py_dir = r'E:\ecan\git\HydroPandas\hydropandas\tests'

hdf1 = 'test_hdf.h5'
poly_shp = 'test_poly.shp'

## Read in data
h1 = Hydro().rd_hdf(os.path.join(py_dir, hdf1))
h1._base_stats_fun()
stats = h1._base_stats


## Test selection options
poly = gpd.read_file(os.path.join(py_dir, poly_shp)).set_index('site')


@pytest.mark.parametrize('poly_in', [os.path.join(py_dir, poly_shp), poly])
def test_sel_by_poly(poly_in):
    h2 = h1.sel_ts_by_poly(poly_in, 100, pivot=True)
    assert (len(h2.columns) == 4)
    h3 = h1.sel_by_poly(poly_in, 100)
    h3._base_stats_fun()
    assert (len(h3._base_stats) == 4)
    h4 = h1.sel_ts_by_poly(poly_in, 100, hydro_id='river / flow / rec / qc', pivot=True)
    assert (len(h4) == 68)


@pytest.mark.parametrize('hydro_id', ['river / flow / rec / qc', 'river / flow / mfield / qc'])
@pytest.mark.parametrize('sites', [[69607, 70105], 66])
@pytest.mark.parametrize('require', [None, [70105]])
@pytest.mark.parametrize('pivot', [True, False])
def test_sel_ts(hydro_id, sites, require, pivot, start='2016-01-22', end='2016-03-01'):
    df1 = h1.sel_ts(hydro_id, sites, require, pivot, start, end)
    if ((hydro_id == 'river / flow / rec / qc') & (sites == 66)):
        assert df1.empty
    elif ((hydro_id == 'river / flow / mfield / qc') & (sites == [69607, 70105])):
        assert df1.empty
    elif (sites == 66) & (require == [70105]):
        assert df1.empty
    elif (hydro_id == 'river / flow / rec / qc') & (sites == [69607, 70105]) & (pivot == False):
        assert (len(df1) == 80)
    else:
        assert (len(df1) > 0)


def test_sel():
    h6 = h1.sel(hydro_id='river / flow / rec / qc', sites=[70105, 69607])
    h6._base_stats_fun()
    assert (len(h6._base_stats) == 2) & (h6._base_stats['count'][0] == 68)







