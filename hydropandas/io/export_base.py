# -*- coding: utf-8 -*-
"""
Functions for exporting data from within a hydro class.
"""
import numpy as np
import pandas as pd
import xarray as xr
from hydropandas.tools.general.spatial.vector import convert_crs


def to_csv(self, csv_path, hydro_id=None, sites=None, pivot=False, require=None, start=None, end=None, return_qual_code=False):
    """
    Function to export data from a hydro class to a MultiIndex csv.
    """

    df1 = self.sel_ts(hydro_id=hydro_id, sites=sites, pivot=pivot, require=require, start=start, end=end, return_qual_code=return_qual_code)
    df1.to_csv(csv_path, header=True)


def to_netcdf(self, nc_path):
    """
    Function to export a copy of a hydro class object to a netcdf file.
    """
    ### package tsdata
    ds0 = xr.Dataset(self.tsdata.reset_index(['site', 'time']))

    ### package other attributes


    ### package site attribue data
    if hasattr(self, 'site_attr'):
        site_attr = getattr(self, 'site_attr').copy()
        site_attr.columns = ['site_attr_' + i for i in site_attr.columns]
        site_attr.index.name = 'site_attr_site'
        ds0 = ds0.merge(xr.Dataset(site_attr))

    ### package geo data
    if hasattr(self, 'geo_point'):
        geo1 = getattr(self, 'geo_point').copy()
        geo1['x'] = geo1.geometry.apply(lambda x: x.x)
        geo1['y'] = geo1.geometry.apply(lambda x: x.y)
        geo2 = geo1.drop('geometry', axis=1)
        geo2.columns = ['geo_point_' + i for i in geo2.columns]
        geo2.index.name = 'geo_point_' + geo2.index.name
        ds0 = ds0.merge(xr.Dataset(geo2))
        crs1 = geo1.crs
        if isinstance(crs1, str):
            crs2 = convert_crs(crs1, 'proj4_dict')
        elif isinstance(crs1, dict):
            crs2 = crs1.copy()
        crs2.update({i: str(crs2[i]) for i in crs2 if type(crs2[i]) == bool})
        ds0['geo_point_x'].attrs = crs2
        ds0['geo_point_y'].attrs = crs2


    if hasattr(self, 'geo_catch'):
        geo1 = getattr(self, 'geo_catch').copy()
        geo1['wkt'] = geo1.geometry.apply(lambda x: x.to_wkt())
        geo2 = geo1.drop('geometry', axis=1)
        geo2.columns = ['geo_catch_' + i for i in geo2.columns]
        geo2.index.name = 'geo_catch_' + geo2.index.name
        ds0 = ds0.merge(xr.Dataset(geo2))

    ### Export data
    ds0.to_netcdf(nc_path)
    ds0.close()


def to_hdf(self, h5_path):
    """
    Function to export a copy of a hydro class object to a netcdf file.
    """
    ### Create store hdf object
    store = pd.HDFStore(h5_path, mode='w')

    try:
        ### package tsdata
        store.append(key='tsdata', value=self.tsdata)

        ### package mfreq
        mfreq_s = pd.DataFrame.from_dict(self.mfreq.copy(), orient='index')[0]
        mfreq_s.index = pd.MultiIndex.from_tuples(mfreq_s.index)
        mfreq_s.name = 'mfreq'
        mfreq_s.index.names = ['hydro_id', 'site']
        store.append(key='mfreq', value=mfreq_s)

        ### package units
        units_dict1 = self.units.copy()
        units_dict = {i: str(units_dict1[i]) for i in units_dict1}
        units_s = pd.DataFrame.from_dict(units_dict, orient='index')[0]
        units_s.name = 'units'
        units_s.index.name = 'hydro_id'
        store.append(key='units', value=units_s)

        ### package site attribue data
        if hasattr(self, 'site_attr'):
            site_attr = getattr(self, 'site_attr').copy()
            store.append(key='site_attr', value=site_attr)

        ### package geo data
        if hasattr(self, 'geo_point'):
            geo1 = getattr(self, 'geo_point').copy()
            geo1['x'] = geo1.geometry.apply(lambda x: x.x)
            geo1['y'] = geo1.geometry.apply(lambda x: x.y)
            geo2 = pd.DataFrame(geo1.drop('geometry', axis=1).reset_index())
            store.append(key='geo_point', value=geo2)

            crs1 = geo1.crs
            if isinstance(crs1, str):
                crs2 = convert_crs(crs1, 'proj4_dict')
            elif isinstance(crs1, dict):
                crs2 = crs1.copy()
            crs_s = pd.DataFrame.from_dict(crs2, 'index')[0].astype(str)
            store.append(key='geo_point_crs', value=crs_s)

        if hasattr(self, 'geo_catch'):
            geo1 = getattr(self, 'geo_catch').copy()
            geo1['wkt'] = geo1.geometry.apply(lambda x: x.to_wkt())
            geo2 = pd.DataFrame(geo1.drop('geometry', axis=1).reset_index())
            store.append(key='geo_catch', value=geo2)

            crs1 = geo1.crs
            if isinstance(crs1, str):
                crs2 = convert_crs(crs1, 'proj4_dict')
            elif isinstance(crs1, dict):
                crs2 = crs1.copy()
            crs_s = pd.DataFrame.from_dict(crs2, 'index')[0].astype(str)
            store.append(key='geo_catch_crs', value=crs_s)

        ### Close file when finished
        store.close()

    except Exception as err:
        store.close()
        raise err


def to_shp(self, shp_path):
    """
    Function to export a shapefile of the site locations.

    Parameters
    ----------
    shp_path : str
        The shapefile path.

    Returns
    -------
    None
    """

    if not hasattr(self, 'geo_point'):
        raise ValueError('Object has no geo points.')

    ### Prepare output
    geo1 = self.geo_point.copy()
    sites = self.sites
    hydroid_sites = self.hydroid_sites.copy()

    if len(hydroid_sites.keys()) > 1:
        t1 = {i: np.in1d(sites, list(hydroid_sites[i])).astype(str).tolist() for i in hydroid_sites}
        df1 = pd.DataFrame(t1, index=sites)
        geo2 = pd.concat([geo1, df1], axis=1).reset_index()
        geo2.to_file(shp_path)
    else:
        self._base_stats_fun()
        stats = self._base_stats.copy()
        stats.index = stats.index.droplevel('hydro_id')
        stats['start_time'] = stats['start_time'].astype(str)
        stats['end_time'] = stats['end_time'].astype(str)
        geo2 = pd.concat([geo1, stats], axis=1).reset_index()
        geo2.to_file(shp_path)













