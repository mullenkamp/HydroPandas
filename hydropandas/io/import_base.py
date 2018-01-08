# -*- coding: utf-8 -*-
"""
Functions for the hydro class for importing data.
"""
from collections import defaultdict
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from shapely.wkt import loads
from shapely.geometry import Point
from hydropandas.io.tools.mssql import rd_sql, rd_sql_ts
from hydropandas.tools.general.spatial.vector import sel_sites_poly, xy_to_gpd
from hydropandas.core.base import all_hydro_ids, mtype_df, ureg, Q_
from hydropandas.core.indexing import _comp_by_catch
from hydropandas.util.unit_conversion import to_units

########################################################
#### Time series data

mfreq_list = ['discrete', 'sum', 'mean']

### Primary import functions


def add_tsdata(self, data, dformat, hydro_id, freq_type, times=None, sites=None, values=None, units=None, qual_codes=None):
    """
    The general function to add time series data to the hydro class object.

    Input data can be either in 'wide' or 'long' dformat.

    The 'wide' dformat is where the columns are the sites or both mtypes and sites as a MultiIndex column. 'time' should either be None if data.index is a DateTimeIndex or a DateTimeIndex. 'sites' should be None. If data has a MultiIndex column of mtypes and sites, they must be arranged in that order (top: mtypes, bottom: sites).

    The 'long' dformat should be a DataFrame with four columns: One column with a DatetimeIndex for 'time', one column with the site values as 'sites', one column with the mtypes values as 'mtypes', and one column with the data values as 'values'.

    Parameters
    ----------
    data : DataFrame
        A Pandas DataFrame in either long or wide format.
    time : DateTimeIndex, str, or None
        The time index reference. Depends on dformat.
    sites : str, int, or None
        The sites reference. Depends on dformat.
    mtypes : str, int, or None
        The mtypes reference. Depends on dformat.
    values : str or None
        Only needed for dformat = 'long'. The column name for the data values.
    dformat : 'wide' or 'long'
        The format of the data table to import.
    add : bool
        Should new data be appended to the existing object? If False, returns a new object.

    Returns
    -------
    Hydro
    """

    ### Convert input data to standarised long format for consumption
    if not isinstance(data, pd.DataFrame):
        raise TypeError('data must be a DataFrame.')
    if data.empty:
        raise ValueError('DataFrame is empty, no data was passed')
    if dformat is 'wide':
        if not isinstance(data.index, pd.DatetimeIndex):
                raise ValueError('If dformat is wide, then the df.index should be a DateTimeIndex')
        if isinstance(data.columns, pd.MultiIndex):
            d1 = data.copy()
            d1.index.name = 'time'
            input1 = d1.reset_index().melt(id_vars='time')
            input1.rename(columns={hydro_id: 'hydro_id', sites: 'site'}, inplace=True)
        else:
            d1 = data.copy()
            d1.index.name = 'time'
            if '/' in hydro_id:
                d1.columns.name = 'site'
                input1 = d1.reset_index().melt(id_vars='time')
                input1['hydro_id'] = hydro_id
            else:
                d1.columns.name = 'hydro_id'
                input1 = d1.reset_index().melt(id_vars='time')
                input1['site'] = sites
    elif dformat is 'long':
        if (len(data.columns) < 4) or not isinstance(hydro_id, str) or not isinstance(sites, str) or not isinstance(times, str) or not isinstance(values, str):
            raise ValueError("Long format DataFrame must have 4 or 5 columns each corresponding to hydro_id, sites, time, and values (and optionally qual_codes).")
        input1 = data.copy()
        input1.rename(columns={hydro_id: 'hydro_id', sites: 'site', times: 'time', values: 'value', qual_codes: 'qual_code'}, inplace=True)
        if input1['time'].dtype.type != np.datetime64:
            input1['time'] = pd.to_datetime(input1['time'])

    ### Remove NaNs
    if any(input1.value.isnull()):
        input1 = input1.dropna()

    ### Categorise hydro_id and site columns and set index
    input2 = input1.copy()
    input2['hydro_id'] = input2['hydro_id'].astype('category')
    input2['site'] = input2['site'].astype('category')
    input2 = input2.set_index(['hydro_id', 'site', 'time'])
    input2.index = input2.index.remove_unused_levels()

    ### Run additional checks
    ## Check hydro_ids
    new_hydro_ids = input2.index.levels[0]
    hydro_ids_bool = np.in1d(new_hydro_ids, all_hydro_ids.index)
    if any(~hydro_ids_bool):
        sel_hydro_ids = new_hydro_ids[hydro_ids_bool]
        input2 = input2.loc(axis=0)[sel_hydro_ids, :, :]
    df_index = input2.index.droplevel('time').drop_duplicates()

    ## Check freq_type and assign
    mfreq_dict = _create_mfreq(df_index, freq_type)

    ### Assign units
    units_dict = _create_units(df_index, units)

#    ### Convert to series'
#    if isinstance(qual_codes, str):
#        qual = input2['qual_code']
#        qual.name = 'qual_code'
#    else:
#        qual = None
#    tsdata = input2['value']
#    tsdata.name = 'tsdata'

    ### Add data to new hydro object
    new1 = self.copy()
    if hasattr(new1, '_base_stats'):
        delattr(new1, '_base_stats')
    if hasattr(new1, 'tsdata'):
        new1 = _combine_tsdata(new1, input2, mfreq_dict, units_dict)
    else:
        setattr(new1, 'tsdata', input2.sort_index())
        setattr(new1, 'mfreq', mfreq_dict)
        setattr(new1, 'units', units_dict)

    ## Reassign attributes
    _import_attr(new1)

    ## Return
    return new1


### Import helper functions


def _import_attr(self):
    ## Assign the list of available mtypes
    self.hydro_id = self.tsdata.index.levels[0].tolist()

    ## Assign the list of available sites
    self.sites = self.tsdata.index.levels[1].tolist()

    ## Assign a dict of sites to mtypes
    hydro_id1 = self.tsdata.index.get_level_values('hydro_id')
    sites1 = self.tsdata.index.get_level_values('site')
    zipped = zip(hydro_id1, sites1)
    ms = defaultdict(set)
    for a, b in zipped:
        ms[a].add(b)
    setattr(self, 'hydroid_sites', dict(ms))

    sm = defaultdict(set)
    for a, b in zipped:
        sm[b].add(a)
    setattr(self, 'sites_hydroid', dict(sm))

    ## Assign a blank site attribute table
#    if hasattr(self, 'site_attr'):
#        sites_needed = set(self.sites).difference(self.site_attr.index.tolist())
#        new_sites = DataFrame(nan, index=sites_needed, columns=site_attr_lst)
#        combine = concat([self.site_attr, new_sites])
#        combine.index.name = 'site'
#        setattr(self, 'site_attr', combine)
#    else:
#        site_attr = DataFrame(nan, index=self.sites, columns=site_attr_lst)
#        site_attr.index.name = 'site'
#        setattr(self, 'site_attr', site_attr)

def _create_mfreq(df_index, freq_type):
    """

    """
    index_list = df_index.tolist()
    if isinstance(freq_type, str):
        if freq_type in mfreq_list:
            mfreq_dict = {i: freq_type for i in index_list}
        else:
            raise ValueError('If freq_type is a str then it must be one of ' +  ', '.join(mfreq_list))
    elif isinstance(freq_type, dict):
        if isinstance(list(freq_type.keys())[0], tuple):
            mfreq_mis = [i for i in index_list if i not in freq_type.keys()]
            if mfreq_mis:
                raise ValueError('If freq_type is a dict with tuples as keys, then they must have the same combination of hydro_ids and sites as the input data. Missing tuples include ' + str(mfreq_mis)[1:-1])
            mfreq_dict = {i: freq_type[i] for i in index_list}
        else:
            mfreq_mis = [i for i in index_list if i not in freq_type.keys()]
            if mfreq_mis:
                raise ValueError('If freq_type is a dict with hydro_ids as keys, then they must have the same number of hydro_ids as the input data. Missing hydro_ids include ' + str(mfreq_mis)[1:-1])
            mfreq_dict = {i: freq_type[i[0]] for i in index_list}
    return mfreq_dict


def _append_mfreq(old_mfreq, new_mfreq):
    """

    """
    ### Check if the values are identical/different for the same hydro_id/site combo
    dup1 = {i: new_mfreq[i] != old_mfreq[i] for i in new_mfreq if i in old_mfreq}

    if any(dup1.values()):
        true1 = [i for i in dup1 if dup1[i]]
        raise ValueError('Cannot have a different freq_type for an existing hydro_id/site combo. The problematic keys are ' + str(true1)[1:-1])

    ### Append/update mfreq
    old_mfreq.update(new_mfreq)

    ### Return
    return old_mfreq


def _create_units(df_index, units):
    """
    Add in dimensionality checks!!!
    """
    hydro_id1 = df_index.levels[0].tolist()
    units_dict = {}
    if isinstance(units, str):
        if len(hydro_id1) != 1:
            raise ValueError('If there are more than one hydro_id, a dict needs to be passed to units (or None).')
        units_dict.update({hydro_id1[0]: ureg(units)})
    elif isinstance(units, dict):
        units_mis = [i for i in hydro_id1 if i not in units.keys()]
        if units_mis:
            raise ValueError('If units is a dict with hydro_ids as keys, then they must have the same number of hydro_ids as the input data. Missing hydro_ids include ' + str(units_mis)[1:-1])
        units1 = {i: units[i] for i in units if i in hydro_id1}
        if isinstance(units1[list(units1.keys())[0]], ureg.Quantity):
            units_dict = units1
        else:
            for u in units1:
                units_dict.update({u: ureg(units1[u])})
    else:
        mtypes = all_hydro_ids.loc[hydro_id1, ['measurement']]
        hydro_id_units = pd.merge(mtypes, mtype_df[['Units']], right_index=True, left_on='measurement')
        hydro_id_units = hydro_id_units.drop('measurement', axis=1)['Units'].to_dict()
        for u in hydro_id_units:
            units_dict.update({u: ureg(hydro_id_units[u])})
    return units_dict


def _append_units(hydro, new_units):
    """
    Add in dimensionality checks!!!
    """
    old_units1 = hydro.units.copy()

    ### Check if the values are identical/different for the same hydro_id
    dup1 = {i: new_units[i] != old_units1[i] for i in new_units if i in old_units1}

    if any(dup1.values()):
        true1 = [i for i in dup1 if dup1[i]]
        convert_new = {i: new_units[i] for i in true1}
        print('Two identical hydro_ids have different units. Converting to new units...')
        to_units(hydro, convert_new, inplace=True)

    ### Append/update units
    hydro.units.update(new_units)


###################################################
#### Combine two hydro class objects together

def _combine_tsdata(self, tsdata, mfreq_dict, units_dict):
    """
    Function to combine two hydro class objects.
    """
    new1 = self.copy()
    if hasattr(new1, '_base_stats'):
        delattr(new1, '_base_stats')
    setattr(new1, 'tsdata', tsdata.combine_first(new1.tsdata).sort_index())
    setattr(new1, 'mfreq', _append_mfreq(new1.mfreq, mfreq_dict))
    _append_units(new1, units_dict)

    return new1

def combine(self, hydro):
    """

    """
    new1 = _combine_tsdata(self, hydro.tsdata, hydro.mfreq, hydro.units)
    if hasattr(hydro, 'site_attr'):
        new1.add_site_attr(hydro.site_attr)
    if hasattr(hydro, 'geo_point'):
        new1.add_geo_point(hydro.geo_point, check=False)
    if hasattr(hydro, 'geo_catch'):
        new1.add_geo_catch(hydro.geo_catch, check=False)
    _import_attr(new1)
    return new1


####################################################
#### Geometry data

### Import geometry with sites

def _add_geo_data(self, geo, obj_name):
    """
    Function to read in a GeoDataFrame to the hydro class. The GeoDataFrame must only contain a geometry column and the index must be the site names. Or it can be a shapefile where the first column has the site names.
    """
    if hasattr(self, obj_name):
        pre_geo = getattr(self, obj_name)
        geo_sites = pre_geo.index.tolist()
    else:
        geo_sites = []
    sites = self.sites
    geo_sites_check = ~pd.Series(sites).isin(geo_sites)
    if (len(sites) < len(geo_sites)) & all(~geo_sites_check):
        pre_geo = pre_geo.loc[sites]
        setattr(self, obj_name, pre_geo)
    if any(geo_sites_check):
        sites_sel = pd.Series(sites)[geo_sites_check]
        if isinstance(geo, (gpd.GeoDataFrame, str)):
            if isinstance(geo, str):
                geo0 = gpd.read_file(geo)
                geo1 = geo0.set_index(geo0.columns[0])
            else:
                geo1 = geo
            site_sel_check = pd.Series(sites_sel).isin(geo1.index)
            sites_sel2 = sites_sel[site_sel_check]
            if any(site_sel_check):
                if hasattr(self, obj_name):
                    setattr(self, obj_name, pd.concat([pre_geo, geo1.loc[sites_sel2, :]]))
                else:
                    setattr(self, obj_name, geo1.loc[sites_sel2, :])
                getattr(self, obj_name).index.name = 'site'
            else:
                raise ValueError('Input geo data must have locations for some of the sites.')
        else:
            raise ValueError('Input data must be a GeoDataFrame or a string path to a shapefile!')


def add_geo_point(self, geo, check=True):
    _add_geo_data(self, geo, 'geo_point')
    if check:
        _check_geo_sites(self, 'geo_point')


def add_geo_catch(self, geo, check=True):
    if hasattr(self, 'geo_point'):
        _add_geo_data(self, geo, 'geo_catch')
        _comp_by_catch(self)
        if check:
            _check_crs(self)
            _check_geo_sites(self, 'geo_catch')
    else:
        raise ValueError('Add the site locations first before adding the catchment polygons.')

### Checks

## Identify missing site locations


def missing_geo_point_sites(self):
    if hasattr(self, 'geo_point'):
        geo_sites = self.geo_point.index.tolist()
        sites = self.sites
        mis_sites = pd.Series(sites)[~pd.Series(sites).isin(geo_sites)].tolist()
        self._mis_geo_sites = mis_sites
        return(mis_sites)
    else:
        raise ValueError('You are missing all of the sites!')

## Make sure that the geo objects have the same crs (reproject if needed)
def _check_crs(self):
    geo_crs = {i: getattr(self, i).crs for i in self.__dict__.keys() if 'geo' in i}
    if len(geo_crs) > 1:
        geo_point_crs = getattr(self, 'geo_point').crs
        for i in geo_crs.keys():
            if not i == 'geo_point':
                geo1 = getattr(self, i)
                geo2 = geo1.to_crs(crs=geo_point_crs)
                setattr(self, i, geo2)

## Check if all sites have been found for the geo data
def _check_geo_sites(self, geo_name):
    all_sites = set(self.sites)
    geo_sites = getattr(self, geo_name).index.tolist()
    mis1 = all_sites.difference(geo_sites)
    if mis1:
        print('Missing sites ' + str(list(mis1)))
    else:
        print('Found all of the sites!')

####################################################
#### Populate site attributes

# def add_site_attr(self, df):
#     """
#     Function to add in site attributes. df must be a dataframe with the sites as an index.
#     """
#     if isinstance(df, pd.DataFrame):
#         sites = self.sites
#         if all(pd.Series(sites).isin(df.index)):
#             new_attr1 = df.loc[df.index.isin(sites), site_attr_lst]
#         elif any(pd.Series(sites).isin(df.index)):
#             new_attr = df.loc[df.index.isin(sites), site_attr_lst]
#             new_attr1 = new_attr.combine_first(self.site_attr)
#         else:
#             raise ValueError('No new sites!')
#         setattr(self, 'site_attr', new_attr1)


####################################################
#### File reading functions (csv and netcdf)


def rd_csv(self, csv_path, dformat, hydro_id, freq_type, multicolumn=False, times=None, sites=None, values=None, units=None, qual_codes=None):
    """
    Simple function to read in time series data and make it regular if needed.
    """
    if dformat == 'wide':
        if multicolumn:
            ts = pd.read_csv(csv_path, parse_dates=True, infer_datetime_format=True, dayfirst=True, header=[0, 1], index_col=0)
        else:
            ts = pd.read_csv(csv_path, parse_dates=True, infer_datetime_format=True, dayfirst=True, header='infer', index_col=0)
    else:
        ts = pd.read_csv(csv_path, parse_dates=[times], infer_datetime_format=True, dayfirst=True, header='infer')
    new1 = self.add_tsdata(ts, dformat=dformat, hydro_id=hydro_id, freq_type=freq_type, times=times, sites=sites, values=values, units=units, qual_codes=qual_codes)
    return new1


def rd_hdf(self, h5_path):
    """
    Function to read a netcdf file (.nc) that was an export from a hydro class.
    """
    ### Read in base tsdata and attributes
    ## Read in tsdata
    tsdata = pd.read_hdf(h5_path, 'tsdata')
    if 'qual_codes' in tsdata.columns:
        qual_codes = 'qual_codes'
    else:
        qual_codes = None

    ## Read in mfreq
    mfreq = pd.read_hdf(h5_path, 'mfreq').to_dict()

    ## Read in units
    units = pd.read_hdf(h5_path, 'units').to_dict()

    ### Make new Hydro class
    new1 = self.add_tsdata(tsdata.reset_index(), dformat='long', hydro_id='hydro_id', freq_type=mfreq, times='time', sites='site', values='value', units=units, qual_codes=qual_codes)

    ### Read in site attributes
    try:
        site_attr = pd.read_hdf(h5_path, 'site_attr')
        setattr(new1, 'site_attr', site_attr)
    except:
        print('No site attributes.')

    ### Read in geo points
    try:
        geo_point1 = pd.read_hdf(h5_path, 'geo_point')
        geo_point_crs = pd.to_numeric(pd.read_hdf(h5_path, 'geo_point_crs'), 'ignore').to_dict()
        geo_point = xy_to_gpd('site', 'x', 'y', geo_point1, geo_point_crs).set_index('site')
        new1.add_geo_point(geo_point, check=False)
    except:
        print('No geo points.')

    ### Read in geo catch
    try:
        geo_catch1 = pd.read_hdf(h5_path, 'geo_point')
        geo1 = [loads(x) for x in geo_catch1.wkt.values]
        geo_catch_crs = pd.to_numeric(pd.read_hdf(h5_path, 'geo_catch_crs'), 'ignore').to_dict()
        gdf_catch = gpd.GeoDataFrame(geo_catch1.drop('wkt', axis=1), geometry=geo1, crs=geo_catch_crs).set_index('site')
        new1.add_geo_point(gdf_catch, check=False)
    except:
        print('No geo catch.')

    return new1


#def rd_netcdf(self, nc_path):
#    """
#    Function to read a netcdf file (.nc) that was an export from a hydro class.
#    """
#
#    with xr.open_dataset(nc_path) as ds1:
#
#        ### Load in the geometry data
#        if any(np.in1d(ds1.data_vars.keys(), 'geo_catch_wkt')):
#    #        geo_catch_cols = [i for i in ds1.keys() if 'geo_catch' in i]
#            df_catch = ds1['geo_catch_wkt'].to_dataframe()
#            df_catch.columns = df_catch.columns.str.replace('geo_catch_', '')
#            geo1 = [loads(x) for x in df_catch.wkt]
#            gdf_catch = gpd.GeoDataFrame(df_catch.drop('wkt', axis=1), geometry=geo1, crs=dict(ds1.attrs['crs']))
#            gdf_catch.index.name = 'site'
#            ds1 = ds1.drop('geo_catch_wkt')
#
#        if any(np.in1d(ds1.data_vars.keys(), 'geo_point_x')):
#            geo_point_cols = [i for i in ds1.keys() if 'geo_point' in i]
#            df_point = ds1[geo_point_cols].to_dataframe()
#            df_point.columns = df_point.columns.str.replace('geo_point_', '')
#            geo2 = [Point(xy) for xy in zip(df_point.x, df_point.y)]
#            crs1 = dict(ds1['geo_point_x'].attrs.copy())
#            crs1.update({i: bool(crs1[i]) for i in crs1 if crs1[i] in ['True', 'False']})
#            gdf_point = gpd.GeoDataFrame(df_point.drop(['x', 'y'], axis=1), geometry=geo2, crs=crs1)
#            gdf_point.index.name = 'site'
#            ds1 = ds1.drop(geo_point_cols)
#
#        ### Load in the site attribute data
#        if any(np.in1d(ds1.data_vars.keys(), 'site_attr')):
#            site_attr_cols = [i for i in ds1.keys() if 'site_attr' in i]
#            site_attr1 = ds1[site_attr_cols].to_dataframe()
#            site_attr1.columns = site_attr1.columns.str.replace('site_attr_', '')
#            site_attr1.index.name = 'site'
#            ds1 = ds1.drop(site_attr_cols)
#
#        ### Load in the ts data
#        df1 = ds1[['site', 'time', 'data']].to_dataframe().reset_index()
#
#        self.add_data(df1, 'time', 'site', 'mtype', 'data', 'long')
#
#        ### Add in the earlier attributes
#        if 'site_attr1' in locals():
#            self.add_site_attr(site_attr1)
#        if 'gdf_point' in locals():
#            self.add_geo_point(gdf_point, check=False)
#        if 'gdf_catch' in locals():
#            self.add_geo_catch(gdf_catch, check=False)
#
#        ### return
#    #    ds1.close()
#        return self


##############################################
#### Functions to read from databases

### mssql


def _rd_hydro_mssql(self, server, database, table, mtype, date_col, site_col, data_col, qual_col=None, sites=None, from_date=None, to_date=None, qual_codes=None, add_where=None, min_count=None, resample_code=None, period=1, fun='mean', val_round=3):
    """
    Function to import data from a MSSQL database. Specific columns can be selected and specific queries within columns can be selected. Requires the pymssql package.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    mtype : str
        The measurement type according to the Hydro mtypes.
    date_col : str
        The DateTime column name.
    site_col : str
        The site name column.
    data_col : str
        The data column.
    qual_col : str
        The quality code column.
    sites : list
        List of sites.
    from_date : str
        From date for data in the format '2000-01-01'.
    to_date : str
        To date for the data in the above format.
    qual_codes : list of int
        List of quality codes.
    add_where : str
        An additional SQL query to be added.
    min_count : int
        The minimum number of data values per site for data extraction.
    resample_code : str or None
        The Pandas time series resampling code. e.g. 'D' for day, 'W' for week, 'M' for month, etc.
    period : int
        The number of resampling periods. e.g. period = 2 and resample = 'D' would be to resample the values over a 2 day period.
    fun : str
        The resampling function. i.e. mean, sum, count, min, or max. No median yet...
    val_round : int
        The number of decimals to round the values.

    Return
    ------
    Hydro
    """

    ## Prepare sql_dict for sites and qual_cdes
    site_qual_dict = {}

    if isinstance(qual_codes, list):
        site_qual_dict.update({qual_col: qual_codes})

    if isinstance(sites, list):
        sites = [str(i) for i in sites]
        site_qual_dict.update({site_col: sites})

    sql_dict = {'server': server, 'database': database, 'table': table, 'groupby_cols': site_col, 'date_col': date_col, 'values_cols': data_col, 'resample_code': resample_code, 'period': period, 'fun': fun, 'val_round': val_round, 'where_col': site_qual_dict, 'from_date': from_date, 'to_date': to_date, 'min_count': min_count}

    df = rd_sql_ts(**sql_dict).reset_index()

    ## Rename columns
    df.columns = ['site', 'time', 'data']

    ## Remove spaces in site names and duplicate data
    df.loc[:, 'site'] = df.loc[:, 'site'].astype(str).str.replace(' ', '').str.upper()
    df = df.drop_duplicates(['site', 'time'])

    df['mtype'] = mtype

    self.add_data(df, 'time', 'site', 'mtype', 'data', 'long')
    return self


def _rd_hydro_geo_mssql(self, server, database, table, geo_dict):
    """
    Function to select sites based on the geo attributes.
    """

    sites1 = rd_sql(server, database, table, 'site', geo_dict)
    sites2 = sites1.site.astype(str).values.tolist()
    return sites2


def _proc_hydro_sql(self, sites_sql_fun, mtype_dict, mtype, sites=None, from_date=None, to_date=None, qual_codes=None, min_count=None, buffer_dis=0, resample_code=None, period=1, fun='mean'):
    """
    Convenience function for reading in mssql data from standardized hydro tables.
    """

    if isinstance(sites, gpd.GeoDataFrame):
        loc1 = sites_sql_fun()
        sites1 = sel_sites_poly(loc1, sites, buffer_dis).index.astype(str)
    else:
        sites1 = pd.Series(sites).astype(str)

    h1 = self.copy()
    if isinstance(mtype_dict, (list, tuple)):
        for i in range(len(mtype_dict)):
            site1 = mtype_dict[i]['site_col']

            sites_stmt = 'select distinct ' + site1 + ' from ' + mtype_dict[i]['table']
            sites2 = rd_sql(mtype_dict[i]['server'], mtype_dict[i]['database'], stmt=sites_stmt).astype(str)[site1]
            sites3 = sites2[sites2.isin(sites1)].astype(str).tolist()
            if not sites3:
                raise ValueError('No sites in database')
            if mtype_dict[i]['qual_col'] is None:
                qual_codes = None
            h1 = h1._rd_hydro_mssql(sites=sites3, mtype=mtype, from_date=from_date, to_date=to_date, qual_codes=qual_codes, min_count=min_count, resample_code=resample_code, period=period, fun=fun, **mtype_dict[i])
    elif isinstance(mtype_dict, dict):
        site1 = mtype_dict['site_col']

        sites_stmt = 'select distinct ' + site1 + ' from ' + mtype_dict['table']
        sites2 = rd_sql(mtype_dict['server'], mtype_dict['database'], stmt=sites_stmt).astype(str)[site1]
        sites3 = sites2[sites2.isin(sites1)].astype(str).tolist()
        if not sites3:
                raise ValueError('No sites in database')
        if mtype_dict['qual_col'] is None:
            qual_codes = None
        h1 = h1._rd_hydro_mssql(sites=sites3, mtype=mtype, from_date=from_date, to_date=to_date, qual_codes=qual_codes, min_count=min_count, resample_code=resample_code, period=period, fun=fun, **mtype_dict)
    elif callable(mtype_dict):
        h1 = mtype_dict(h1, sites=sites1, mtype=mtype, from_date=from_date, to_date=to_date, min_count=min_count)

    return h1







