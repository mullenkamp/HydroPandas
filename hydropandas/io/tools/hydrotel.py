# -*- coding: utf-8 -*-
"""
Created on Tue Jan 02 10:05:15 2018

@author: MichaelEK
Functions to read hydrotel data.
"""
import numpy as np
import pandas as pd
from hydropandas.io.tools.mssql import rd_sql, rd_sql_ts
from hydropandas.util.misc import select_sites, save_df

######################################
### Parameters
## hydro_ids dict
hydro_ids_dict = {'river / flow / rec / raw': 'Flow Rate',
               'aq / wl / rec / raw': 'Water Level',
               'atmos / precip / rec / raw': 'Rainfall Depth',
               'river / wl / rec / raw': 'Water Level',
               'river / T / rec / raw': 'Water Temperature'}
resample_dict = {'river / flow / rec / raw': 'mean',
               'aq / wl / rec / raw': 'mean',
               'atmos / precip / rec / raw': 'sum',
               'river / wl / rec / raw': 'mean',
               'river / T / rec / raw': 'mean'}

## Database parameters
server = 'SQL2012PROD05'
database = 'Hydrotel'

data_tab = 'Hydrotel.dbo.Samples'
points_tab = 'Hydrotel.dbo.Points'
objects_tab = 'Hydrotel.dbo.Objects'
hydro_ids_tab = 'Hydrotel.dbo.ObjectVariants'
sites_tab = 'Hydrotel.dbo.Sites'

data_col = ['Point', 'DT', 'SampleValue']
points_col = ['Point', 'Object']
objects_col = ['Object', 'Site', 'Name', 'ObjectType', 'ObjectVariant']
hydro_ids_col = ['ObjectType', 'ObjectVariant', 'Name']
sites_col = ['Site', 'Name', 'ExtSysId']

db_bgauging = 'Bgauging'
bg_tab = 'RSITES'
bg_col = ['SiteNumber']

db_wells = 'Wells'
wells_tab = 'WELL_DETAILS'
wells_col = ['Well_NO']


def hydrotel_sites_by_hydroid(hydro_id):
    """
    Function to return all the sites associated with a particular hydro_id.

    Parameters
    ----------
    hydro_id: str
        The HydroPandas hydro_id.

    Returns
    -------
    DataFrame
        of hydrotel_id and ECan site id.
    """
    if hydro_id in ['aq / wl / rec / raw', 'aq / T / rec / raw']:
        sites1 = rd_sql(server, db_wells, wells_tab, wells_col)[wells_col[0]]
        site_val0 = rd_sql(server, database, sites_tab, ['Site', 'Name'])
        site_val0.loc[:, 'Name'] = site_val0.apply(lambda x: x.Name.split(' ')[0], axis=1)
        site_val1 = site_val0[site_val0.Name.isin(sites1)].copy()
    elif hydro_id in ['atmos / precip / rec / raw']:
        sites1 = rd_sql(server, db_bgauging, bg_tab, bg_col, {'RainfallSite': 'R'})[bg_col[0]]
        site_val1 = rd_sql(server, database, objects_tab, ['Site', 'ExtSysId'], 'ExtSysId', sites1.astype('int32').tolist()).sort_values('Site')
#        site_val0 = rd_sql(server, database, sites_tab, ['Site', 'Name'], 'Site', site_ob1.Site.tolist())
#        site_val1 = pd.merge(site_val0, site_ob1, on='Site')
    else:
        sites1 = rd_sql(server, db_bgauging, bg_tab, bg_col, {'RainfallSite': 'N'})[bg_col[0]]
        site_val1 = rd_sql(server, database, sites_tab, ['Site', 'ExtSysId'], 'ExtSysId', sites1.astype('int32').tolist()).sort_values('Site')
    site_val1.columns = ['hydrotel_id', 'site']
    site_val2 = site_val1.drop_duplicates('site')

    return site_val2



def rd_hydrotel(sites, hydro_id, from_date=None, to_date=None, resample_code='D', period=1, val_round=3, min_count=None, pivot=False, export_path=None):
    """
    Function to extract time series data from the hydrotel database.

    Parameters
    ----------
    sites: list, array, dataframe, or str
        Site list or a str path to a single column csv file of site names/numbers.
    hydro_id: str
        'river / flow / rec / raw', 'aq / wl / rec / raw', 'atmos / precip / rec / raw', 'river / wl / rec / raw', or 'river / T / rec / raw'.
    from_date: str or None
        The start date in the format '2000-01-01'.
    to_date: str or None
        The end date in the format '2000-01-01'.
    resample_code : str
        The Pandas time series resampling code. e.g. 'D' for day, 'W' for week, 'M' for month, etc.
    period: int
        The number of resampling periods. e.g. period = 2 and resample = 'D' would be to resample the values over a 2 day period.
    fun: str
        The resampling function. i.e. mean, sum, count, min, or max. No median yet...
    val_round: int
        The number of decimals to round the values.
    pivot: bool
        Should the output be pivotted into wide format?
    export_path: str or None
        The path and file name to be saved.

    Returns
    -------
    Series or DataFrame
        A MultiIndex Pandas Series if pivot is False and a DataFrame if True
    """
    #### Import data and select the correct sites

    sites = select_sites(sites)
    if hydro_id == 'atmos / precip / rec / raw':
        site_ob1 = rd_sql(server, database, objects_tab, ['Site', 'ExtSysId'], 'ExtSysId', sites.astype('int32').tolist())
        site_val0 = rd_sql(server, database, sites_tab, ['Site', 'Name'], 'Site', site_ob1.Site.tolist())
        site_val1 = pd.merge(site_val0, site_ob1, on='Site')
    elif hydro_id in ['aq / wl / rec / raw', 'aq / T / rec / raw']:
        site_val0 = rd_sql(server, database, sites_tab, ['Site', 'Name'])
        site_val0.loc[:, 'Name'] = site_val0.apply(lambda x: x.Name.split(' ')[0], axis=1)
        site_val1 = site_val0[site_val0.Name.isin(sites)].copy()
        site_val1.loc[:, 'ExtSysId'] = site_val1.loc[:, 'Name']
    else:
        site_val1 = rd_sql(server, database, sites_tab, sites_col, 'ExtSysId', sites.astype('int32').tolist())

    if site_val1.empty:
        raise ValueError('No site(s) in database')

    site_val1.loc[:, 'ExtSysId'] = pd.to_numeric(site_val1.loc[:, 'ExtSysId'], errors='ignore')
    site_val1 = site_val1.drop_duplicates('ExtSysId')
    site_val = site_val1.Site.astype('int32').tolist()
    if isinstance(hydro_id, (list, np.ndarray, pd.Series)):
        hydro_ids = [hydro_ids_dict[i] for i in hydro_id]
    elif isinstance(hydro_id, str):
        hydro_ids = [hydro_ids_dict[hydro_id]]
    else:
        raise ValueError('hydro_id must be a str, list, ndarray, or Series.')
    hydro_ids_val = rd_sql(server, database, hydro_ids_tab, hydro_ids_col, 'Name', hydro_ids)

    where_col = {'Site': site_val,
                 'ObjectVariant': hydro_ids_val.ObjectVariant.astype('int32').tolist(),
                 'ObjectType': hydro_ids_val.ObjectType.astype('int32').tolist()}

    object_val1 = rd_sql(server, database, objects_tab, objects_col, where_col)
    if hydro_id == 'aq / wl / rec / raw':
        object_val1 = object_val1[object_val1.Name == 'Water Level']
    elif hydro_id == 'atmos / precip / rec / raw':
        object_val1 = object_val1[object_val1.Name == 'Rainfall']
    elif hydro_id == 'river / T / rec / raw':
        object_val1 = object_val1[object_val1.Name == 'Water Temperature']
    object_val = object_val1.Object.values.astype(int).tolist()

    #### Rearrange data
    point_val1 = rd_sql(server, database, points_tab, points_col, where_col='Object', where_val=object_val)
    point_val = point_val1.Point.values.astype(int).tolist()

    #### Big merge
    comp_tab1 = pd.merge(site_val1, object_val1[['Object', 'Site']], on='Site')
    comp_tab2 = pd.merge(comp_tab1, point_val1, on='Object')
    comp_tab2.set_index('Point', inplace=True)

    #### Pull out the data
    ### Make SQL statement
    data1 = rd_sql_ts(server, database, data_tab, 'Point', 'DT', 'SampleValue', resample_code, period, resample_dict[hydro_id], val_round, {'Point': point_val}, from_date=from_date, to_date=to_date, min_count=min_count)['SampleValue']

    data1.index.names = ['site', 'time']
    data1.name = 'value'
    site_numbers = [comp_tab2.loc[i, 'ExtSysId'] for i in data1.index.levels[0]]
    data1.index.set_levels(site_numbers, level='site', inplace=True)

    if pivot:
        data3 = data1.unstack(0)
    else:
        data3 = data1

    #### Export and return
    if export_path is not None:
        save_df(data3, export_path)

    return data3
