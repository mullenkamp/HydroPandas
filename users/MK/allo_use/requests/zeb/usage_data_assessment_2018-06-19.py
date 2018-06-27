# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 14:02:36 2018

@author: michaelek
"""
import os
import numpy as np
import pandas as pd
from pdsql import mssql
import geopandas as gpd
from shapely.geometry import Point, Polygon


def xy_to_gpd(id_col, x_col, y_col, df=None, crs=2193):
    """
    Function to convert a DataFrame with x and y coordinates to a GeoDataFrame.

    Parameters
    ----------
    df: Dataframe
        The DataFrame with the location data.
    id_col: str or list of str
        The column(s) from the dataframe to be returned. Either a one name string or a list of column names.
    xcol: str or ndarray
        Either the column name that has the x values within the df or an array of x values.
    ycol: str or ndarray
        Same as xcol except for y.
    crs: int
        The projection of the data.

    Returns
    -------
    GeoDataFrame
        Of points.
    """

    if type(x_col) is str:
        geometry = [Point(xy) for xy in zip(df[x_col], df[y_col])]
    if isinstance(id_col, str) & (df is not None):
        id_data = df[id_col]
    crs1 = {'init' :'epsg:' + str(crs)}
    gpd1 = gpd.GeoDataFrame(id_data, geometry=geometry, crs=crs1)
    return gpd1


#################################################
### Parameters

server = 'sql2012dev01'
database = 'hydro'

crc_allo_table = 'CrcAllo'
crc_wap_allo_table = 'CrcWapAllo'
sites_table = 'ExternalSite'

ts_table = 'TSDataNumericDaily'
ts_cols = ['ExtSiteID', 'DateTime', 'Value']

ts_summ_table = 'TSDataNumericDailySumm'

datasettypeids = [9, 12]

#status_names = ['Issued - Active', 'Issued - s124 Continuance']

## Output
base_dir = r'E:\ecan\local\Projects\requests\zeb\2018-06-19'

count_csv = 'crc_cwms_count_2015-2016.csv'
vol_csv = 'crc_cwms_vol_2015-2016.csv'
usage_vol_csv = 'crc_usage_cwms_vol_2015-2016.csv'
allo_usage_shp = 'crc_allo_usage_2015-2016.shp'

###############################################
### Read in data

#crc_allo = mssql.rd_sql(server, database, crc_allo_table, where_col={'crc_status': status_names})

crc_allo = mssql.rd_sql(server, database, crc_allo_table)
crc_allo = crc_allo[(crc_allo.from_date <= '2015-07-01') & (crc_allo.to_date >= '2016-07-01')]

crc_wap_allo = mssql.rd_sql(server, database, crc_wap_allo_table, where_col={'crc': crc_allo.crc.unique()})

sites = mssql.rd_sql(server, database, sites_table, where_col={'ExtSiteID': crc_wap_allo.wap.unique()})

ts_usage = mssql.rd_sql(server, database, ts_table, ts_cols, where_col={'DatasetTypeID': datasettypeids, 'ExtSiteID': sites.ExtSiteID.unique().tolist()}, from_date='2017-07-01', date_col='DateTime')
ts_usage_summ = mssql.rd_sql(server, database, ts_summ_table, where_col={'DatasetTypeID': datasettypeids, 'ExtSiteID': sites.ExtSiteID.unique().tolist()})

sites.rename(columns={'ExtSiteID': 'wap'}, inplace=True)
ts_usage.rename(columns={'ExtSiteID': 'wap'}, inplace=True)
ts_usage_summ.rename(columns={'ExtSiteID': 'wap'}, inplace=True)


############################################
### Process data

## Count

usage_waps = ts_usage.wap.unique()

crc_allo1 = crc_allo[(crc_allo.in_gw_allo & (crc_allo.take_type == 'Take Groundwater')) | (crc_allo.take_type == 'Take Surface Water')]

crc_wap_allo1 = crc_wap_allo[crc_wap_allo.crc.isin(crc_allo1.crc.unique())]

crc_wap_allo2 = crc_wap_allo1[(crc_wap_allo1.in_sw_allo & (crc_wap_allo1.take_type == 'Take Surface Water')) | (crc_wap_allo1.take_type == 'Take Groundwater')]

crc_allo2 = crc_allo1[crc_allo1.crc.isin(crc_wap_allo2.crc.unique())]


allo_wap_cwms = pd.merge(crc_wap_allo2, sites[['wap', 'CwmsName']], on='wap')

crc_take_cwms = allo_wap_cwms[['crc', 'take_type', 'CwmsName']].drop_duplicates()

grp1 = crc_take_cwms.groupby(['CwmsName', 'take_type']).crc.count()
grp1.name = 'All consents'

crc_take_use = allo_wap_cwms[allo_wap_cwms.wap.isin(usage_waps)][['crc', 'take_type', 'CwmsName']].drop_duplicates()

grp2 = crc_take_use.groupby(['CwmsName', 'take_type']).crc.count()
grp2.name = 'Consents with usage'

count1 = pd.concat([grp1, grp2], axis=1)

count1['usage/allo'] = (count1['Consents with usage']/count1['All consents']).round(2)

count1.to_csv(os.path.join(base_dir, count_csv))

count_sum = count1.sum()
count_sum['Consents with usage']/count_sum['All consents']

## Volume

allo_cwms = pd.merge(crc_allo2, crc_take_cwms, on=['crc', 'take_type'], how='left')

grp3 = allo_cwms.groupby(['CwmsName', 'take_type']).feav.sum()
grp3.name = 'All consented allocation (m3)'

allo_cwms_use = pd.merge(crc_allo2, crc_take_use, on=['crc', 'take_type'], how='left')

grp4 = allo_cwms_use.groupby(['CwmsName', 'take_type']).feav.sum()
grp4.name = 'Consented allocation with usage data (m3)'

vol_sum1 = pd.concat([grp3, grp4], axis=1)

vol_sum1['usage/allo'] = (vol_sum1['Consented allocation with usage data (m3)']/vol_sum1['All consented allocation (m3)']).round(2)

vol_sum1.to_csv(os.path.join(base_dir, vol_csv))


## Usage volume to allocation

ts_usage_yr = ts_usage.groupby(['wap']).Value.sum().round()

usage_100 = ts_usage_yr[ts_usage_yr < 100]

allo_wap_cwms1 = allo_wap_cwms.loc[allo_wap_cwms.crc.isin(crc_allo2.crc.unique()), ['crc', 'take_type', 'wap', 'CwmsName']].drop_duplicates(['crc', 'take_type', 'wap'])

take_wap = allo_wap_cwms1.drop('crc', axis=1).sort_values('take_type', ascending=False).drop_duplicates('wap')

use_cwms = pd.merge(ts_usage_yr.reset_index(), take_wap, on='wap')

grp5 = use_cwms.groupby(['CwmsName', 'take_type']).Value.sum().round()
grp5.name = 'Usage (m3)'

vol_sum2 = pd.concat([grp4, grp5], axis=1)

vol_sum2['usage/allo'] = (vol_sum2['Usage (m3)']/vol_sum2['Consented allocation with usage data (m3)']).round(2)

vol_sum2.to_csv(os.path.join(base_dir, usage_vol_csv))


###############################################
#### volume by crc, take_type

dup_waps = allo_wap_cwms.groupby(['wap']).max_rate_wap.count()
dup_waps.name = 'Duplicate WAPs'

use_cwms1 = pd.merge(allo_wap_cwms1, dup_waps.reset_index(), on='wap', how='left')

use_cwms2 = pd.merge(use_cwms1, ts_usage_yr.reset_index(), on='wap', how='left').dropna().copy()

use_cwms2.rename(columns={'Value': 'usage'}, inplace=True)

use_cwms2['usage'] = use_cwms2['usage']/use_cwms2['Duplicate WAPs'].round().astype(int)

allo_feav1 = crc_allo2.groupby(['crc', 'take_type']).feav.sum().round().astype(int)

grp6 = use_cwms2.groupby(['crc', 'take_type'])

use1 = grp6.usage.sum()
wap_cwms2 = grp6[['wap', 'CwmsName', 'Duplicate WAPs']].max()

use_cwms3 = pd.concat([wap_cwms2, use1, allo_feav1], axis=1).dropna()
use_cwms3['usage/allo'] = (use_cwms3['usage']/use_cwms3['feav']).round(2)

gpd_sites = xy_to_gpd('wap', 'NZTMX', 'NZTMY', sites)

use_cwms4 = pd.merge(gpd_sites, use_cwms3, on='wap', how='right')

use_cwms4.to_file(os.path.join(base_dir, allo_usage_shp))

























