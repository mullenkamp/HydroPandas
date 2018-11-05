# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql
from allocation_ts import allo_ts_proc, use_type_dict
#import geopandas as gpd
#import gistools.vector as vec
pd.options.display.max_columns = 10

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'
sites_table = 'ExternalSite'
crc_wap_table = 'CrcWapAllo'
crc_table = 'CrcAllo'

dw_server = 'sql2012prod03'
dw_db = 'datawarehouse'
permit_table = 'F_ACC_Permit'

permit_cols = ['b1_alt_id', 'ConsentLocation', 'HolderAddressFullName']
permit_cols_rename = ['crc', 'address', 'consent_holder_name']

#sites = ['J36/0016', 'K37/3262', '69302']
datasets = [9, 12]
#cwms = ['kaikoura']
#catch_group = ['Selwyn River']
#cwms = ['Selwyn - Waihora']
#rdr_site = 'J36/0016-M1'

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

from_date = '2016-07-01'
to_date = '2017-06-30'

#years = {'2016-01-01': '2016-06-30', '2017-01-01': '2017-06-30', '2018-01-01': '2018-06-30'}

#rdr_hts = [r'H:\Data\Annual\ending_2016\ending_2016.dsn', r'H:\Data\Annual\ending_2017\ending_2017.dsn', r'H:\Data\Annual\ending_2018\ending_20128.dsn']
#
#hts_dsn = r'H:\Data\WaterUSeAll.dsn'

#export_dir = r'E:\ecan\local\Projects\requests\lgoima\2018-09-28'
#export1 = 'crc_usage_summary_2018-10-03.csv'
#export2 = 'crc_permit_data_2018-10-04.csv'
#export2 = 'cpw_allo_usage_summary_2018-09-11.csv'
#export3 = 'cpw_crc_wap_usage_data_2018-09-11.csv'


def grp_ts_agg(df, grp_col, ts_col, freq_code):
    """
    Simple function to aggregate time series with dataframes with a single column of sites and a column of times.

    Parameters
    ----------
    df : DataFrame
        Dataframe with a datetime column.
    grp_col : str or list of str
        Column name that contains the sites.
    ts_col : str
        The column name of the datetime column.
    freq_code : str
        The pandas frequency code for the aggregation (e.g. 'M', 'A-JUN').

    Returns
    -------
    Pandas resample object
    """

    df1 = df.copy()
    if type(df[ts_col].iloc[0]) is pd.Timestamp:
        df1.set_index(ts_col, inplace=True)
        if type(grp_col) is list:
            grp_col.extend([pd.Grouper(freq=freq_code)])
        else:
            grp_col = [grp_col, pd.Grouper(freq=freq_code)]
        df_grp = df1.groupby(grp_col)
        return (df_grp)
    else:
        print('Make one column a timeseries!')


############################################
### Estimate allocation

allo1 = allo_ts_proc(server, database, crc_table, crc_wap_table, from_date, to_date, 'A-JUN', 'annual volume')
allo2 = allo1.reset_index()
allo2.rename(columns={'date': 'year', 'allo': 'feav'}, inplace=True)

############################################
### Extract data

permit1 = mssql.rd_sql(dw_server, dw_db, permit_table, permit_cols, rename_cols=permit_cols_rename)

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'CatchmentGroupName'])

crc = mssql.rd_sql(server, database, crc_table).drop('mod_date', axis=1)
crc = crc.replace({'use_type': use_type_dict})

allo3 = pd.merge(allo2, crc[['crc', 'take_type', 'allo_block', 'use_type']], on=['crc', 'take_type', 'allo_block'], how='left')

crc_wap = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'allo_block', 'wap', 'in_sw_allo'])
crc_wap1 = crc_wap[((crc_wap.take_type == 'Take Surface Water') & (crc_wap.in_sw_allo)) | (crc_wap.take_type == 'Take Groundwater')]

sites2 = sites1.rename(columns={'ExtSiteID': 'wap'})
crc_wap1a = pd.merge(crc_wap1, sites2, on=['wap'])

crc_wap2 = pd.merge(allo3, crc_wap1a[['crc', 'take_type', 'allo_block', 'wap', 'CatchmentGroupName']], on=['crc', 'take_type', 'allo_block'])

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_col={'ExtSiteID': crc_wap2.wap.unique().tolist(), 'DatasetTypeID': datasets}, from_date=from_date, to_date=to_date, date_col='DateTime')
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)

tsdata2 = grp_ts_agg(tsdata1, ['ExtSiteID', 'DatasetTypeID'], 'DateTime', 'A-JUN').Value.sum().round()
tsdata3 = tsdata2.reset_index().copy()
tsdata3.rename(columns={'DateTime': 'year', 'ExtSiteID': 'wap', 'DatasetTypeID': 'take_type'}, inplace=True)
tsdata3.replace({'take_type': {9: 'Take Surface Water', 12: 'Take Groundwater'}}, inplace=True)

tsdata4 = pd.merge(crc_wap2, tsdata3, on=['year', 'wap', 'take_type'], how='left')
count1 = tsdata4.groupby(['year', 'wap']).crc.transform('count')

tsdata4['Value'] = (tsdata4['Value']/count1).round()
tsdata4['feav'] = (tsdata4['feav']/count1).round()

tsdata5 = tsdata4[~(tsdata4.Value > (tsdata4.feav*1.8))]

### Estimate metered allo and usage
usage1 = tsdata5.dropna().groupby(['year', 'take_type', 'CatchmentGroupName', 'use_type'])['feav', 'Value'].sum()
usage1.columns = ['Metered Allocation (m3)', 'Usage (m3)']

### Estimate total allocation
allo1 = tsdata5.groupby(['year', 'take_type', 'CatchmentGroupName', 'use_type']).feav.sum()
allo1.name = 'Total Allocation (m3)'

### For the count of waps
tsdata6 = tsdata5[['year', 'take_type', 'CatchmentGroupName', 'use_type', 'wap']].copy().drop_duplicates()
grp2 = tsdata6.groupby(['year', 'take_type', 'CatchmentGroupName', 'use_type'])['wap'].count()
grp2.name = 'All WAPs count'
tsdata7 = tsdata5.dropna()[['year', 'take_type', 'CatchmentGroupName', 'use_type', 'wap']].copy().drop_duplicates()
grp3 = tsdata7.groupby(['year', 'take_type', 'CatchmentGroupName', 'use_type'])['wap'].count()
grp3.name = 'Metered WAPs count'

### Combine all
all1 = pd.concat([allo1, usage1, grp2, grp3], axis=1).sort_index()
all1.index.name = 'Year End'

#all1.to_csv(os.path.join(export_dir, export1))
#
#
#
#
#permit2 = pd.merge(allo3, permit1, on='crc')
#
#permit2.to_csv(os.path.join(export_dir, export2), index=False)



all2 = all1.reset_index()
all3 = all2[all2.use_type != 'hydroelectric']

















