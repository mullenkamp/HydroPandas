# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import numpy as np
import pandas as pd
from pdsql import mssql
from allotools import allocation_ts
#from hilltoppy import web_service as wb
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

#sites = ['J36/0016', 'K37/3262', '69302']
datasets = {12: 'Take Groundwater'}
#cwms = ['kaikoura']
#catch_group = ['Ashburton River']
cwms = ['Christchurch - West Melton', 'Banks Peninsula']
#rdr_site = 'J36/0016-M1'
summ_col = 'CwmsName'

#base_url = 'http://wateruse.ecan.govt.nz'
#hts = 'WaterUse.hts'

from_date = '2012-07-01'
to_date = '2018-06-30'

export_dir = r'E:\ecan\local\Projects\requests\the_press\2019-01-25'
export1 = 'crc_usage_summary_2019-01-25.csv'
export2 = 'crc_feav_usage_2019-01-25.csv'
#export2 = 'swaz_usage_2018-12-17.csv'
#export3 = 'swaz_usage_pivot_2018-12-17.csv'


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

allo1 = allocation_ts.allo_ts(server, from_date, to_date, 'A-JUN', 'annual volume')
allo2 = allo1.reset_index()
allo2.rename(columns={'date': 'year', 'allo': 'feav'}, inplace=True)

############################################
### Extract data

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', summ_col], where_col={summ_col: cwms})

crc = mssql.rd_sql(server, database, crc_table, where_col={'in_gw_allo': [True], 'take_type': ['Take Groundwater']}).drop('mod_date', axis=1)
crc = crc.replace({'use_type': allocation_ts.param.use_type_dict})

allo3 = pd.merge(allo2, crc[['crc', 'take_type', 'allo_block']], on=['crc', 'take_type', 'allo_block'])

crc_wap = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'allo_block', 'wap'])
crc_wap1 = crc_wap[(crc_wap.take_type == 'Take Groundwater')]
#crc_wap1 = crc_wap1[crc_wap1.take_type.isin(list(datasets.values()))].copy()


sites2 = sites1.rename(columns={'ExtSiteID': 'wap'})
crc_wap1a = pd.merge(crc_wap1, sites2, on=['wap'])

crc_wap2 = pd.merge(allo3, crc_wap1a[['crc', 'take_type', 'allo_block', 'wap', summ_col]], on=['crc', 'take_type', 'allo_block'])

count0 = crc_wap2.groupby(['crc', 'take_type', 'allo_block', 'year']).crc.transform('count')

crc_wap2['feav'] = (crc_wap2['feav']/count0).round()

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_col={'ExtSiteID': crc_wap2.wap.unique().tolist(), 'DatasetTypeID': list(datasets.keys())}, from_date=from_date, to_date=to_date, date_col='DateTime')
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)

tsdata2 = grp_ts_agg(tsdata1, ['ExtSiteID', 'DatasetTypeID'], 'DateTime', 'A-JUN').Value.sum().round()
tsdata3 = tsdata2.reset_index().copy()
tsdata3.rename(columns={'DateTime': 'year', 'ExtSiteID': 'wap', 'DatasetTypeID': 'take_type'}, inplace=True)
tsdata3.replace({'take_type': datasets}, inplace=True)

tsdata4 = pd.merge(crc_wap2, tsdata3, on=['year', 'wap', 'take_type'], how='left')
count1 = tsdata4.groupby(['year', 'wap']).crc.transform('count')

tsdata4['Value'] = (tsdata4['Value']/count1).round()

tsdata5 = tsdata4[~(tsdata4.Value > (tsdata4.feav*1.8))]

### Estimate metered allo and usage
usage1 = tsdata5.dropna().groupby(['year', 'take_type', summ_col])['feav', 'Value'].sum()
usage1.columns = ['Metered Allocation (m3)', 'Usage (m3)']

### Estimate total allocation
allo1 = tsdata5.groupby(['year', 'take_type', summ_col]).feav.sum()
allo1.name = 'Total Allocation (m3)'

### For the count of waps
tsdata6 = tsdata5[['year', 'take_type', summ_col, 'wap']].copy().drop_duplicates()
grp2 = tsdata6.groupby(['year', 'take_type', summ_col])['wap'].count()
grp2.name = 'All WAPs count'
tsdata7 = tsdata5.dropna()[['year', 'take_type', summ_col, 'wap']].copy().drop_duplicates()
grp3 = tsdata7.groupby(['year', 'take_type', summ_col])['wap'].count()
grp3.name = 'Metered WAPs count'

### Combine all
all1 = pd.concat([allo1, usage1, grp2, grp3], axis=1).sort_index()
all1.index.name = 'Year End'

all1.to_csv(os.path.join(export_dir, export1))

### Agg by crc

crc_agg = tsdata5.groupby(['crc', 'year'])[['feav', 'Value']].sum()
crc_agg.loc[crc_agg['Value'] == 0, 'Value'] = np.nan

crc_agg.groupby(level='year').sum()

crc_agg1 = crc_agg[crc_agg.Value.notnull()].copy()
crc_agg1.groupby(level='year').sum()

crc_agg.rename(columns={'Value': 'Usage (m3)', 'feav': 'feav (m3)'}, inplace=True)

crc_agg.to_csv(os.path.join(export_dir, export2))


#### Daily usage by SWAZ
#
#swaz1 = pd.merge(tsdata1, sites1, on='ExtSiteID')
#swaz1.replace({'DatasetTypeID': datasets}, inplace=True)
#swaz1.rename(columns={'DatasetTypeID': 'take_type'}, inplace=True)
#
#swaz2 = swaz1.groupby(['SwazName', 'take_type',  'DateTime']).Value.sum().round()
#swaz2.name = 'Usage (m3)'
#
#swaz2.to_csv(os.path.join(export_dir, export2), header=True)
#
#swaz3 = swaz2.unstack([0, 1])
#
#swaz3.to_csv(os.path.join(export_dir, export3), header=True)
#
#
#### Extra checks
#
#big1 = swaz2.groupby(level=['SwazName']).quantile(0.97) * 2
#
#for name, val in swaz2.groupby(level=['SwazName']):
#    count1 = val[val > big1[name]]
#    print(count1)
#
#tsdata8 = tsdata1.groupby(['ExtSiteID', 'DateTime']).Value.sum().round()
#tsdata8.name = 'Usage (m3)'
#
#big1 = tsdata8.groupby(level=['ExtSiteID']).quantile(0.97) * 2
#
#for name, val in tsdata8.groupby(level=['ExtSiteID']):
#    count1 = val[val > big1[name]]
#    print(count1)
#
#
##################################################
#
#sites = ('K36/0927', 'K36/0854', 'K36/1000', 'K37/0792', 'K37/1442', 'K37/1725', 'K37/2034', 'K37/2238', 'K37/2239', 'K37/2893', 'K37/2894', 'K37/3582', 'L37/0239', 'L37/1265', 'L37/1430')
#
#from_date = '2016-06-15'
#to_date = '2016-07-15'
#
#site = 'K36/1000-M1'
#
#mtypes = wb.measurement_list(base_url, hts, site)
#mtypes
#
#td1 = wb.get_data(base_url, hts, site, 'Compliance Volume', from_date, to_date)
#
#td2 = td1.reset_index().drop(['Site', 'Measurement'], axis=1).set_index('DateTime')
#td2.idxmax()
#td2.plot()
#
#
#swaz = 'Mt Harding'
#
#d1 = tsdata5[tsdata5.SwazName == swaz].copy()
#d2 = d1.groupby(['crc', 'year']).sum()
#
#d2.to_csv(os.path.join(export_dir, 'test5.csv'))
#
#
#d2.loc[(slice(None), slice(None), 'CRC169504'), :]
#
#d2.loc['CRC169504']
#
#tsdata4[tsdata4.crc == 'CRC169504']
#
#tsdata4a = tsdata4.copy()
#
#tsdata4a['wap_count'] = tsdata4.groupby(['year', 'wap']).crc.transform('count')
#
#tsdata4a[tsdata4a.crc == 'CRC169504']
#
#c1 = tsdata4.groupby(['year', 'wap']).crc.count()
#
#tsdata4a[tsdata4a.wap == 'BY20/0089']
#
#
#all1.loc[(slice(None), 'Take Surface Water', 'Mt Harding'), :]
#
#allo2[allo2.crc == 'CRC169502']
#
#crc_wap2[crc_wap2.crc.isin(['CRC012123', 'CRC169502'])]

