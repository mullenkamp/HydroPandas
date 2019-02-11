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

server2 = 'sql2012dev01'
est_usage_table = 'est_allo_usage_2017_09_28'

#sites = ['J36/0016', 'K37/3262', '69302']
datasets = {9: 'Take Surface Water', 12: 'Take Groundwater'}
#cwms = ['kaikoura']
#catch_group = ['Ashburton River']
#cwms = ['Christchurch - West Melton', 'Banks Peninsula']
#rdr_site = 'J36/0016-M1'
catch = ['Rangitata River']
summ_col = 'CatchmentName'

#base_url = 'http://wateruse.ecan.govt.nz'
#hts = 'WaterUse.hts'

from_date = '1987-07-01'
to_date = '2018-06-30'

export_dir = r'E:\ecan\local\Projects\requests\lgoima\2019-02-04'
export1 = 'allo_usage_summary_2019-02-04.csv'
#export2 = 'crc_feav_usage_2019-01-25.csv'
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
### Get sites and crc

#sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', summ_col], where_col={summ_col: catch})
#
#sites2 = sites1[sites1.ExtSiteID.str.contains('[A-Z]+\d\d/\d\d\d\d')].copy()
#sites2.rename(columns={'ExtSiteID': 'wap'}, inplace=True)
#
#crc_wap = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'allo_block', 'wap'], where_col={'wap': sites2.wap.tolist()})
#
#crc = mssql.rd_sql(server, database, crc_table, where_col={'crc': crc_wap.crc.unique().tolist()}).drop('mod_date', axis=1)
#crc = crc.replace({'use_type': allocation_ts.param.use_type_dict})



############################################
### Estimate allocation

allo1 = allocation_ts.allo_ts(server, from_date, to_date, 'A-JUN', 'annual volume', site_filter={summ_col: catch}).reset_index()
allo1.rename(columns={'date': 'year', 'allo': 'feav'}, inplace=True)
allo1a = allo1.groupby(['crc', 'take_type', 'year'])['feav'].sum().reset_index()

############################################
### Extract data

est_usage1 = mssql.rd_sql(server2, database, est_usage_table, where_col={'crc': allo1.crc.unique().tolist()})

est_usage2 = est_usage1.groupby(['crc', 'take_type', 'time'])[['ann_allo_m3', 'mon_allo_m3', 'usage_est']].sum().reset_index()
est_usage2['time'] = pd.to_datetime(est_usage2['time'])

grp1 = grp_ts_agg(est_usage2, 'crc', 'time', 'A-JUN')
est_usage3a = grp1.usage_est.sum()
ann_allo1 = grp1.ann_allo_m3.first()
est_usage3 = pd.concat([ann_allo1, est_usage3a], axis=1).reset_index()
est_usage3.rename(columns={'time': 'year', 'ann_allo_m3': 'allo'}, inplace=True)
est_usage3.replace({'crc': {'CRC011237': 'CRC182542'}}, inplace=True)

allo1a = allo1a[~allo1a.crc.isin(['CRC011237'])].copy()

allo2 = allo1a[allo1a.year <= '2008-06-30'].copy()

allo3 = pd.merge(allo2, est_usage3, on=['crc', 'year'], how='left')

allo4 = allo3.dropna().copy()

allo4['ratio'] = allo4['feav']/allo4['allo']
allo4.loc[allo4['ratio'] == np.inf, 'ratio'] = 1

allo4['usage_est'] = allo4['usage_est'] * allo4['ratio']

allo5 = allo4.drop(['allo', 'ratio'], axis=1).copy()
allo5['ratio'] = allo5['usage_est']/ allo5['feav']

index1 = allo5['ratio'] < 1
allo6 = allo5[index1].copy()

allo7 = allo6.groupby(['year', 'take_type']).sum()
allo7['ratio2'] = allo7['usage_est']/allo7['feav']
#allo7.loc[(slice(None), 'Take Surface Water'), 'ratio2'] = allo7.loc[(slice(None), 'Take Surface Water'), 'ratio2']*0.5

allo8 = pd.merge(allo5, allo7['ratio2'].reset_index(), on=['year', 'take_type'])

index2 = allo8['ratio'] < 1
allo8.loc[~index2, 'usage_est'] = allo8.loc[~index2, 'feav'] * allo8.loc[~index2, 'ratio2']

allo9 = allo8.drop(['ratio', 'ratio2'], axis=1)
allo9.loc[allo9.take_type == 'Take Surface Water', 'usage_est'] = allo9.loc[allo9.take_type == 'Take Surface Water', 'usage_est']*0.5


#allo9.groupby(['take_type', 'year']).sum()



### New data

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', summ_col], where_col={summ_col: catch})

crc = mssql.rd_sql(server, database, crc_table).drop('mod_date', axis=1)
crc = crc.replace({'use_type': allocation_ts.param.use_type_dict})

allo3 = pd.merge(allo1, crc[['crc', 'take_type', 'allo_block']], on=['crc', 'take_type', 'allo_block'])
allo3 = allo3[allo3.year > '2008-06-30'].copy()

crc_wap = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'allo_block', 'wap'])
#crc_wap1 = crc_wap[(crc_wap.take_type == 'Take Groundwater')]
#crc_wap1 = crc_wap1[crc_wap1.take_type.isin(list(datasets.values()))].copy()


sites2 = sites1.rename(columns={'ExtSiteID': 'wap'})
crc_wap1a = pd.merge(crc_wap, sites2, on=['wap'])

crc_wap2 = pd.merge(allo3, crc_wap1a[['crc', 'take_type', 'allo_block', 'wap', summ_col]], on=['crc', 'take_type', 'allo_block'])

count0 = crc_wap2.groupby(['crc', 'take_type', 'allo_block', 'year']).crc.transform('count')

crc_wap2['feav'] = (crc_wap2['feav']/count0).round()

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_col={'ExtSiteID': crc_wap2.wap.unique().tolist(), 'DatasetTypeID': list(datasets.keys())}, from_date='2008-07-01', to_date=to_date, date_col='DateTime')
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)

tsdata2 = grp_ts_agg(tsdata1, ['ExtSiteID', 'DatasetTypeID'], 'DateTime', 'A-JUN').Value.sum().round()
tsdata3 = tsdata2.reset_index().copy()
tsdata3.rename(columns={'DateTime': 'year', 'ExtSiteID': 'wap', 'DatasetTypeID': 'take_type'}, inplace=True)
tsdata3.replace({'take_type': datasets}, inplace=True)

tsdata4 = pd.merge(crc_wap2, tsdata3, on=['year', 'wap', 'take_type'], how='left')
count1 = tsdata4.groupby(['year', 'wap']).crc.transform('count')

tsdata4['Value'] = (tsdata4['Value']/count1).round()

tsdata5 = tsdata4[~(tsdata4.Value > (tsdata4.feav*1.8))].copy()

good_index = (tsdata5.Value > 100)

usage1 = tsdata5[good_index].copy()

usage2 = usage1.groupby(['take_type', 'year'])[['feav', 'Value']].sum()
ratio1 = (usage2['Value']/usage2['feav'])
ratio1.name = 'ratio'

usage3 = pd.merge(tsdata5, ratio1.reset_index(), on=['take_type', 'year'])
usage3['usage_est'] = usage3.feav * usage3.ratio

good_index = (usage3.Value > 100)

usage3.loc[~good_index, 'Value'] = usage3.loc[~good_index, 'usage_est']

usage4 = usage3.groupby(['crc', 'take_type', 'year']).sum().drop(['ratio', 'usage_est'], axis=1).reset_index()

usage4.rename(columns={'Value': 'usage_est'}, inplace=True)

### Combine datasets

usage5 = pd.concat([allo9, usage4])

summ1 = usage5.groupby(['take_type', 'year']).sum().reset_index()
#summ1['ratio'] = summ1['usage_est']/summ1['feav']

summ1.rename(columns={'take_type': 'Take Type', 'year': 'Water Year End', 'feav': 'Full Effective Annual Volume (m3)', 'usage_est': 'Usage Estimate (m3)'}, inplace=True)

summ1.to_csv(os.path.join(export_dir, export1), index=False)







