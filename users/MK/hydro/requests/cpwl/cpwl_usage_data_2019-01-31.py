# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql
import geopandas as gpd
import gistools.vector as vec
from allotools import allocation_ts

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
datasets = [12]
#cwms = ['kaikoura']
catch_group = ['Selwyn River']
#cwms = ['Selwyn - Waihora']
#rdr_site = 'J36/0016-M1'

s1_shp = r'E:\ecan\shared\GIS_base\vector\cpw\S1.shp'
sheff_shp = r'E:\ecan\shared\GIS_base\vector\cpw\Sheffield.shp'

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

from_date = '2012-07-01'
to_date = '2018-06-30'

years = {'2015-01-01': '2015-06-30', '2016-01-01': '2016-06-30', '2017-01-01': '2017-06-30', '2018-01-01': '2018-06-30'}

#rdr_hts = [r'H:\Data\Annual\ending_2016\ending_2016.dsn', r'H:\Data\Annual\ending_2017\ending_2017.dsn', r'H:\Data\Annual\ending_2018\ending_20128.dsn']
#
#hts_dsn = r'H:\Data\WaterUSeAll.dsn'

export_dir = r'E:\ecan\local\Projects\requests\cpwl\2019-01-31'
export1 = 'cpwl_crc_usage_daily_2019-01-31.csv'
#export2 = 'cpwl_allo_usage_summary_2019-01-31.csv'
#export3 = 'cpwl_crc_wap_usage_data_2019-01-31.csv'



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

cpw_s1 = gpd.read_file(s1_shp)
cpw_sheff = gpd.read_file(sheff_shp)
cpw = pd.concat([cpw_s1, cpw_sheff])


## Pull out recorder data
#sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID'], where_col={'CatchmentGroupName': catch_group}).ExtSiteID.tolist()

#sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID'], where_col={'CwmsName': cwms}).ExtSiteID.tolist()

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY'])
sites2 = sites1[sites1.ExtSiteID.str.contains('[A-Z]+\d+/\d+')].copy()
sites3 = vec.xy_to_gpd('ExtSiteID', 'NZTMX', 'NZTMY', sites2)

sites4 = vec.sel_sites_poly(sites3, cpw)

#crc = mssql.rd_sql(server, database, crc_table).drop('mod_date', axis=1)
#crc.to_date = pd.to_datetime(crc.to_date, errors='coerce')
#crc.from_date = pd.to_datetime(crc.from_date)
#
#crc1 = crc[crc.use_type.str.contains('Irrigation') & crc.to_date.notnull()]

crc_wap = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'wap',], where_col={'wap': sites4.ExtSiteID.tolist(), 'take_type': ['Take Groundwater']}).drop_duplicates()

#crc2 = crc1[crc1.crc.isin(crc_wap1.crc)]
#crc3 = crc2[~((crc2.take_type == 'Take Groundwater') & (~crc2.in_gw_allo))]
#
#crc4_list = []
#
#for i in years:
#    sel1 = crc3[(crc3.from_date < i) & (crc3.to_date > i)].copy()
#    sel1['year'] = pd.Timestamp(years[i])
#    crc4_list.append(sel1)
#
#crc4 = pd.concat(crc4_list)[['crc', 'feav', 'year']].copy()
#crc_wap2 = pd.merge(crc4, crc_wap1[['crc', 'wap']], on='crc', how='left').drop('feav', axis=1)

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'ExtSiteID': crc_wap.wap.unique().tolist(), 'DatasetTypeID': datasets}, from_date=from_date, to_date=to_date, date_col='DateTime')
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)
tsdata1.rename(columns={'DateTime': 'date', 'ExtSiteID': 'wap', 'Value': 'Usage (m3)'}, inplace=True)

tsdata2 = pd.merge(crc_wap, tsdata1, on='wap')

count1 = tsdata2.groupby(['date', 'wap']).crc.transform('count')

tsdata2['Usage (m3)'] = (tsdata2['Usage (m3)']/count1).round()

tsdata3 = tsdata2.groupby(['crc', 'date']).sum().reset_index()


tsdata4 = grp_ts_agg(tsdata3, 'crc', 'date', 'A-JUN')['Usage (m3)'].sum().round().reset_index()
#
#c1 = crc_wap2.groupby(['year', 'wap']).crc.count()
#dups = c1[c1 > 1]
#
#tsdata4 = pd.merge(crc_wap2, tsdata3, on=['year', 'wap'])
#count1 = tsdata4.groupby(['year', 'wap']).crc.transform('count')
#
#tsdata4['Value'] = (tsdata4['Value']/count1).round()
#
#usage1 = tsdata4.groupby(['year']).Value.sum()
#usage1.name = 'Usage (m3)'
#
#allo1 = crc4.groupby(['year']).feav.sum()
#allo1.name = 'Total Allocation (m3)'
#
#crc5 = pd.merge(crc4, tsdata4[['crc', 'year']].drop_duplicates(), on=['crc', 'year'])
#allo2 = crc5.groupby(['year']).feav.sum()
#allo2.name = 'Metered Allocation (m3)'
#
#all1 = pd.concat([allo1, allo2, usage1], axis=1)
#all1.index.name = 'Year End'
#
#all1.to_csv(os.path.join(export_dir, export2))
#
#tsdata5 = tsdata4.groupby(['crc', 'year']).Value.sum().reset_index()
#
#crc_usage1 = pd.merge(crc4, tsdata5, on=['crc', 'year'], how='left')
#crc_usage1.rename(columns={'feav': 'allocation_m3', 'Value': 'usage_m3'}, inplace=True)
#crc_usage2 = crc_usage1.set_index(['year', 'crc'])

tsdata3.to_csv(os.path.join(export_dir, export1))

#ml4 = ws.measurement_list(base_url, hts, rdr_site)

#crc_wap_usage1 = pd.merge(tsdata4, crc4, on=['crc', 'year'])
#count2 = crc_wap_usage1.groupby(['year', 'crc']).wap.transform('count')
#crc_wap_usage1['feav'] = (crc_wap_usage1['feav']/count2).round()
#crc_wap_usage1.rename(columns={'feav': 'allocation_m3', 'Value': 'usage_m3'}, inplace=True)
#sites5 = sites2.rename(columns={'ExtSiteID': 'wap'})
#
#crc_wap_usage2 = pd.merge(crc_wap_usage1, sites5, on='wap', how='left')
#crc_wap_usage3 = crc_wap_usage2.set_index(['year', 'crc', 'wap'])
#
#crc_wap_usage3.to_csv(os.path.join(export_dir, export3))






































