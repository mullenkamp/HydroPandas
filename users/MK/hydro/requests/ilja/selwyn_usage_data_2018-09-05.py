# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'
sites_table = 'ExternalSite'
crc_wap_table = 'CrcWapAllo'
crc_table = 'CrcAllo'

#sites = ['J36/0016', 'K37/3262', '69302']
datasets = [9, 12]
#cwms = ['kaikoura']
catch_group = ['Selwyn River']
#rdr_site = 'J36/0016-M1'

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

from_date = '2015-07-01'
to_date = '2018-06-30'

years = {'2016-01-01': '2016-06-30', '2017-01-01': '2017-06-30', '2018-01-01': '2018-06-30'}

#rdr_hts = [r'H:\Data\Annual\ending_2016\ending_2016.dsn', r'H:\Data\Annual\ending_2017\ending_2017.dsn', r'H:\Data\Annual\ending_2018\ending_20128.dsn']
#
#hts_dsn = r'H:\Data\WaterUSeAll.dsn'

export_dir = r'E:\ecan\local\Projects\requests\Ilja\2018-09-05'
#export1 = 'selwyn_usage_data_2018-09-06.csv'
export2 = 'selwyn_allo_usage_summary_2018-09-06.csv'


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
### Extract data

## Pull out recorder data
sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID'], where_col={'CatchmentGroupName': catch_group}).ExtSiteID.tolist()

crc = mssql.rd_sql(server, database, crc_table).drop('mod_date', axis=1)
crc.to_date = pd.to_datetime(crc.to_date, errors='coerce')
crc.from_date = pd.to_datetime(crc.from_date)

crc1 = crc[crc.use_type.str.contains('Irrigation') & crc.to_date.notnull()]

crc_wap = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'allo_block', 'wap', 'in_sw_allo'], where_col={'wap': sites1})
crc_wap1 = crc_wap[(crc_wap.take_type == 'Take Groundwater')]

crc2 = crc1[crc1.crc.isin(crc_wap1.crc)]
crc3 = crc2[~((crc2.take_type == 'Take Groundwater') & (~crc2.in_gw_allo))]

crc4_list = []

for i in years:
    sel1 = crc3[(crc3.from_date < i) & (crc3.to_date > i)].copy()
    sel1['year'] = pd.Timestamp(years[i])
    crc4_list.append(sel1)

crc4 = pd.concat(crc4_list)[['crc', 'feav', 'year']].copy()
crc_wap2 = pd.merge(crc4, crc_wap1[['crc', 'wap']], on='crc', how='left').drop('feav', axis=1)

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'ExtSiteID': crc_wap2.wap.unique().tolist(), 'DatasetTypeID': datasets}, from_date=from_date, to_date=to_date, date_col='DateTime')
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)

tsdata2 = grp_ts_agg(tsdata1, 'ExtSiteID', 'DateTime', 'A-JUN').Value.sum().round()
tsdata3 = tsdata2.reset_index().copy()
tsdata3.rename(columns={'DateTime': 'year', 'ExtSiteID': 'wap'}, inplace=True)

c1 = crc_wap2.groupby(['year', 'wap']).crc.count()
dups = c1[c1 > 1]

tsdata4 = pd.merge(crc_wap2, tsdata3, on=['year', 'wap'])

usage1 = tsdata4.groupby(['year']).Value.sum()
usage1.name = 'Usage (m3)'

allo1 = crc4.groupby(['year']).feav.sum()
allo1.name = 'Total Allocation (m3)'

crc5 = pd.merge(crc4, tsdata4[['crc', 'year']].drop_duplicates(), on=['crc', 'year'])
allo2 = crc5.groupby(['year']).feav.sum()
allo2.name = 'Metered Allocation (m3)'

all1 = pd.concat([allo1, allo2, usage1], axis=1)
all1.index.name = 'Year End'

all1.to_csv(os.path.join(export_dir, export2))


#ml4 = ws.measurement_list(base_url, hts, rdr_site)


