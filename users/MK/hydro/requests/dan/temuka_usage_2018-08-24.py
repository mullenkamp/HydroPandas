# -*- coding: utf-8 -*-
"""
Created on Fri Aug 24 14:38:51 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql
from hilltoppy import web_service as ws


def grp_ts_agg(df, grp_col, ts_col, freq_code, discrete=False):
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
    discrete : bool
        Is the data discrete? Will use proper resampling using linear interpolation.

    Returns
    -------
    Pandas resample object
    """

    df1 = df.copy()
    if type(df[ts_col].iloc[0]) is pd.Timestamp:
        df1.set_index(ts_col, inplace=True)
        if isinstance(grp_col, str):
            grp_col = [grp_col]
        else:
            grp_col = grp_col[:]
        if discrete:
            val_cols = [c for c in df1.columns if c not in grp_col]
            df1[val_cols] = (df1[val_cols] + df1[val_cols].shift(-1))/2
        grp_col.extend([pd.Grouper(freq=freq_code)])
        df_grp = df1.groupby(grp_col)
        return (df_grp)
    else:
        print('Make one column a timeseries!')

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'
sites_table = 'ExternalSite'
crc_allo_table = 'CrcAllo'
crc_wap_allo_table = 'CrcWapAllo'

swaz = 'temuka'
#sites = ['J36/0016', 'K37/3262', '69302']
datasets = [9, 12]

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

export_dir = r'E:\ecan\local\Projects\requests\dan\2018-08-24'
export1 = 'temuka_usage_2018-08-24.csv'
export2 = 'temuka_zero_usage_2018-08-24.csv'
export3 = 'temuka_zero_usage_crc_2018-08-24.csv'

############################################
### Extract data

sites = mssql.rd_sql(server, database, sites_table, where_col={'SwazName': [swaz]})

tsdata = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_col={'ExtSiteID': sites.ExtSiteID.tolist(), 'DatasetTypeID': datasets})
tsdata.DateTime = pd.to_datetime(tsdata.DateTime)
tsdata = tsdata[(tsdata.DateTime < '2018-07-01') & (tsdata.DateTime >= '2010-07-01')].copy()

grp1 = grp_ts_agg(tsdata, ['ExtSiteID', 'DatasetTypeID'], 'DateTime', 'A-JUN').Value

tot1 = grp1.sum()
tot1.name = 'total'
count1 = grp1.count()
count1.name = 'count'

set1 = pd.concat([tot1, count1], axis=1)

zeros = set1[(set1.total < 100) & (set1['count'] > 350)]

grp2 = grp_ts_agg(tsdata, ['ExtSiteID', 'DatasetTypeID'], 'DateTime', 'M').Value

w1 = grp2.sum()
bad1s = w1[w1 > 50000000].reset_index().ExtSiteID.unique().tolist()
grp2.count()

tsdata1 = tsdata[~tsdata.ExtSiteID.isin(bad1s)]

grp3 = grp_ts_agg(tsdata1, 'DatasetTypeID', 'DateTime', 'M').Value

sum3 = grp3.sum().round()
sum3.name = 'tot_m3'
count3 = grp3.count()
count3.name = 'count1'

set3 = pd.concat([sum3, count3], axis=1).reset_index()
set3['n_waps'] = (set3.count1/set3.DateTime.dt.daysinmonth).round()

set3['type'] = 'SW'
set3.loc[set3['DatasetTypeID'] == 12, 'type'] = 'GW'

set4 = set3.drop(['DatasetTypeID', 'count1'], axis=1)

set4.to_csv(os.path.join(export_dir, export1), index=False)
zeros.to_csv(os.path.join(export_dir, export2))

zeros1 = zeros.reset_index().ExtSiteID.unique().tolist()

crc_wap1 = mssql.rd_sql(server, database, crc_wap_allo_table, where_col={'wap': zeros1})
crc_wap2 = crc_wap1.crc.unique().tolist()
crc1 = mssql.rd_sql(server, database, crc_allo_table, where_col={'crc': crc_wap2})

crc2 = pd.merge(crc1, crc_wap1[['crc', 'take_type', 'allo_block', 'wap']], on=['crc', 'take_type', 'allo_block'])

crc2.to_csv(os.path.join(export_dir, export3), index=False)























