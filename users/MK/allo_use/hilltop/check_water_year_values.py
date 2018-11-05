# -*- coding: utf-8 -*-
"""
Created on Mon Nov  5 14:38:55 2018

@author: michaelek
"""
import scipy
import numpy as np
import os
import pandas as pd
from pdsql import mssql
from allotools import allocation_ts
from hilltoppy import web_service as wb
import matplotlib.pyplot as plt

pd.options.display.max_columns = 10

#################################################
### Parameters

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'
ts_summ_table = 'TSDataNumericDailySumm'

datasets = [9, 12]

export_dir = r'E:\ecan\local\Projects\requests\katie\2018-11-06'
big_ex = 'all_highs.csv'
wy_ex = 'water_year_border_highs.csv'

#################################################
### Do work

sel_sites = mssql.rd_sql(server, database, ts_summ_table, where_col={'DatasetTypeID': datasets})

high1s_list = []

for site in sel_sites.ExtSiteID.unique():
    print(site)
    data1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'DatasetTypeID': datasets, 'ExtSiteID': [site]})
    data2 = data1.groupby(['ExtSiteID', 'DateTime']).sum().round()
#    sd1 = data2.std()[0] * 8
    quan1 = data2.quantile(0.99) * 2
    above1 = data2[data2 > quan1].dropna()
    high1s_list.append(above1)

high1s = pd.concat(high1s_list)
high1s3 = high1s.reset_index().copy()
high1s3['DateTime'] = pd.to_datetime(high1s3['DateTime'])

data2.plot()

site = 'BY20/0003'

sel1 = high1s3[(high1s3.DateTime.dt.day.isin([28, 29, 30]) & high1s3.DateTime.dt.month.isin([6])) | (high1s3.DateTime.dt.day.isin([1, 2, 3]) & high1s3.DateTime.dt.month.isin([7]))]

high1s3.to_csv(os.path.join(export_dir, big_ex), index=False)
sel1.to_csv(os.path.join(export_dir, wy_ex), index=False)
























































