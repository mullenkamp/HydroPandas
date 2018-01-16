# -*- coding: utf-8 -*-
"""
Created on Mon May 08 09:58:51 2017

@author: MichaelEK

Script to extract time series data from Hydrotel and save them to sql tables.
"""
import pandas as pd
import os
from datetime import date
from hydropandas.io.tools.hydrotel import rd_hydrotel, hydrotel_sites_by_hydroid
from hydropandas.io.tools.mssql import to_mssql, rd_sql
from hydropandas.util.misc import save_df

#############################################
### Parameters
base_dir = r'\\fs02\ManagedShares2\Data\Surface Water\shared\base_data'

max_date_stmt = "IF OBJECT_ID('HydrotelTSDataDaily', 'U') IS NOT NULL select max(Time) from HydrotelTSDataDaily else select 0"
today1 = str(date.today())
#server1 = 'SQL2012DEV01'
#database1 = 'HydstraArchive'
dtype_dict = {'Site': 'VARCHAR(19)', 'Time': 'DATE', 'Value': 'float', 'HydroID': 'VARCHAR(29)'}

hydro_ids_dict = {'river / flow / rec / raw': 'Flow Rate',
               'aq / wl / rec / raw': 'Water Level',
               'atmos / precip / rec / raw': 'Rainfall Depth',
               'river / wl / rec / raw': 'Water Level',
               'river / T / rec / raw': 'Water Temperature'}
interval = 'D'
#export = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'HydstraTSDataHourly'}
export = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'HydrotelTSDataDaily'}

#############################################
### Determine the last date that was extracted
max_date1 = rd_sql(stmt=max_date_stmt, **export).loc[0][0]


### Iterate through hydstra codes and save as hdf files

for i in hydro_ids_dict:
    print('Hydro_id: ' + i)
    sites = hydrotel_sites_by_hydroid(i)
    s1 = rd_hydrotel(sites.site, i, from_date=past, to_date=today1, resample_code=interval).reset_index()
    s1['HydroID'] = i
    s1.rename(columns={'site': 'Site', 'time': 'Time', 'value': 'Value'}, inplace=True)
    err = write_sql(s1, dtype_dict=dtype_dict, create_table=False, **export)



#t7 = read_hdf(join(base_dir, hydstra_code_dict[i] + '_' + str(today1) + '.h5'))


