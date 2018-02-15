# -*- coding: utf-8 -*-
"""
Created on Thu Feb 15 11:16:50 2018

@author: MichaelEK
"""
import os
from hydropandas.io.tools.mssql import rd_sql


###############################################
### Parameters

server = 'SQL2012DEV01'
database = 'Hydro'
summ_table = 'TSDataNumericDailySummTemp'
sites_table = 'SiteLinkTemp'
hydro_id_table = 'vFeatureMtypeSourceLongNames'

sites_cols = ['Site', 'NZTMX', 'NZTMY']

hydro_ids = [4, 5, 6, 7, 8]

base_dir = r'E:\ecan\local\Projects\requests\takiwa\2018-02-15'
summ_csv = 'site_ts_data_summary.csv'
site_csv = 'site_location.csv'
hydro_csv = 'IDs_table.csv'

###############################################
### Read data

ts_summ1 = rd_sql(server, database, summ_table, where_col={'FeatureMtypeSourceID': hydro_ids})

sites = rd_sql(server, database, sites_table, col_names=sites_cols)

sites2 = sites[sites.Site.isin(ts_summ1.Site.unique())].copy()

ts_summ2 = ts_summ1[ts_summ1.Site.isin(sites2.Site)].copy()

hydro_ids1 = rd_sql(server, database, hydro_id_table, where_col={'FeatureMtypeSourceID': hydro_ids})

hydro_ids1.LoggingMethodName = 'Mean'

### Export data

ts_summ2.to_csv(os.path.join(base_dir, summ_csv), index=False)
sites2.to_csv(os.path.join(base_dir, site_csv), index=False)
hydro_ids1.to_csv(os.path.join(base_dir, hydro_csv), index=False)

































