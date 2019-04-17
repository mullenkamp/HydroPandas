# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 10:44:36 2019

@author: michaelek
"""
import os
import pandas as pd
from pdsql import mssql

pd.options.display.max_columns = 10

############################################
### Parameters

server = 'edwprod01'
database = 'hydro'
sites_table = 'ExternalSite'
lf_sites_table = 'LowFlowRestrSite'

sites_cols = ['ExtSiteID', 'NZTMX', 'NZTMY', 'CwmsName']
lf_cols = ['site', 'date', 'restr_category']

from_date = '2018-01-01'
to_date = '2018-02-28'

export_path = r'E:\ecan\local\Projects\requests\AnitaF\2019-04-09'
lf_cat_csv = 'lowflow_categories_2019-04-09.csv'
lf_cwms_csv = 'lowflow_cwms_summ_2019-04-09.csv'


############################################
### Process data

lf1 = mssql.rd_sql(server, database, lf_sites_table, lf_cols, where_in={'site_type': ['LowFlow']}, from_date=from_date, to_date=to_date, date_col='date')

lf1.loc[lf1['restr_category'] == 'Deactivated', 'restr_category'] = 'No'
lf1.loc[lf1['restr_category'] == 'Full', 'restr_category'] = 'Partial'

sites1 = mssql.rd_sql(server, database, sites_table, sites_cols, where_in={'ExtSiteID': lf1.site.unique().tolist()})
sites1.rename(columns={'ExtSiteID': 'site'}, inplace=True)

count1 = lf1.groupby('site').date.count().max()
lf2 = lf1.groupby('site').restr_category.value_counts()

lf3 = lf2.unstack(1)

lf3['category'] = 'Some Days on Restriction'
lf3.loc[lf3['No'] == count1, 'category'] = 'Not on Restriction'
lf3.loc[lf3['Partial'] == count1, 'category'] = 'All Days on Restriction'

lf4 = lf3.drop(['No', 'Partial'], axis=1).reset_index().copy()

both1 = pd.merge(lf4, sites1, on='site')

cwms1 = both1.groupby('CwmsName').category.value_counts().unstack(1)
cwms1[cwms1.isnull()] = 0
cwms1 = cwms1.astype(int)

both1.to_csv(os.path.join(export_path, lf_cat_csv), index=False)
cwms1.to_csv(os.path.join(export_path, lf_cwms_csv))





































































