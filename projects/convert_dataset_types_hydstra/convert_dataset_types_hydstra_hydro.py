# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 08:44:22 2018

@author: MichaelEK
"""

import pandas as pd
from pdsql import mssql, create_engine


#########################################
### Parameters

server  ='sql2012test01'
database = 'hydro'

site_tab = 'ExternalSite'
dataset_type_tab = 'DatasetType'

data_provider_tab = 'DataProvider'


#######################################
### Read in data

provider1 = mssql.rd_sql(server, database, data_provider_tab, ['DataProviderID', 'DataProvider'])
provider1.DataProvider = provider1.DataProvider.str.lower()
provider2 = provider1[provider1.DataProvider != 'ecan'].copy()

site_owner = mssql.rd_sql(server, database, site_tab, ['ExtSiteID', 'Owner'], where_col={'Owner': provider2.DataProvider.tolist()})
site_owner.Owner = site_owner.Owner.str.lower()
site_owner.rename(columns={'Owner': 'DataProvider'}, inplace=True)
site_owner = site_owner[~site_owner.ExtSiteID.str.match('[A-Z]+\d+/\d+')].copy()

dataset_types = mssql.rd_sql(server, database, dataset_type_tab).drop('DatasetTypeName', axis=1)

daily_summ1 = mssql.rd_sql(server, database, 'TSDataNumericDailySumm', ['ExtSiteID', 'DatasetTypeID'], where_col={'ExtSiteID': site_owner.ExtSiteID.tolist()})

daily_summ2 = pd.merge(daily_summ1, site_owner, on='ExtSiteID', how='left')
daily_summ3 = pd.merge(daily_summ2, dataset_types, on='DatasetTypeID', how='left').drop('DataProviderID', axis=1)
daily_summ4 = pd.merge(daily_summ3, provider2, on='DataProvider', how='left')

daily_summ4.rename(columns={'DatasetTypeID': 'DTypeID'}, inplace=True)

daily_summ5 = pd.merge(daily_summ4, dataset_types, on=['FeatureID', 'MTypeID', 'CTypeID', 'DataCodeID', 'DataProviderID'], how='left')

assert len(daily_summ5[daily_summ5.DatasetTypeID.isnull()]) == 0

base_stmt = "update TSDataNumericDaily set DatasetTypeID = {ds_new} where ExtSiteID = '{site}' and DatasetTypeID = {ds_old}"

engine = create_engine('mssql', server, database)
conn = engine.connect()

for row in daily_summ5.itertuples():
    print(row)
    stmt = base_stmt.format(ds_new=row.DatasetTypeID, site=row.ExtSiteID, ds_old=row.DTypeID)
    trans = conn.begin()
    conn.execute(stmt)
    trans.commit()

conn.close()




