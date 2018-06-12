# -*- coding: utf-8 -*-
"""
Created on Wed Apr  4 09:52:48 2018

@author: michaelek
"""
import os
from pdsql.mssql import rd_sql


#####################################
### Parameters

server = 'sql2012dev01'
database = 'Hydro'

base_dir = r'E:\ecan\git\HydroPandas\users\MK\hydro\data_structure\tables'

tab_lst = ['BgaugingLink', 'DataProviderMaster', 'FeatureMaster', 'FeatureMtypeSource', 'HydstraLink', 'HydstraQualCodeLink', 'LoggingMethodMaster', 'MSourceMaster', 'MtypeMaster', 'MtypeSecMaster', 'WusLink']

#####################################
### Extract tables

for l in tab_lst:
    tab1 = rd_sql(server, database, l)
    tab1.to_csv(os.path.join(base_dir, l + '.csv'), index=False)

####################################
### Testing






