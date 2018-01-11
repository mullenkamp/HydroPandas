# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 08:35:28 2018

@author: MichaelEK
"""
import pandas as pd
from hydropandas.io.tools.sql_arg_class import sql_arg
from hydropandas.io.tools.mssql import to_mssql, create_mssql_table, rd_sql
from hydropandas.tools.general.spatial.vector import pts_poly_join

#################################
### Parameters
## Import
sql_lowflow_site = 'lowflow_sites_gis'
sql_cwms = 'cwms_gis'
sql_catch_grp = 'catch_grp_gis'

## Export
sql_site_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowSiteGeo1'}
site_dtype_dict = {'SiteID': 'int', 'site': 'varchar(19)', 'cwms': 'varchar(49)', 'catch_grp': 'int', 'catch_grp_name': 'varchar(49)'}
site_pkey = ['site']

################################
### Read data and combine

sql1 = sql_arg()

sites = rd_sql(**sql1.get_dict(sql_lowflow_site))
cwms = rd_sql(**sql1.get_dict(sql_cwms))
catch_grp = rd_sql(**sql1.get_dict(sql_catch_grp))

sites_cwms, cwms1 = pts_poly_join(sites, cwms, 'cwms')

sites_cwms_catch, poly2 = pts_poly_join(sites_cwms, catch_grp, ['catch_grp', 'catch_grp_name'])

sites_cwms_catch1 = pd.DataFrame(sites_cwms_catch.drop('geometry', axis=1).copy())

### Same to SQL
create_mssql_table(primary_keys=site_pkey, dtype_dict=site_dtype_dict, **sql_site_dict)

to_mssql(sites_cwms_catch1, **sql_site_dict)



