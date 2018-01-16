# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 08:35:28 2018

@author: MichaelEK
"""
import pandas as pd
import geopandas as gpd
from hydropandas.io.tools.sql_arg_class import sql_arg
from hydropandas.io.tools.mssql import to_mssql, create_mssql_table, rd_sql
from hydropandas.tools.general.spatial.vector import pts_poly_join

#################################
### Parameters
## Import
sql_lowflow_site = 'lowflow_sites_gis'
sql_residual_site = 'residual_sites_gis'
sql_cwms = 'cwms_gis'
sql_catch_grp = 'catch_grp_gis'
sql_site_restr = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowRestrSite'}

## Export
sql_site_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowSiteGeo1'}
site_dtype_dict = {'SiteID': 'int', 'site': 'varchar(19)', 'site_type': 'varchar(9)', 'cwms': 'varchar(49)', 'catch_grp': 'int', 'catch_grp_name': 'varchar(49)', 'flow_method': 'varchar(59)', 'crc_count': 'int'}
site_pkey = ['site']

sql_cwms_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowSiteCWMS'}
cwms_dtype_dict = {'cwms': 'varchar(59)', 'lowflow_tot': 'int', 'residual_tot': 'int', 'lowflow_crc': 'int', 'residual_crc': 'int', 'lowflow_telem': 'int', 'residual_telem': 'int'}
cwms_pkey = 'cwms'

sql_catch_dict = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'LowFlowSiteCatch'}
catch_dtype_dict = {'catch_grp': 'int', 'lowflow_tot': 'int', 'residual_tot': 'int', 'lowflow_crc': 'int', 'residual_crc': 'int', 'lowflow_telem': 'int', 'residual_telem': 'int'}
catch_pkey = 'catch_grp'

################################
### Read data and combine

restr = rd_sql(where_col={'date': ['2018-01-15']}, **sql_site_restr)
restr1 = restr[['site', 'site_type', 'flow_method', 'crc_count']].copy()

sql1 = sql_arg()

lowflow_dict = sql1.get_dict(sql_lowflow_site)
lowflow_dict

lowflow_sites = rd_sql(**sql1.get_dict(sql_lowflow_site))
#lowflow_sites['site_type'] = 'lowflow'
residual_sites = rd_sql(**sql1.get_dict(sql_residual_site))
#residual_sites['site_type'] = 'residual'

sites = pd.concat([lowflow_sites, residual_sites])

cwms = rd_sql(**sql1.get_dict(sql_cwms))
catch_grp = rd_sql(**sql1.get_dict(sql_catch_grp))

sites_cwms, cwms1 = pts_poly_join(sites, cwms, 'cwms')

sites_cwms_catch, poly2 = pts_poly_join(sites_cwms, catch_grp, ['catch_grp', 'catch_grp_name'])

sites_cwms_catch1 = pd.DataFrame(sites_cwms_catch.drop('geometry', axis=1).copy())

set1 = pd.merge(sites_cwms_catch1, restr1, on='site')

## Summarize
# CWMS
cwms_grp = set1.groupby('cwms')
cwms_lowflow = cwms_grp['site_type'].apply(lambda x: (x == 'LowFlow').sum())
cwms_residual = cwms_grp['site_type'].apply(lambda x: (x == 'Residual').sum())
cwms_lowflow_crc = cwms_grp.apply(lambda x: x.loc[x.site_type == 'LowFlow', 'crc_count'].sum())
cwms_residual_crc = cwms_grp.apply(lambda x: x.loc[x.site_type == 'Residual', 'crc_count'].sum())
cwms_lowflow_telem = cwms_grp.apply(lambda x: x.loc[(x.site_type == 'LowFlow') & (x.flow_method == 'Telemetered'), 'flow_method'].count())
cwms_residual_telem = cwms_grp.apply(lambda x: x.loc[(x.site_type == 'Residual') & (x.flow_method == 'Telemetered'), 'flow_method'].count())

cwms_summ = pd.concat([cwms_lowflow, cwms_residual, cwms_lowflow_crc, cwms_residual_crc, cwms_lowflow_telem, cwms_residual_telem], axis=1)
cwms_summ.columns = ['lowflow_tot', 'residual_tot', 'lowflow_crc', 'residual_crc', 'lowflow_telem', 'residual_telem']

# catch grp
catch_grp_grp = set1.groupby('catch_grp')
catch_grp_lowflow = catch_grp_grp['site_type'].apply(lambda x: (x == 'LowFlow').sum())
catch_grp_residual = catch_grp_grp['site_type'].apply(lambda x: (x == 'Residual').sum())
catch_grp_lowflow_crc = catch_grp_grp.apply(lambda x: x.loc[x.site_type == 'LowFlow', 'crc_count'].sum())
catch_grp_residual_crc = catch_grp_grp.apply(lambda x: x.loc[x.site_type == 'Residual', 'crc_count'].sum())
catch_grp_lowflow_telem = catch_grp_grp.apply(lambda x: x.loc[(x.site_type == 'LowFlow') & (x.flow_method == 'Telemetered'), 'flow_method'].count())
catch_grp_residual_telem = catch_grp_grp.apply(lambda x: x.loc[(x.site_type == 'Residual') & (x.flow_method == 'Telemetered'), 'flow_method'].count())

catch_grp_summ = pd.concat([catch_grp_lowflow, catch_grp_residual, catch_grp_lowflow_crc, catch_grp_residual_crc, catch_grp_lowflow_telem, catch_grp_residual_telem], axis=1)
catch_grp_summ.columns = ['lowflow_tot', 'residual_tot', 'lowflow_crc', 'residual_crc', 'lowflow_telem', 'residual_telem']

### Same to SQL
create_mssql_table(primary_keys=site_pkey, dtype_dict=site_dtype_dict, **sql_site_dict)
create_mssql_table(primary_keys=cwms_pkey, dtype_dict=cwms_dtype_dict, **sql_cwms_dict)
create_mssql_table(primary_keys=catch_pkey, dtype_dict=catch_dtype_dict, **sql_catch_dict)

to_mssql(set1, **sql_site_dict)
to_mssql(cwms_summ, index=True, **sql_cwms_dict)
to_mssql(catch_grp_summ, **sql_catch_dict, index=True)























