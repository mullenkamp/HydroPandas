# -*- coding: utf-8 -*-
"""
Created on Thu Jan 25 11:22:51 2018

@author: MichaelEK
"""

import numpy as np
import pandas as pd
from datetime import datetime
from hydropandas.io.tools.mssql import rd_sql, to_mssql
from hydropandas.io.tools.sql_arg_class import sql_arg

#######################################
### Parameters

server = 'sql2012dev01'
database = 'Hydro'
view_site_table = 'vSiteData'
temp_site_table = 'SiteLinkTemp'
master_site_table = 'SiteMaster'
link_master_table = 'SiteLinkMaster'

sql_log = {'server': 'SQL2012DEV01', 'database': 'Hydro', 'table': 'ExtractionLog'}

today1 = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

#######################################
### All sites and comparison between view and temp table

view_sites = rd_sql(server, database, view_site_table)
temp_site = rd_sql(server, database, temp_site_table)

site_index = ~view_sites.Site.isin(temp_site.Site)

try:
    if any(site_index):
        new_sites = view_sites.loc[site_index].copy()
        max_site_id = temp_site.SiteID.max()
        new_site_ids = np.arange(1, len(new_sites) + 1) + max_site_id
        new_sites['SiteID'] = new_site_ids
        temp_site = pd.concat([temp_site, new_sites])
        to_mssql(new_sites, server, database, temp_site_table)
        to_mssql(new_sites.drop('Site', axis=1), server, database, master_site_table)
        new_link = new_sites[['Site', 'SiteID', 'DataProviderID']].copy()
        new_link.rename(columns={'Site': 'EcanSiteID'}, inplace=True)
        to_mssql(new_link, server, database, link_master_table)

        log1 = pd.DataFrame([[today1, 'Sites tables', 'pass', 'Additional sites were added', today1]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime'])
        to_mssql(log1, **sql_log)
    else:
        log1 = pd.DataFrame([[today1, 'Sites tables', 'pass', 'No additional sites were added', today1]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime'])
        to_mssql(log1, **sql_log)
except Exception as err:
    err1 = err
    print(err1)
    log2 = pd.DataFrame([[today1, 'Sites tables', 'fail', str(err1), today1]], columns=['Time', 'HydroTable', 'RunResult', 'Comment', 'FromTime'])
    to_mssql(log2, **sql_log)






