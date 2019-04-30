# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 09:32:35 2019

@author: michaelek
"""
import os
import pandas as pd
from gistools import rec, vector
from ecandbparams import sql_arg
from allotools import AlloUsage
from pdsql import mssql

pd.options.display.max_columns = 10
today1 = str(pd.Timestamp.today().date())

#######################################
### Parameters

py_path = os.path.realpath(os.path.dirname(__file__))
project_path = os.path.split(py_path)[0]

site_csv = 'ashburton_sites.csv'

server = 'edwprod01'
database = 'hydro'
sites_table = 'ExternalSite'
crc_wap_table = 'CrcWapAllo'

rec_rivers_sql = 'rec_rivers_gis'
rec_catch_sql = 'rec_catch_gis'

from_date = '2019-04-01'
to_date = '2019-06-30'

input_path = os.path.join(project_path, 'input')
results_path = os.path.join(project_path, 'results')

catch_del_shp = 'catch_del_{}.shp'.format(today1)
allo_csv = 'allo_{}.csv'.format(today1)
allo_wap_csv = 'allo_wap_{}.csv'.format(today1)
wap_shp = 'wap_{}.shp'.format(today1)
sites_shp = 'sites_{}.shp'.format(today1)

######################################
### Read in data

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY'])

ash_sites1 = pd.read_csv(os.path.join(input_path, site_csv)).site.astype(str)

sites0 = sites1[sites1.ExtSiteID.isin(ash_sites1)].copy()

sites2 = vector.xy_to_gpd('ExtSiteID', 'NZTMX', 'NZTMY', sites0)
sites2.to_file(os.path.join(results_path, sites_shp))

sql1 = sql_arg()

rec_rivers_dict = sql1.get_dict(rec_rivers_sql)
rec_catch_dict = sql1.get_dict(rec_catch_sql)

rec_rivers = mssql.rd_sql(**rec_rivers_dict)
rec_catch = mssql.rd_sql(**rec_catch_dict)

###################################
### Catchment delineation and WAPs

catch1 = rec.catch_delineate(sites2, rec_rivers, rec_catch)
catch1.to_file(os.path.join(results_path, catch_del_shp))

wap1 = mssql.rd_sql(server, database, crc_wap_table, ['wap']).wap.unique()

sites3 = sites1[sites1.ExtSiteID.isin(wap1)].copy()

sites4 = vector.xy_to_gpd('ExtSiteID', 'NZTMX', 'NZTMY', sites3)
sites4.rename(columns={'ExtSiteID': 'wap'}, inplace=True)

sites5, poly1 = vector.pts_poly_join(sites4, catch1, 'ExtSiteID')
sites5.to_file(os.path.join(results_path, wap_shp))

##################################
### Get crc data

allo1 = AlloUsage(crc_wap_filter={'wap': sites5.wap.unique().tolist()}, from_date=from_date, to_date=to_date)

#allo1.allo[allo1.allo.crc_status == 'Terminated - Replaced']

allo_wap1 = allo1.allo_wap.copy()
allo_wap2 = pd.merge(allo_wap1.reset_index(), sites5.drop('geometry', axis=1), on='wap')

allo_wap2.to_csv(os.path.join(results_path, allo_wap_csv))
allo1.allo.to_csv(os.path.join(results_path, allo_csv))


#################################
### Testing

#nzreach1 = 13053151
#
#c1 = rec_catch[rec_catch.NZREACH == nzreach1]
#r1 = rec_rivers[rec_rivers.NZREACH == nzreach1]
#r2 = rec_streams[rec_streams.NZREACH == nzreach1]























