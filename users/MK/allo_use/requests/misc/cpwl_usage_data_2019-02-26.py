# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql
import geopandas as gpd
import gistools.vector as vec
from allotools import AlloUsage

pd.options.display.max_columns = 10

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'
sites_table = 'ExternalSite'
crc_wap_table = 'CrcWapAllo'
crc_table = 'CrcAllo'

#sites = ['J36/0016', 'K37/3262', '69302']
datasets = [12]
#cwms = ['kaikoura']
catch_group = ['Selwyn River']
#cwms = ['Selwyn - Waihora']
#rdr_site = 'J36/0016-M1'

s1_shp = r'E:\ecan\shared\GIS_base\vector\cpw\S1.shp'
sheff_shp = r'E:\ecan\shared\GIS_base\vector\cpw\Sheffield.shp'

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WaterUse.hts'

from_date = '2012-07-01'
to_date = '2018-06-30'

years = {'2015-01-01': '2015-06-30', '2016-01-01': '2016-06-30', '2017-01-01': '2017-06-30', '2018-01-01': '2018-06-30'}

freq = 'A-JUN'

groupby = ['crc', 'wap', 'date']

#rdr_hts = [r'H:\Data\Annual\ending_2016\ending_2016.dsn', r'H:\Data\Annual\ending_2017\ending_2017.dsn', r'H:\Data\Annual\ending_2018\ending_20128.dsn']
#
#hts_dsn = r'H:\Data\WaterUSeAll.dsn'

export_path = r'E:\ecan\local\Projects\requests\cpwl\2019-01-31'
export1 = 'cpwl_crc_usage_daily_2019-02-26.csv'
#export2 = 'cpwl_allo_usage_summary_2019-01-31.csv'
#export3 = 'cpwl_crc_wap_usage_data_2019-01-31.csv'

############################################
### Extract data

cpw_s1 = gpd.read_file(s1_shp)
cpw_sheff = gpd.read_file(sheff_shp)
cpw = pd.concat([cpw_s1, cpw_sheff])


## Pull out recorder data
#sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID'], where_col={'CatchmentGroupName': catch_group}).ExtSiteID.tolist()

#sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID'], where_col={'CwmsName': cwms}).ExtSiteID.tolist()

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY'])
sites2 = sites1[sites1.ExtSiteID.str.contains('[A-Z]+\d+/\d+')].copy()
sites3 = vec.xy_to_gpd('ExtSiteID', 'NZTMX', 'NZTMY', sites2)

sites4 = vec.sel_sites_poly(sites3, cpw)

sites5 = sites4.ExtSiteID.unique().tolist()

a3 = AlloUsage(from_date, to_date, crc_wap_filter={'wap': sites5}, in_allo=False)

combo2 = a3.get_ts(['allo', 'usage'], freq, groupby)[['total_allo', 'total_usage']].round()

combo2.to_csv(os.path.join(export_path, export1))

#combo2.loc['CRC962531.3']
























