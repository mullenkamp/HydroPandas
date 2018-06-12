# -*- coding: utf-8 -*-
"""
Created on Tue May 22 09:03:41 2018

@author: MichaelEK
"""

import pandas as pd
from pdsql import mssql
import os
from pyhydllp import hyd, sql


### Parameters

hyd_server = 'sql2012prod03'
hyd_database = 'hydstra'
server = 'sql2012dev01'
database = 'hydro'
ts_daily_table = 'TSDataNumericDaily'
ts_hourly_table = 'TSDataNumericHourly'
ts_summ_table = 'TSDataNumericDailySumm'
lf_table = 'LowFlowRestrSite'
sites_table = 'ExternalSite'

lf_date = '2018-05-21'
min_date = '2018-01-01'
min_count = 10

datasets = [5, 4]

## Hydstra
ini_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd'
dll_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd\sys\run'
username = ''
password = ''
hydllp_filename = 'hydllp.dll'
hyaccess_filename = 'Hyaccess.ini'
hyconfig_filename = 'HYCONFIG.INI'

## Export

export_dir = r'E:\ecan\local\Projects\requests\tony\2018-05-22'
export_summ1 = 'hydstra_summ_stats.csv'

################################
### Extract data

## Hydro

summ_data = mssql.rd_sql(server, database, ts_summ_table, where_col={'DatasetTypeID': datasets})

summ_data.rename(columns={'ExtSiteID': 'site'}, inplace=True)

wl_data = summ_data[(summ_data.DatasetTypeID == 4)].copy()
flow_data = summ_data[(summ_data.DatasetTypeID == 5) & (summ_data.ToDate >= min_date)].copy()
flow_data = summ_data[(summ_data.DatasetTypeID == 5)].copy()

mis_sites = wl_data[~wl_data.ExtSiteID.isin(flow_data.ExtSiteID.unique())].sort_values('ToDate', ascending=False)


## Hydstra

hyd1 = hyd(ini_path, dll_path, hydllp_filename=hydllp_filename,
           hyaccess_filename=hyaccess_filename, hyconfig_filename=hyconfig_filename,
           username=username, password=password)

var_periods = hyd1.sites_var_periods(hyd_server, hyd_database, 140)
var_periods1 = var_periods.rename(columns={'from_date': 'hyd_from_date', 'to_date': 'hyd_to_date'}).sort_values('hyd_to_date', ascending=False).copy()
var_periods1 = var_periods1.drop_duplicates('site')

var_periods_wl = hyd1.sites_var_periods(hyd_server, hyd_database, 100)
var_periods_wl1 = var_periods_wl.rename(columns={'from_date': 'hyd_from_date', 'to_date': 'hyd_to_date'}).sort_values('hyd_to_date', ascending=False).copy()

## Combine

hydro_hydstra = pd.merge(flow_data, var_periods1, on='site', how='outer')

mis_sites = hydro_hydstra[hydro_hydstra.DatasetTypeID.isnull()]

diff_time = (hydro_hydstra.hyd_to_date - hydro_hydstra.ToDate).dt.days

outdated = hydro_hydstra[diff_time > 7]

hydro_hydstra_wl = pd.merge(wl_data, var_periods_wl1, on='site', how='outer')

diff_time_wl = (hydro_hydstra_wl.hyd_to_date - hydro_hydstra_wl.ToDate).dt.days

outdated_wl = hydro_hydstra_wl[diff_time_wl > 7]


sites = ['1688218', '69514', '69615']

t1 = hyd1.get_ts_data(site)

bi1 = hyd1.get_ts_blockinfo(sites, variables=['100', '140'])

rc1 = sql.rating_changes(hyd_server, hyd_database, site, from_mod_date='2017-07-01')

var_periods = hyd1.sites_var_periods(hyd_server, hyd_database, 140)

vl1 = hyd1.get_variable_list(sites)


extract_sites = outdated[['site', 'varfrom', 'varto', 'hyd_from_date', 'hyd_to_date']].copy()
extract_sites.columns = ['site', 'varfrom', 'varto', 'from_date', 'to_date']

extract_sites.from_date = (extract_sites.from_date + pd.DateOffset(days=1)).dt.date
extract_sites.to_date = extract_sites.to_date.dt.date























