# -*- coding: utf-8 -*-
"""
Created on Tue Oct 03 08:51:46 2017

@author: MichaelEK
"""
import os
import pandas as pd
import numpy as np
from hydropandas.io.tools.hilltop import rd_hilltop_sites, rd_ht_quan_data, rd_ht_wq_data
from hydropandas.io.tools.mssql import to_mssql, create_mssql_table, del_mssql_table_rows, rd_sql

#############################################
### Parameters
## Import
hts_gw = r'\\hilltop01\Hilltop\Data\WQGroundwater.hts'
hts_sw = r'\\hilltop01\Hilltop\Data\WQSurfacewater.hts'

#sample_params_dict = {'ProjectNumber': 'Project', 'CostCode': 'Cost Code', 'SampledBy': 'Technician', 'SampleComment': 'Sample Comment', 'FieldComment': 'Field Comment'}
sample_params_list = ['Project', 'Cost Code', 'Technician', 'Sample ID', 'Sample Comment', 'Field Comment', 'Sample Appearance', 'Sample Colour', 'Sample Odour', 'Water Colour', 'Water Clarity']
#mtype_params_dict = {'LabMethod': 'Lab Method', 'LabName': 'Lab Name'}
mtype_params_list = ['Lab Method', 'Lab Name']

max_date_stmt = "IF OBJECT_ID('HilltopWQSites', 'U') IS NOT NULL select max(ToDate) from HilltopWQSites else select 0"

## Export

sites_dtype = {'SiteID': 'VARCHAR(19)', 'MeasurementType': 'VARCHAR(59)', 'Units': 'VARCHAR(29)', 'FromDate': 'DATETIME', 'ToDate': 'DATETIME'}

samples_dtype = {'SiteID': 'VARCHAR(19)', 'Param': 'VARCHAR(59)', 'Value': 'VARCHAR(255)', 'CollectionTime': 'DATETIME'}

mtypes_dtype = {'SiteID': 'VARCHAR(19)', 'MeasurementType': 'VARCHAR(59)', 'CollectionTime': 'DATETIME', 'Param': 'VARCHAR(59)', 'Value': 'VARCHAR(255)'}

col_name_convert = {'site': 'SiteID', 'mtype': 'MeasurementType', 'unit': 'Units', 'start_date': 'FromDate', 'end_date': 'ToDate', 'time': 'CollectionTime'}

server = 'SQL2012DEV01'
database = 'Hydro'
sites_table = 'HilltopWQSites'
mtypes_table = 'HilltopWQMtypes'
samples_table = 'HilltopWQSamples'

sites_pkeys = ['SiteID', 'MeasurementType']
samples_pkeys = ['SiteID', 'Param', 'CollectionTime']
mtypes_pkeys = ['SiteID', 'MeasurementType', 'Param', 'CollectionTime']

base_dir = r'\\fs02\ManagedShares2\Data\Surface Water\shared\base_data'

gw_wq_sites_h5 = 'gw_wq_sites.h5'
gw_wq_samples_h5 = 'gw_wq_samples.h5'
gw_wq_mtypes_h5 = 'gw_wq_mtypes.h5'

sw_wq_sites_h5 = 'sw_wq_sites.h5'
sw_wq_samples_h5 = 'sw_wq_samples.h5'
sw_wq_mtypes_h5 = 'sw_wq_mtypes.h5'

##############################################
### Determine lastest date if sites table exists
max_date1 = rd_sql(server, database, stmt=max_date_stmt)

max_date = max_date1.loc[0][0]

if not isinstance(max_date, pd.Timestamp):
    raise ValueError('No sites table!')

###############################################
### Extract data

mtype_list = list(mtype_params_list)
mtype_list.append('data')

## GW
gw_wq_data, gw_sites_info = rd_ht_wq_data(hts_gw, start=str(max_date.date()), output_site_data=True, sample_params=sample_params_list, mtype_params=mtype_params_list)

gw_sites_info1 = gw_sites_info.drop(['data_source', 'divisor'], axis=1)

gw_wq_data1 = gw_wq_data.melt(['site', 'mtype', 'time'], var_name='Param', value_name='Value')

gw_wq_data_mtype = gw_wq_data1[gw_wq_data1['Param'].isin(mtype_list)]

gw_wq_data_sample = gw_wq_data1[gw_wq_data1['Param'].isin(sample_params_list)]
gw_wq_data_sample = gw_wq_data_sample.drop('mtype', axis=1).drop_duplicates(['site', 'Param', 'time'])
gw_wq_data_sample['Value'] = gw_wq_data_sample['Value'].str.strip()
gw_wq_data_sample = gw_wq_data_sample[~((gw_wq_data_sample['Value'] == '') | (gw_wq_data_sample['Value'] == '..'))]

# Rename columns
gw_sites_info1.rename(columns=col_name_convert, inplace=True)
gw_wq_data_sample.rename(columns=col_name_convert, inplace=True)
gw_wq_data_mtype.rename(columns=col_name_convert, inplace=True)

# Remove duplicate primary keys
gw_sites_info2 = gw_sites_info1.drop_duplicates(sites_pkeys).copy()
gw_wq_data_sample2 = gw_wq_data_sample.drop_duplicates(samples_pkeys).copy()
gw_wq_data_mtype2 = gw_wq_data_mtype.drop_duplicates(mtypes_pkeys).copy()

## SW
sw_wq_data, sw_sites_info = rd_ht_wq_data(hts_sw, output_site_data=True, sample_params=sample_params_list, mtype_params=mtype_params_list)
sw_sites_info1 = sw_sites_info.drop(['data_source', 'divisor'], axis=1)

sw_wq_data1 = sw_wq_data.melt(['site', 'mtype', 'time'], var_name='Param', value_name='Value')
sw_wq_data_mtype = sw_wq_data1[sw_wq_data1['Param'].isin(mtype_list)]

sw_wq_data_sample = sw_wq_data1[sw_wq_data1['Param'].isin(sample_params_list)]
sw_wq_data_sample = sw_wq_data_sample.drop('mtype', axis=1).drop_duplicates(['site', 'Param', 'time'])
sw_wq_data_sample['Value'] = sw_wq_data_sample['Value'].str.strip()
sw_wq_data_sample = sw_wq_data_sample[~((sw_wq_data_sample['Value'] == '') | (sw_wq_data_sample['Value'] == '..'))]

# Rename columns
sw_sites_info1.rename(columns=col_name_convert, inplace=True)
sw_wq_data_sample.rename(columns=col_name_convert, inplace=True)
sw_wq_data_mtype.rename(columns=col_name_convert, inplace=True)

# Remove duplicate primary keys
sw_sites_info2 = sw_sites_info1.drop_duplicates(sites_pkeys).copy()
sw_wq_data_sample2 = sw_wq_data_sample.drop_duplicates(samples_pkeys).copy()
sw_wq_data_mtype2 = sw_wq_data_mtype.drop_duplicates(mtypes_pkeys).copy()




###############################################
### Dump on sql database

## GW
save_df(gw_sites_info2, path.join(base_dir, gw_wq_sites_h5))
save_df(gw_wq_data_sample2, path.join(base_dir, gw_wq_samples_h5))
save_df(gw_wq_data_mtype2, path.join(base_dir, gw_wq_mtypes_h5))

## SW
save_df(sw_sites_info2, path.join(base_dir, sw_wq_sites_h5))
save_df(sw_wq_data_sample2, path.join(base_dir, sw_wq_samples_h5))
save_df(sw_wq_data_mtype2, path.join(base_dir, sw_wq_mtypes_h5))

## All

# Combine

# Save


##############################################
### Testing

gw_sites_info2 = pd.read_hdf(os.path.join(base_dir, gw_wq_sites_h5))
gw_wq_data_sample2 = pd.read_hdf(os.path.join(base_dir, gw_wq_samples_h5))
gw_wq_data_mtype2 = pd.read_hdf(os.path.join(base_dir, gw_wq_mtypes_h5))

sw_sites_info2 = pd.read_hdf(os.path.join(base_dir, sw_wq_sites_h5))
sw_wq_data_sample2 = pd.read_hdf(os.path.join(base_dir, sw_wq_samples_h5))
sw_wq_data_mtype2 = pd.read_hdf(os.path.join(base_dir, sw_wq_mtypes_h5))

sites_info2 = pd.concat([gw_sites_info2, sw_sites_info2])
wq_data_sample2 = pd.concat([gw_wq_data_sample2, sw_wq_data_sample2])
wq_data_mtype2 = pd.concat([gw_wq_data_mtype2, sw_wq_data_mtype2])

# Remove duplicate primary keys
sites_info2 = sites_info2.drop_duplicates(sites_pkeys).dropna(subset=sites_pkeys)
wq_data_sample2 = wq_data_sample2.drop_duplicates(samples_pkeys).dropna(subset=samples_pkeys)
wq_data_mtype2 = wq_data_mtype2.drop_duplicates(mtypes_pkeys).dropna(subset=mtypes_pkeys)

wq_data_sample2['CollectionTime'] = pd.to_datetime(wq_data_sample2['CollectionTime'])
wq_data_mtype2['CollectionTime'] = pd.to_datetime(wq_data_mtype2['CollectionTime'])

create_mssql_table(server, database, sites_table, dtype_dict=sites_dtype, primary_keys=sites_pkeys)
to_mssql(sites_info2, server, database, sites_table)

create_mssql_table(server, database, samples_table, dtype_dict=samples_dtype, primary_keys=samples_pkeys)
to_mssql(wq_data_sample2, server, database, samples_table)

create_mssql_table(server, database, mtypes_table, dtype_dict=mtypes_dtype, primary_keys=mtypes_pkeys)
to_mssql(wq_data_mtype2, server, database, mtypes_table)





























