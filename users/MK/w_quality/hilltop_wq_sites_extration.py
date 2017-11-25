# -*- coding: utf-8 -*-
"""
Created on Tue Oct 03 08:51:46 2017

@author: MichaelEK
"""

from core.ecan_io.hilltop import rd_hilltop_sites, rd_ht_quan_data, rd_ht_wq_data
from pandas import Series
from core.ecan_io import write_sql
from core.misc import save_df


#############################################
### Parameters

sites_dtype = {'SiteID': 'VARCHAR(19)', 'MeasurementType': 'VARCHAR(59)', 'Units': 'VARCHAR(19)', 'FromDate': 'DATETIME', 'ToDate': 'DATETIME'}

samples_dtype = {'SiteID': 'VARCHAR(19)', 'Param': 'VARCHAR(59)', 'Value': 'VARCHAR(255)', 'CollectionTime': 'DATETIME'}

mtypes_dtype = {'SiteID': 'VARCHAR(19)', 'MeasurementType': 'VARCHAR(59)', 'Param': 'VARCHAR(59)', 'Value': 'VARCHAR(255)', 'CollectionTime': 'DATETIME'}

#sample_params_dict = {'ProjectNumber': 'Project', 'CostCode': 'Cost Code', 'SampledBy': 'Technician', 'SampleComment': 'Sample Comment', 'FieldComment': 'Field Comment'}
sample_params_list = ['Project', 'Cost Code', 'Technician', 'Sample Comment', 'Field Comment']
#mtype_params_dict = {'LabMethod': 'Lab Method', 'LabName': 'Lab Name'}
mtype_params_list = ['Lab Method', 'Lab Name']

col_name_convert = {'site': 'SiteID', 'mtype': 'MeasurementType', 'unit': 'Units', 'start_date': 'FromDate', 'end_date': 'ToDate', 'time': 'CollectionTime'}

server = 'SQL2012DEV01'
database = 'Hydro'
sites_table = 'WQ_sites'
mtypes_table = 'WQ_mtypes'
samples_table = 'WQ_samples'

sites_keys = ['SiteID', 'MeasurementType']
samples_keys = ['SiteID', 'Param', 'CollectionTime']
mtypes_keys = ['SiteID', 'MeasurementType', 'Param', 'CollectionTime']

hts_gw = r'\\hilltop01\Hilltop\Data\WQGroundwater.hts'
hts_sw = r'\\hilltop01\Hilltop\Data\WQSurfacewater.hts'

gw_wq_sites_h5 = r'E:\ecan\shared\projects\end_of_squalarc\gw_wq_sites.h5'
gw_wq_samples_h5 = r'E:\ecan\shared\projects\end_of_squalarc\gw_wq_samples.h5'
gw_wq_mtypes_h5 = r'E:\ecan\shared\projects\end_of_squalarc\gw_wq_mtypes.h5'
#sites = ['SQ36287']

###############################################
### Extract data

gw_wq_data, gw_sites_info = rd_ht_wq_data(hts_gw, output_site_data=True, sample_params=sample_params_list, mtype_params=mtype_params_list)
sw_wq_data, sw_sites_info = rd_ht_wq_data(hts_sw, output_site_data=True, sample_params=sample_params_list, mtype_params=mtype_params_list)

gw_sites_info1 = gw_sites_info.drop(['data_source', 'divisor'], axis=1)
sw_sites_info1 = sw_sites_info.drop(['data_source', 'divisor'], axis=1)

gw_wq_data1 = gw_wq_data.melt(['site', 'mtype', 'time'], var_name='Param', value_name='Value')

mtype_list = list(mtype_params_list)
mtype_list.append('data')
gw_wq_data_mtype = gw_wq_data1[gw_wq_data1['Param'].isin(mtype_list)]

gw_wq_data_sample = gw_wq_data1[gw_wq_data1['Param'].isin(sample_params_list)]
gw_wq_data_sample = gw_wq_data_sample.drop('mtype', axis=1).drop_duplicates(['site', 'Param', 'time'])
gw_wq_data_sample['Value'] = gw_wq_data_sample['Value'].str.strip()
gw_wq_data_sample = gw_wq_data_sample[~((gw_wq_data_sample['Value'] == '') | (gw_wq_data_sample['Value'] == '..'))]

### Remove duplicate primary keys
gw_sites_info2 = gw_sites_info1.drop_duplicates(sites_keys).copy()
gw_wq_data_sample2 = gw_wq_data_sample.drop_duplicates(samples_keys).copy()
gw_wq_data_mtype2 = gw_wq_data_mtype.drop_duplicates(mtypes_keys).copy()

### Rename columns
gw_sites_info2.rename(columns=col_name_convert, inplace=True)
gw_wq_data_sample2.rename(columns=col_name_convert, inplace=True)
gw_wq_data_mtype2.rename(columns=col_name_convert, inplace=True)

###############################################
### Dump on sql database

save_df(gw_sites_info2, gw_wq_sites_h5)
save_df(gw_wq_data_sample2, gw_wq_samples_h5)
save_df(gw_wq_data_mtype2, gw_wq_mtypes_h5)

write_sql(gw_sites_info2, server, database, sites_table, sites_dtype, primary_keys=sites_keys, drop_table=True)
write_sql(gw_wq_data_sample2[['SiteID', 'Param', 'CollectionTime', 'Value']], server, database, samples_table, samples_dtype, primary_keys=samples_keys, drop_table=True)
write_sql(gw_wq_data_mtype2[['SiteID', 'MeasurementType', 'Param', 'CollectionTime', 'Value']], server, database, mtypes_table, mtypes_dtype, primary_keys=mtypes_keys, drop_table=True)

##############################################
### Testing

gw_sites_info = rd_hilltop_sites(hts_gw)
param2 = Series(sites1.mtype.unique())


param2[param2.str.contains('nitrogen', case=False, na=False)].sort_values()

param2[param2.str.contains('nitrate', case=False, na=False)].sort_values()
param2[param2.str.contains('nitrite', case=False, na=False)].sort_values()

sites_df = sites1.copy()

df6, sites1 = rd_ht_wq_data(hts_gw, sites=['M36/1160'], output_site_data=True)


t1.to_json(r'E:\ecan\shared\projects\end_of_squalarc\wq_data.json', orient='records', date_format='iso')

hts = r'\\hilltop01\Hilltop\Data\WQGroundwater.hts'
sites = ['M36/1160']

sample_params = ['Technician', 'Project']
mtype_params = ['Lab Method', 'Lab Name']


sample_params_dict = {'ProjectNumber': 'Project', 'CostCode': 'Cost Code', 'SampledBy': 'Technician', 'SampleComment': 'Sample Comment', 'FieldComment': 'Field Comment'}
mtype_params_dict = {'LabMethod': 'Lab Method', 'LabName': 'Lab Name'}


sites = ['M36/1160']

gw_wq_data, gw_sites_info = rd_ht_wq_data(hts_gw, sites, output_site_data=True, sample_params=sample_params_dict.values(), mtype_params=mtype_params_dict.values())


site = 'BU24/0002'


















































