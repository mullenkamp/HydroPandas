# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 14:37:56 2017

@author: michaelek
"""

from pandas import read_csv, merge
from core.ecan_io import rd_sql

############################################
#### Parameters

crc_csv = r'C:\ecan\local\Projects\requests\ross\2017-02-23\Mackenzie_irrigation_consents.csv'
crc_waps_csv = 'S:/Surface Water/shared/base_data/usage/crc_waps.csv'
wu_waps_csv = r'C:\ecan\hilltop\xml_test\usable_sites.csv'

export_path1 = r'C:\ecan\local\Projects\requests\ross\2017-02-23\crc_with_usage.csv'
export_path2 = r'C:\ecan\local\Projects\requests\ross\2017-02-23\crc_wap.csv'

############################################
#### Read in data

crc = read_csv(crc_csv)
crc_wap = rd_sql('SQL2012PROD03', 'DataWarehouse', 'D_ACC_Act_Water_TakeWaterWAPAlloc', ['RecordNo', 'WAP'])
crc_wap.columns = ['crc', 'wap']
crc_wap.loc[:, 'wap'] = crc_wap.wap.str.replace(' ', '')
crc_wap.loc[:, 'wap'] = crc_wap.wap.str.upper()
#crc_wap = read_csv(crc_waps_csv)
wu = read_csv(wu_waps_csv)

###########################################
#### Merge and filter

waps = crc_wap.loc[crc_wap.crc.isin(crc.crc.values)].sort_values('crc')
waps1 = waps.drop_duplicates()
waps1.columns = ['crc', 'wap_name']

set1 = merge(wu, waps1, on='wap_name').sort_values('wap_name')

### Export
set1.to_csv(export_path1, index=False)
waps1.to_csv(export_path2, index=False)











###########################################
#### Testing

w1 = 'I38/0056'
wu[wu.wap_name == w1]

crc_wap[crc_wap.wap == w1]


crc_wap[crc_wap.wap == w1]


crc_wap = rd_sql(code='crc_wap_act_acc')











































