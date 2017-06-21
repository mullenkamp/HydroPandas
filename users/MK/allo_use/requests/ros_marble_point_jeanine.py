# -*- coding: utf-8 -*-
"""
Created on Tue May 09 14:23:36 2017

@author: MichaelEK
"""

from core.allo_use import crc_band_flow
from pandas import read_csv, merge

#########################################
#### Parameters

site = 64602
allo_csv = r'E:\ecan\shared\base_data\usage\allo.csv'

ros_export = r'E:\ecan\local\Projects\requests\jeanine\2017-05-09\64602_min_flow.csv'
allo_ros_export = r'E:\ecan\local\Projects\requests\jeanine\2017-05-09\64602_min_flow_allo_wap.csv'

#######################################
#### Extract ros data

ros1 = crc_band_flow([site])
ros1.loc[:, 'crc'] = ros1.loc[:, 'crc'].str.upper()

ros2 = ros1[['site', 'band', 'crc']].drop_duplicates().sort_values(['site', 'band', 'crc'])

#### Combine with allocation data
allo = read_csv(allo_csv)

allo_ros = merge(ros2, allo, how='left', on='crc')

#### Save data
ros1.to_csv(ros_export, index=False)
allo_ros.to_csv(allo_ros_export, index=False)






































































