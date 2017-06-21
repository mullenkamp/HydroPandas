# -*- coding: utf-8 -*-
"""
Script to update the water allocation estimates from the reliability of supply.
"""

from pandas import merge, read_csv

########################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results3_with_cav.csv'
ros_csv = 'C:/ecan/base_data/usage/low_flow_restr5_with_cav.csv'

ros_col = ['crc', 'wap', 'dates', 'ann_allo_m3', 'ann_allo_wqn_m3', 'usage_m3', 'band_num', 'band_restr_365', 'band_restr_212', 'up_allo_m3',  'up_allo_wqn_m3', 'gauge_num']

export_path = 'C:/ecan/base_data/usage/usage_takes_series_sw_up3_with_cav.csv'

########################################
### Read in data

series = read_csv(series_csv)
ros = read_csv(ros_csv)[ros_col]

#######################################
### Aggregate all data by crc and date

#series_grp1 = series.groupby(['crc', 'wap', 'dates'])
#series2 = series_grp1.sum().reset_index()
##series2.dates = to_datetime(series2.dates)
#
#ros_grp1 = ros.groupby(['crc', 'wap', 'dates'])
#ros_sum1 = ros_grp1[['ann_allo_m3', 'ann_allo_wqn_m3', 'usage_m3', 'up_allo_m3', 'up_allo_wqn_m3']].sum().reset_index()

### Merge ROS and prioir allo together and paste in old allo

use_ros1 = merge(series, ros[['crc', 'wap', 'dates', 'band_num', 'band_restr_365', 'band_restr_212', 'up_allo_m3', 'up_allo_wqn_m3', 'gauge_num']], on=['crc', 'wap', 'dates'], how='left')

use_ros1.loc[use_ros1.up_allo_m3.isnull(), 'up_allo_m3'] = use_ros1.loc[use_ros1.up_allo_m3.isnull(), 'ann_allo_m3']
use_ros1.loc[use_ros1.up_allo_wqn_m3.isnull(), 'up_allo_wqn_m3'] = use_ros1.loc[use_ros1.up_allo_wqn_m3.isnull(), 'ann_allo_wqn_m3']

use_ros1.loc[:, 'up_allo_m3'] = use_ros1.loc[:, 'up_allo_m3'].round(2)
use_ros1.loc[:, 'up_allo_wqn_m3'] = use_ros1.loc[:, 'up_allo_wqn_m3'].round(2)

### Save data

use_ros1.to_csv(export_path, index=False)
