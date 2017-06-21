# -*- coding: utf-8 -*-
"""
Created on Wed Nov 09 09:39:05 2016

@author: michaelek
"""

from allo_use_fun import flow_ros, ros_freq
from os import path
from pandas import concat

##############################
#### Parameters

start_stats = '2000'

periods = ['water year', 'summer']
export_path = 'C:/ecan/shared/projects/otop/min_flow/ros'

###########################
#### Run ROS

ros = flow_ros()

for i in periods:
    partial, full = ros_freq(ros, period=i)
    partial_sum = partial.mean()
    full_sum = full.mean()
    sum1 = concat([partial_sum, full_sum], axis=1).round(1)
    sum1.columns = ['partial', 'full']

    partial.to_csv(path.join(export_path, 'partial_' + i + '.csv'))
    full.to_csv(path.join(export_path, 'full_' + i + '.csv'))
    sum1.to_csv(path.join(export_path, 'mean_days_' + i + '.csv'))







































