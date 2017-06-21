# -*- coding: utf-8 -*-
"""
Query script for reliability of supply results.
This is similar to the usage and allocation query script.
"""

from pandas import read_csv
from misc_functions_v01 import printf
from query_ROS_v01 import ros_query

########################################
### Parameters

ros_csv = 'C:/ecan/base_data/usage/low_flow_restr5_with_cav.csv'

export_path = 'C:/ecan/Projects/otop/usage/otop_ros_results1.csv'
cwms_zone = ['Orari-Opihi-Pareora']


########################################
### Read in data

ros = read_csv(ros_csv)

#######################################
### Query data

q1 = ros_query(ros, cwms_zone=cwms_zone, export_path=export_path)
