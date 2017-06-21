# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 12:12:28 2016

@author: MichaelEK
"""

from ts_stats_fun import flow_stats, malf7d
from import_hydstra_fun import rd_hydstra_csv

########################
### Parameters

flow_csv = 'C:/ecan/local/Projects/otop/flow/Halswell_flow.csv'

########################
### Import data

flow = rd_hydstra_csv(flow_csv)

########################
### Run stats

stats1 = flow_stats(flow)

malf, alf, alf_mis = malf7d(flow)




