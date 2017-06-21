# -*- coding: utf-8 -*-
"""
Created on Fri Oct 14 11:47:59 2016

@author: MichaelEK
"""

#### Import packages

from os import path
from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric, ExcelWriter
from misc_fun import printf
from import_fun import rd_hydstra_csv, rd_ts, rd_sql, rd_henry, rd_hydrotel, rd_vcn, rd_hydstra_db
from ts_stats_fun import w_resample, ts_comp, flow_dur, gauge_proc, fre_accrual, malf7d, flow_stats, tsreg
from spatial_fun import sel_sites_poly
from hydro_plot_fun import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot
from geopandas import read_file
from allo_use_fun import stream_nat, flow_ros, ros_freq

######################################
#### Define parameters

### Input
base_path = 'C:\\ecan\\shared\\projects\\otop\\scenarios\\current_pathways\\data'
flow_csv = 'C:/ecan/shared/base_data/flow/all_flow_data.csv'

sites = [70105, 70103, 69645, 69643, 69644, 69602, 69621, 69642, 69635, 69614, 69661, 69615, 69616, 69619, 69607, 69650, 69618, 69520, 69519, 69516, 69514, 69508, 69505]
max_date = '2016-06-30'


## Databases


## Output
csv_file = 'results_shirley_v02.csv'


####################################
#### Load data

### Flow
## processed flow data
flow = rd_ts(flow_csv)
flow.columns = flow.columns.astype(int)

otop = flow[sites][:max_date]


##################################
#### Run stats

stats1 = flow_stats(otop, below_median=True)

malf, a, b = malf7d(otop, num_years=True)

fre3a = fre_accrual(otop)
#fre3b = fre_accrual(otop, day_gap=1)


##################################
#### Combine all results and export

results = concat([stats1, malf, fre3a], axis=1)

results.to_csv(path.join(base_path, csv_file))









