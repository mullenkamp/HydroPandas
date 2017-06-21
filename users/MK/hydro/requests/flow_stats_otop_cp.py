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
flow_csv = 'S:/Surface Water/shared/base_data/flow/all_flow_data.csv'

opihi_csv = 'opihi_data.csv'
orari_cal_csv = 'orari_cal_data.csv'
orari_mod_csv = 'orari_mod_data.csv'

other_sites = [69505, 70105]

## Databases


## Output
excel_file = 'results.xlsx'
df_names = ['current', 'cal', 'mod']

stats_export = 'stats'
ros_means_export = 'ros_means'
ros_partial_export = 'ros_partial'
ros_full_export = 'ros_full'
ros_partial_norm_export = 'ros_partial_norm'
ros_full_norm_export = 'ros_full_norm'


####################################
#### Basics with flow data

### Flow
## processed flow data
opihi_mod = tsreg(rd_ts(path.join(base_path, opihi_csv)))
orari_cal = tsreg(rd_ts(path.join(base_path, orari_cal_csv)))
orari_mod = tsreg(rd_ts(path.join(base_path, orari_mod_csv)))

cal = orari_cal
cal.columns = cal.columns.astype(int)

mod = concat([opihi_mod, orari_mod], axis=1)
mod.columns = mod.columns.astype(int)

## Load in from Hydstra
all_sites = []
all_sites.extend(other_sites)
all_sites.extend(mod.columns.tolist())

#flow1 = rd_hydstra_db(all_sites)
#flow1b = flow1[flow2.index]
flow1a = rd_ts(flow_csv)
flow1a.columns = flow1a.columns.astype(int)
current = flow1a.loc[mod.index, all_sites]

#### Run loop for all three sets
sets = [current, cal, mod]
excel = ExcelWriter(path.join(base_path, excel_file))


for i in range(len(sets)):
    flow = sets[i]

    ## Run stats for Huts and SH1 on the Pareora

    stats1 = flow_stats(flow)
    malf, alf, alf_mising = malf7d(flow)

    #fdc, fdc = flow_dur(flow)

    stats2 = concat([stats1, malf[['MALF7D 10 yrs', 'MALF7D 20 yrs', 'MALF7D all yrs']]], axis=1)

    #######################################
    #### Reliability of supply

    ros = flow_ros(flow.columns.values, flow_csv=flow)
    ros_partial, ros_full = ros_freq(ros, period='summer')
    ros_partial_norm, ros_full_norm = ros_freq(ros, period='summer', norm=True)

    ros_partial_mean = ros_partial.mean().round(1)
    ros_full_mean = ros_full.mean().round(1)
    ros_partial_norm_mean = ros_partial_norm.mean().round(3)
    ros_full_norm_mean = ros_full_norm.mean().round(3)

    ros_means = concat([ros_partial_mean, ros_partial_norm_mean, ros_full_mean, ros_full_norm_mean], axis=1)
    ros_means.columns = ['ros_partial', 'ros_partial_norm', 'ros_full', 'ros_full_norm']

    ######################################
    #### Export results
    out1 = df_names[i]

    stats2.to_excel(excel, out1 + '_' + stats_export)
    ros_means.to_excel(excel, out1 + '_' + ros_means_export)

    ros_partial.to_excel(excel, out1 + '_' + ros_partial_export)
    ros_full.to_excel(excel, out1 + '_' + ros_full_export)
    ros_partial_norm.to_excel(excel, out1 + '_' + ros_partial_norm_export)
    ros_full_norm.to_excel(excel, out1 + '_' + ros_full_norm_export)



#    stats2.to_csv(path.join(base_path, out1 + '_' + stats_export))
#    ros_means.to_csv(path.join(base_path, out1 + '_' + ros_means_export))
#
#    ros_partial.to_csv(path.join(base_path, out1 + '_' + ros_partial_export))
#    ros_full.to_csv(path.join(base_path, out1 + '_' + ros_full_export))
#    ros_partial_norm.to_csv(path.join(base_path, out1 + '_' + ros_partial_norm_export))
#    ros_full_norm.to_csv(path.join(base_path, out1 + '_' + ros_full_norm_export))
#

excel.save()











