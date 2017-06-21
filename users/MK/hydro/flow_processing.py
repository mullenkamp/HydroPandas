# -*- coding: utf-8 -*-
"""
Script to import and process Hydstra flow data and export it as a common csv file. All available data in Hydstra is processed.
"""

from pandas import merge, read_csv, DataFrame, concat
from import_fun import rd_hydstra_csv, rd_hydstra_dir, rd_henry
from ts_stats_fun import flow_stats, malf7d, ts_comp, fre_accrual
from misc_fun import printf
from hydro_plot_fun import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot

#################################
### Parameters

## import parameters
flow_dir = 'C:/ecan/shared/base_data/flow/RAW/proc_set'

min_days = 365

end = '2019-06-30'

lps_sites = [63501, 64610, 67602, 66405]    # niwa sites in l/s

## output parameters
export_path = 'C:/ecan/shared/base_data/flow/all_flow_data.csv'
export_sites_path = 'C:/ecan/shared/base_data/flow/all_flow_sites_stats.csv'

#################################
### Read in data

flow = rd_hydstra_dir(flow_dir)


################################
### Process data

## Remove sites without any data
flow1 = flow[:end].dropna(axis=1, how='all')

## Remove site with less than threshold of days
min_days_index = flow1.count() > min_days
flow2 = flow1.loc[:, min_days_index]

## Convert l/s to m3/s at some (NIWA) sites
flow2.loc[:, lps_sites] = (flow2.loc[:, lps_sites] * 0.001).round(4)

## Check
stats = flow_stats(flow2)
stats.index.name = 'sites'

malf, a, b = malf7d(flow2)

stats2 = concat([stats, malf.transpose().MALF7D], axis=1)

###############################
### Save data

flow2.index.name = 'date'

flow2.to_csv(export_path)

#sites = flow2.columns.to_series()

stats2.to_csv(export_sites_path)


