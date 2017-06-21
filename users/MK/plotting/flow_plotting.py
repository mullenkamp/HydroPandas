# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
import numpy as np
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame, concat
from gaugings_fun import rd_henry
from import_hydstra_fun import rd_hydstra_csv
from ts_stats_fun import rd_ts, flow_stats, malf7d, ts_comp, fre_accrual
from misc_fun import printf
from hydro_plot_fun import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot

#################################
### Parameters

#plt.ioff()

## import parameters
flow_csv = 'C:/ecan/shared/base_data/flow/otop/otop_flow_recorders_v02.csv'
nat_flow_csv = 'C:/ecan/shared/base_data/flow/otop/rec_flow_nat.csv'
salt_wash_csv = 'C:/ecan/shared/base_data/flow/otop/salt_wash_wl.csv'

gauge_num = [46, 66, 92, 2034, 2150, 169535, 1696288, 1696290, 1696294, 1696295, 1695131, 1696318, 2078]
nat_gauge_csv = 'C:/ecan/shared/base_data/flow/otop/gauge_flow_nat.csv'

## Other parameters
par_lst = [70105, 70103]
tengawai_lst = [69621, 69635, 69607]
opihi_lst = [69618, 69607]
opuha_lst = [69619, 69661, 69607]
orari_lst = [69505, 69514]
temuka_lst = [69644, 69602]

opihi_catch_lst = [69602, 69607, 69614, 69615, 69618, 69619, 69621, 69633, 69634, 69635, 69642, 69643, 69644, 69645, 69648, 69661]

river_lst = [par_lst, tengawai_lst, opihi_lst, opuha_lst, orari_lst, temuka_lst]
river_names = ['Pareora', 'Tengawai', 'Opihi', 'Opuha', 'Orari', 'Temuka']

start = '1990'
end = '2015-06-30'
start2 = '2003-07-01'

## output parameters
export_path = 'C:/ecan/shared/projects/otop/reports/current_state_2016/figures/flow'
export_name_flow = 'flow_dual_boxplot.png'
export_name_flow_2004 = 'flow_dual_boxplot_2004-2015.png'
export_name_multi = 'flow_multi_barplot.png'
export_name_alf = 'alf_multi_barplot.png'
export_name_reg1 = 'pareora_reg_flow_all.png'
export_name_reg2 = 'pareora_reg_flow_sel.png'

#################################
### Read in data

flow = rd_hydstra_csv(flow_csv)
nat_flow = rd_ts(nat_flow_csv)
nat_flow.columns = nat_flow.columns.astype('int')
nat_flow = nat_flow[flow.columns]

salt_wash = rd_hydstra_csv(salt_wash_csv)

#gauge = rd_henry(sites=gauge_num, agg_day=True, sites_by_col=True)
#nat_gauge = rd_ts(nat_gauge_csv)

################################
### Assess data

stats = flow_stats(flow[:end])
yr_index = stats.loc['Tot data yrs', :] >= 5
stats.loc['Mean', yr_index]

flow_all = concat([flow, nat_flow], axis=1, join='inner', keys=['recorded', 'naturalised'])

malf, alf, alf_mis = malf7d(flow_all['recorded'])
alf_mis[69607]
malf.loc[:, malf.loc['MALF7D', :].notnull()]

malf, alf, alf_mis = malf7d(flow_all['naturalised'])
alf_mis[69607]
malf.loc[:, malf.loc['MALF7D', :].notnull()]

salt_wash_stats = flow_stats(salt_wash[:end])


################################
### Plots

#mon_boxplot(flow, export_path=export_path, export_name=export_name_flow)
dual_mon_boxplot(flow, nat_flow, export_path=export_path, export_name=export_name_flow)
dual_mon_boxplot(flow[start2:end], nat_flow[start2:end], export_path=export_path, export_name=export_name_flow_2004)

for i in range(len(river_names)):
    name = river_names[i]
    river_sites = river_lst[i]

    multi_yr_barplot(flow, nat_flow, col=river_sites, start=start, export_path=export_path, export_name=name + '_' + export_name_multi)

for i in range(len(river_names)):
    name = river_names[i]
    river_sites = river_lst[i]

    multi_yr_barplot(flow, nat_flow, alf=True, col=river_sites, start=start, export_path=export_path, export_name=name + '_' + export_name_alf)

## Regressions
reg_all = reg_plot(flow[par_lst[0]], flow[par_lst[1]], export_path=export_path, export_name=export_name_reg1)
reg_sub = reg_plot(flow[par_lst[0]], flow[par_lst[1]], export_path=export_path, export_name=export_name_reg2, x_max=10, y_max=10)

#################################
### Testing

par_flow = flow[par_lst][:end]
par_nat_flow = nat_flow[par_lst][:end]

flow_stats(par_flow)
flow_stats(par_nat_flow)

malf, alf, alf_mis = malf7d(par_flow)
malf
malf, alf, alf_mis = malf7d(par_nat_flow)
malf

fre_accrual(par_flow)
fre_accrual(par_nat_flow)

x = flow[par_lst[0]]
#y = flow[par_lst[1]]

ts_comp(par_flow, end='2015')
ts_comp(par_nat_flow, end='2015')

y = par_flow[70103]
y2 = par_nat_flow[70103]
x = par_flow[70105]

y[y == 0].resample('A-JUN').count()
y2[y2 == 0].resample('A-JUN').count()
x[x == 0].resample('A-JUN').count()

opihi_flow = flow[opihi_catch_lst][:end]
opihi_nat_flow = nat_flow[opihi_catch_lst][:end]

opihi_stats = flow_stats(opihi_flow)
opihi_nat_stats = flow_stats(opihi_nat_flow)

malf1, alf1, alf_mis1 = malf7d(opihi_flow)
malf1
malf2, alf2, alf_mis2 = malf7d(opihi_nat_flow)
malf2

fre3_opihi1 = fre_accrual(opihi_flow)
fre3_opihi2 = fre_accrual(opihi_nat_flow)

malf2_trans = malf2.transpose()
opihi_nat_stats_trans = opihi_nat_stats.transpose()

opihi_all = concat([opihi_nat_stats_trans, malf2_trans, fre3_opihi2], axis=1)

csv_path = 'C:/ecan/shared/projects/otop/reports/current_state_2016/opihi_nat_stats.csv'

opihi_all.to_csv(csv_path)


y = opihi_flow[69645]

y[y == 0].resample('A-JUN').count()



