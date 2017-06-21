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
from ts_stats_fun import w_resample, ts_comp, flow_dur, gauge_proc, fre_accrual, malf7d, flow_stats, tsreg, summ_stats
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
aqua_cal_csv = 'aqua_cal_data.csv'
aqua_mod_csv = 'aqua_mod_data.csv'

#other_sites = [69505, 70105]
aqua_sites = [70103, 69607, 69602, 69683, 69684, 1965]

### Plotting

orari_sites = [69514, 69508]
opihi_sites = [69607, 69602, 1965, 69683, 69684]
pareora_sites = [70103]

stats_col = ['Median', 'Mean', 'MALF7D all yrs', 'Avg_num_days', 'fre_calc', 'mean_accrual']
stats_col_names = ['Median (m3/s)', 'Mean (m3/s)', '7DMALF (m3/s)', 'Mean days below 7DMALF', 'Fre3 events per year', 'Accrual period (days)']
ros_col = ['ros_partial_norm', 'ros_full_norm']
ros_col_names = ['Site', 'Bands', 'Partial Restrictions (%)', 'Full Restrictions (%)']
#ros_names_dict = {'orari_a_2040': 'Orari A', 'orari_b': 'Orari B', 'ohapi_a': 'Ohapi A', 'tengawai_1': 'Tengawai 1', 'tengawai_3': 'Tengawai 3', 'tengawai_5b': 'Tengawai 5b', 'opihi_irr_an': 'Opihi Irr AN', 'opihi_irr_aa': 'Opihi Irr AA', 'opihi_irr_bn': 'Opihi Irr BN', 'opihi_irr_ba': 'Opihi Irr BA', 'opihi_ws_an': 'Opihi WS AN', 'opihi_ws_aa': 'Opihi WS AA', 'opihi_ws_bn': 'Opihi WS BN', 'opihi_ws_ba': 'Opihi WS BA', 'temuka_irr_a': 'Temuka Irr A', 'temuka_irr_b': 'Temuka Irr B', 'temuka_ws_a': 'Temuka WS A', 'temuka_ws_b': 'Temuka WS B'}
ros_names_dict = {'orari_a_2040': 'A', 'orari_b': 'B', 'ohapi_a': 'A', 'tengawai_1': '1', 'tengawai_3': '3', 'tengawai_5b': '5b', 'opihi_irr_an': 'Irr AN', 'opihi_irr_aa': 'Irr AA', 'opihi_irr_bn': 'Irr BN', 'opihi_irr_ba': 'Irr BA', 'opihi_ws_an': 'WS AN', 'opihi_ws_aa': 'WS AA', 'opihi_ws_bn': 'WS BN', 'opihi_ws_ba': 'WS BA', 'temuka_irr_a': 'Irr A', 'temuka_irr_b': 'Irr B', 'temuka_ws_a': 'WS A', 'temuka_ws_b': 'WS B'}
site_names_dict = {69607: 'Opihi River at SH1', 69602: 'Temuka River at Manse Bridge', 69514: 'Orari River Upstream of Ohapi River', 69508: 'Ohapi Creek at Brown Road', 70103: 'Pareora River at SH1', 1965: 'Te Moana River at Speechleys Bridge', 69683: 'Wiahi River at SH72', 69684: 'Dobies Stream at Woolscours Road', 69635: 'Te Nga Wai River at Picnic Grounds', 69618: 'Opihi River at Rockwood', 69619: 'South Opuha River at Stoneleigh Road', 69614: 'Opuha River at Skipton', 69615: 'North Opuha River at Clayton Road'}

### Output
excel_file_pat = 'pat_results_v04.xlsx'
excel_file_aqua = 'aqua_results_v04.xlsx'
pat_df_names = ['current', 'cal', 'mod']
aqua_df_names = ['current', 'cal', 'mod']
excel_summ_file = 'summ_results_v02.xlsx'

####################################
#### Load flow data

### Patrick's model
## processed flow data
opihi_mod = tsreg(rd_ts(path.join(base_path, opihi_csv)))
orari_cal = tsreg(rd_ts(path.join(base_path, orari_cal_csv)))
orari_mod = tsreg(rd_ts(path.join(base_path, orari_mod_csv)))

cal = orari_cal
cal.columns = cal.columns.astype(int)

mod = concat([opihi_mod, orari_mod], axis=1)
mod.columns = mod.columns.astype(int)

## Load in from Hydstra...or not...
all_sites = []
#all_sites.extend(other_sites)
all_sites.extend(mod.columns.tolist())

flow1a = rd_ts(flow_csv)
flow1a.columns = flow1a.columns.astype(int)
current = flow1a.loc[mod.index, all_sites]

pat_sets = [current, cal, mod]

### Aqualinks model
## processed flow data
aqua_cal = tsreg(rd_ts(path.join(base_path, aqua_cal_csv)))
aqua_mod = tsreg(rd_ts(path.join(base_path, aqua_mod_csv)))

cal = aqua_cal
cal.columns = cal.columns.astype(int)

mod = aqua_mod
mod.columns = mod.columns.astype(int)

## Load in from Hydstra...or not...
all_sites = []
all_sites.extend(mod.columns.tolist())

flow1a = rd_ts(flow_csv)
flow1a.columns = flow1a.columns.astype(int)
current = flow1a.loc[mod.index, aqua_sites]

aqua_sets = [current, cal[aqua_sites], mod[aqua_sites]]

###################################
#### Run the main function

stats_pat, ros_means_pat = summ_stats(pat_sets, pat_df_names, path.join(base_path, excel_file_pat))
stats_aqua, ros_means_aqua = summ_stats(aqua_sets, aqua_df_names, path.join(base_path, excel_file_aqua))


###############################
#### Plotting/reordering

mod_stats_pat = stats_pat[2][stats_col]
mod_stats_aqua = stats_aqua[2][stats_col]

mod_stats = concat([mod_stats_pat, mod_stats_aqua])

mod_stats.columns = stats_col_names
mod_stats.index = [site_names_dict[i] for i in mod_stats.index]
mod_stats.index.name = 'Site'

mod_stats = mod_stats.sort_index()


mod_ros_pat = ros_means_pat[2][ros_col]
mod_ros_aqua = ros_means_aqua[2][ros_col]

mod_ros = concat([mod_ros_pat, mod_ros_aqua]).drop(['orari_a_3yrs', 'orari_a', 'tengawai_CRC091757'], level=1).reset_index()
mod_ros.columns = ros_col_names

mod_ros.loc[:, ros_col_names[0]] = [site_names_dict[i] for i in mod_ros.loc[:, ros_col_names[0]]]
mod_ros.loc[:, ros_col_names[1]] = [ros_names_dict[i] for i in mod_ros.loc[:, ros_col_names[1]]]

mod_ros2 = mod_ros.set_index(keys=ros_col_names[0:2]).sort_index() * 100

## Export summary of results
excel = ExcelWriter(path.join(base_path, excel_summ_file))
mod_stats.to_excel(excel, 'mod_stats')
mod_ros2.to_excel(excel, 'mod_ros')
#excel.save()

### Calibrated models

cal_stats_pat = stats_pat[1][stats_col]
cal_stats_aqua = stats_aqua[1][stats_col]

cal_stats = concat([cal_stats_pat, cal_stats_aqua])

cal_stats.columns = stats_col_names
cal_stats.index = [site_names_dict[i] for i in cal_stats.index]
cal_stats.index.name = 'Site'

cal_stats = cal_stats.sort_index()


cal_ros_pat = ros_means_pat[1][ros_col]
cal_ros_aqua = ros_means_aqua[1][ros_col]

cal_ros = concat([cal_ros_pat, cal_ros_aqua]).drop(['orari_a_3yrs', 'orari_a', 'tengawai_CRC091757'], level=1).reset_index()
cal_ros.columns = ros_col_names

cal_ros.loc[:, ros_col_names[0]] = [site_names_dict[i] for i in cal_ros.loc[:, ros_col_names[0]]]
cal_ros.loc[:, ros_col_names[1]] = [ros_names_dict[i] for i in cal_ros.loc[:, ros_col_names[1]]]

cal_ros2 = cal_ros.set_index(keys=ros_col_names[0:2]).sort_index() * 100

### Export summary of results
#excel = ExcelWriter(path.join(base_path, excel_summ_file))
cal_stats.to_excel(excel, 'cal_stats')
cal_ros2.to_excel(excel, 'cal_ros')
excel.save()





##################################
#### Extra
t1 = cal[69515].round(3)
t1.to_csv(path.join(base_path, '69515_mod_flow.csv'))












