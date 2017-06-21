# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from os import path, makedirs
from import_fun import rd_vcn
from ts_stats_fun import w_resample, flow_stats
from hydro_plot_fun import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot

######################################
### Parameters

data_dir = 'Y:/VirtualClimate/VCN_precip_ET_2016-06-06'
site_loc_csv = 'C:/ecan/local/Projects/otop/GIS/tables/pareora_vcn.csv'
site_col = 'Data VCN_s'

data_type1 = 'ET'
data_type2 = 'precip'

save_path1 = 'Y:/VirtualClimate/test1_output.csv'

export_path = 'C:/ecan/shared/projects/otop/reports/current_state_2016/figures/climate_v02'
et_mon_png = 'pareora_et_mon.png'
et_yr_png = 'pareora_et_yr.png'
precip_mon_png = 'pareora_precip_mon.png'
precip_yr_png = 'pareora_precip_yr.png'
diff_mon_png = 'pareora_diff_mon.png'
diff_yr_png = 'pareora_diff_yr.png'

## Create directories if needed
if not path.exists(export_path):
    makedirs(export_path)

#####################################
### Create dataframe from many VCN csv files

et = rd_vcn(data_dir=data_dir, select=site_loc_csv, site_col=site_col,  data_type=data_type1, export=False, export_path=save_path1)
precip = rd_vcn(data_dir=data_dir, select=site_loc_csv, site_col=site_col, data_type=data_type2, export=False, export_path=save_path1)

### Run stats

et_day = w_resample(et, period='days', agg=True, agg_fun='mean')
precip_day = w_resample(precip, period='days', agg=True, agg_fun='mean')
diff = precip_day - et_day

et_yr = w_resample(et, period='water year', agg=True, agg_fun='mean')
precip_yr = w_resample(precip, period='water year', agg=True, agg_fun='mean')

#flow_stats(precip_day)

et_yr['1992':'2015'].describe()
precip_yr.describe()

####################################
### Plot yearly and monthly values

mon_boxplot(et_day, dtype='PET', fun='sum', export_path=export_path, export_name=et_mon_png)
mon_boxplot(precip_day, dtype='precip', fun='sum', export_path=export_path, export_name=precip_mon_png)
mon_boxplot(diff, dtype='diff', fun='sum', export_path=export_path, export_name=diff_mon_png)

multi_yr_barplot(precip_day, et_day, col='all', dtype='both', single=True, fun='sum', start='1992', end='2015', alf=False, export_path=export_path, export_name=precip_yr_png)






