# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from pandas import read_table, to_datetime, DataFrame, read_csv, concat
from VCN_process import rd_rain
from ts_stats_v01 import precip_agg, w_resample

######################################
### Parameters

data_dir = 'C:/ecan/base_data/precip/VCN data with ECan sites/rain_ECan (fixed)'
site_loc_csv = 'C:/ecan/Projects/otop/precip/input/69618_sites.csv'
site_loc_col = 'Data VCN_s'
save_path1 = 'C:/ecan/Projects/otop/precip/69618_precip1.csv'

#####################################
### Run processing function

precip = rd_rain(data_dir, select=site_loc_csv, site_col=site_loc_col, export_path=save_path1)

### Additional stats
agg1 = precip_agg(precip)
yr1 = w_resample(agg1, period='water year', s_mon=7)

yr1_mean = yr1_grp.mean()
yr1_cv = yr1_grp.std()/yr1_mean
yr1_count = yr1_grp.count()


mon1 = w_resample(agg1, period='month', s_mon=1)

mon1_grp = mon1.groupby(mon1.index.month)

mon1_mean = mon1_grp.mean()
mon1_cv = mon1_grp.std()/mon1_mean
mon1_count = mon1_grp.count()

