# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from VCN_process_fun import rd_vcn
from ts_stats_fun import w_resample

######################################
### Parameters

data_dir = 'Y:/Data/VirtualClimate/VCN_precip_ET_2016-06-06'
site_loc_csv = 'Y:/Data/VirtualClimate/test1.csv'

data_type = 'ET'
#data_type = 'precip'

save_path1 = 'Y:/Data/VirtualClimate/test1_output.csv'

#####################################
### Create dataframe from many VCN csv files

et = rd_vcn(data_dir=data_dir, select=site_loc_csv, data_type=data_type, export=False, export_path=save_path1)

### Run stats

et_mon = w_resample(et, period='month', agg=True, agg_fun='mean')
et_yr = w_resample(et, period='water year', agg=True, agg_fun='mean')

####################################
### Plot yearly and monthly values






