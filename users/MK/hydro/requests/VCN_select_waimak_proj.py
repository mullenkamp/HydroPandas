# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from VCN_process_fun import rd_vcn, rd_vcn_niwa_lst_dir
from ts_stats_fun import w_resample

######################################
### Parameters

#data_dir = 'Y:/VirtualClimate/VCN_precip_ET_2016-06-06'
data_dir_cur = 'Y:/VirtualClimate/projections/Waimakariri/current'
data_dir_proj = 'Y:/VirtualClimate/projections/Waimakariri/2040'

#site_loc_csv = 'C:/ecan/local/Projects/requests/raymond/site_list.csv'
#site_col = 'sites'

proj_index = [0, 3, 4, 15]
cur_index = [0, 3, 4, 15]

col_names = ['site', 'date', 'rain', 'pet']
date_col=2
date_format = '%Y%m%d'
ext = 'lst'
header=None

data_type1 = 'ET'
data_type2 = 'precip'

save_path1 = 'C:/ecan/local/Projects/requests/raymond/PET_data_current.csv'
save_path2 = 'C:/ecan/local/Projects/requests/raymond/precip_data_current.csv'
save_path3 = 'C:/ecan/local/Projects/requests/raymond/PET_data_proj.csv'
save_path4 = 'C:/ecan/local/Projects/requests/raymond/precip_data_proj.csv'

#####################################
### Read in RAW data and organize

current = rd_vcn_niwa_lst_dir(data_dir_cur, ext, cols=cur_index, col_names=col_names, date_format=date_format, date_col=date_col, header=header)

proj = rd_vcn_niwa_lst_dir(data_dir_proj, ext, cols=cur_index, col_names=col_names, date_format=date_format, date_col=date_col, header=header)

#####################################
### Reorganize df

current_rain = current.pivot('date', 'site', 'rain')
current_pet = current.pivot('date', 'site', 'pet')

proj_rain = proj.pivot('date', 'site', 'rain')
proj_pet = proj.pivot('date', 'site', 'pet')

####################################
### Save data

current_rain.to_csv(save_path2)
current_pet.to_csv(save_path1)

proj_rain.to_csv(save_path4)
proj_pet.to_csv(save_path3)






