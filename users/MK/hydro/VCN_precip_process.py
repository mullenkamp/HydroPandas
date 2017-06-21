# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from pandas import read_table, to_datetime, DataFrame, read_csv, concat, Series
from os import path
from pandas import to_datetime, DataFrame
from numpy import array, in1d, where, nan
from misc_fun import rd_dir
from VCN_process_fun import rd_vcn_ecan_dat, rd_vcn_niwa_orig_lst
from re import findall

######################################
### Parameters

precip_dir = 'C:/ecan/base_data/precip/RAW_VCN/VCN data with ECan sites/rain_ECan (fixed)'
et_dir = 'C:/ecan/base_data/precip/RAW_VCN/NIWA_original'
site_info_csv='C:/ecan/base_data/precip/niwa_precip_loc.csv'
save_dir = 'C:/ecan/base_data/precip/VCN_data'

data_dir = 'C:/ecan/base_data/precip/RAW_VCN/vcsn_2016'
up_dir = 'new'

#####################################
### Run processing function for first sets

## Read in general info
precip_files, precip_sites = rd_dir(precip_dir, 'dat', True)
et_files, et_sites = rd_dir(et_dir, 'lst', True)
site_info1 = read_csv(site_info_csv)
    
## Organize useful sites
site_info2 = site_info1[['Network', 'Data VCN_s']]
site_info2.columns = ['niwa_name', 'ecan_name']
site_info2.loc[:, 'niwa_name'] = array([int(findall("\d+", fi)[0]) for fi in site_info2['niwa_name']])
site_info3 = site_info2[site_info2.ecan_name.notnull()]

precip_sel_index = in1d(precip_sites, site_info3['ecan_name'])
precip_files_sel = precip_files[precip_sel_index]

et_sel_index = in1d(et_sites, site_info3['niwa_name'])
et_files_sel = et_files[et_sel_index]

## Process all sites
for i in precip_files_sel:
    print(i)
    site_num = int(findall("\d+", i)[0])
    t1 = rd_vcn_ecan_dat(precip_dir, i, rd_index=True)
    t1.columns = ['precip']

    site_niwa = site_info3.loc[site_info3.ecan_name == site_num, 'niwa_name']
    et_loc_index = where(et_sites == site_niwa.values)[0]
    if et_loc_index.size > 0:
        et_file = et_files[where(et_sites == site_niwa.values)][0]
        t2 = rd_vcn_niwa_orig_lst(et_dir, et_file, rd_index=True)['ET']
        time1 = t1.index[0]
        t2 = t2[time1:]
    
        t3 = concat([t1, t2], axis=1)
        t3.index.name = 'date'
    else:
        t3 = t1
        t3['ET'] = nan
        t3.index.name = 'date'

    t3.to_csv(path.join(save_dir, 'precip_ET_' + str(site_num) + '.csv'))


############################################
### Processing for the new data

append_vcn(save_dir, data_dir, up_dir)













