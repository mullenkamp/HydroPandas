# -*- coding: utf-8 -*-
"""
Created on Thu Aug 03 15:37:50 2017

@author: MichaelEK
"""

from core.ecan_io.hilltop import rd_hilltop_data, rd_hilltop_sites, proc_ht_use_data, convert_site_names
from core.misc import rd_dir
from os.path import join, split
from pandas import concat, Series
from core.misc import save_df

#######################################################
#### Parameters

#fpath = {'tel': r'E:\ecan\hilltop\xml_test\tel', 'annual': r'E:\ecan\hilltop\xml_test\annual', 'archive': r'E:\ecan\hilltop\xml_test\archive'}
#fpath = {'tel': r'\\hilltop01\Hilltop\Data\Telemetry', 'annual': r'\\hilltop01\Hilltop\Data\Annual', 'archive': r'\\hilltop01\Hilltop\Data\Archive'}
fpath = {'tel': r'\\hilltop01\Hilltop\Data\Telemetry', 'annual': r'\\hilltop01\Hilltop\Data\Annual'}
exclude_hts = ['Anonymous_AccToVol.hts', 'Anonymous_FlowToVolume.hts', 'RenameSites.hts', 'Telemetry.hts']

hts1 = r'\\hilltop01\Hilltop\Data\Telemetry\WaterMetrics.hts'

output_base = r'E:\ecan\local\Projects\hilltop\2017-08_tests'
sites_csv = 'ht_site_info.csv'
out_data_w_m = 'ht_usage_with_Ms.csv'
out_data = 'ht_usage_no_Ms.csv'
out_data_hdf = 'ht_usage_daily.h5'

########################################################
#### Functions



####################################################

ht_sites_lst = []
ht_data_lst = []
for i in fpath:
    hts_files = rd_dir(fpath[i], '.hts')
    for j in hts_files:
        if j not in exclude_hts:
            print(join(fpath[i], j))
            ## Sites
            sdata = rd_hilltop_sites(join(fpath[i], j))
            sdata['hts_file'] = j
            sdata['folder'] = split(fpath[i])[1]
            ht_sites_lst.append(sdata)

            ## Data
            if not sdata.empty:
                ht_data1 = rd_hilltop_data(join(fpath[i], j), agg_period='day')
                ht_data_lst.append(ht_data1)

ht_sites = concat(ht_sites_lst)
ht_data = concat(ht_data_lst)

ht_sites.to_csv(join(output_base, sites_csv), index=False, encoding='utf8')

ht_data = rd_hilltop_data(hts1, sites=None, mtypes=None, start=None, end=None, agg_period='day', agg_n=1, fun=None)

ht2 = proc_ht_use_data(ht_data)
ht2.to_csv(join(output_base, out_data_w_m), header=True)

ht3 = ht2.reset_index()
site_names = ht3.site

site_names1 = convert_site_names(site_names)
ht3.loc[:, 'site'] = site_names1
ht4 = ht3[ht3.site.notnull()]

ht5 = ht4.groupby(['site', 'time']).data.sum().round(2)
ht5.to_csv(join(output_base, out_data), header=True)
save_df(ht5, join(output_base, out_data_hdf))

















