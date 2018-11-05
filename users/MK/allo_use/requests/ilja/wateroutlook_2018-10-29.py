# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import pandas as pd
from hilltoppy import com, web_service

############################################
### Parameters

hts = r'\\hilltop01\data\telemetry\WaterOutlook.hts'

base_url = 'http://wateruse.ecan.govt.nz'
hts1 = 'WaterUse.hts'

export_dir = r'E:\ecan\local\Projects\requests\suz\2018-09-10'
export1 = 'rdr_rwl_usage_data_2018-09-10.csv'


############################################
### Extract data

mtypes = com.measurement_list(hts)

tsdata_list = []
for name, group in mtypes.iterrows():
    print(group)
    start_date = str(pd.to_datetime(group.end_date) - pd.DateOffset(days=1))
    tsdata = com.get_data_quantity(hts, sites=[group.site], mtypes=[group.mtype], start=start_date, end=group.end_date).reset_index()
    tsdata_list.append([group.site, group.mtype, pd.infer_freq(tsdata.time)])

ts_summ1 = pd.DataFrame(tsdata_list, columns=['site', 'mtype', 'freq'])


start_date = '2018-10-27'
end_date = '2018-10-28'
sites = ['M35/2278-M1', 'M35/3137-M1']
mtype = ['Average Flow']

tsdata = com.get_data_quantity(hts, sites, mtype, start=start_date, end=group.end_date).reset_index()

tsdata1 = web_service.get_data(base_url, hts1, 'M35/2278-M1', 'Average Flow', from_date=start_date, to_date=end_date)

tsdata2 = web_service.get_data(base_url, hts1, 'M35/3137-M1', 'Average Flow', from_date=start_date, to_date=end_date)


