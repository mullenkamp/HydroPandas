# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 13:59:43 2018

@author: MichaelEK
"""
import pandas as pd
from hilltoppy import web_service as ws

pd.options.display.max_columns = 10

#############################################
### Parameters

base_url = 'http://wateruse.ecan.govt.nz'
hts = 'WQAll.hts'
site = 'SQ31045'
measurement = 'Total Phosphorus'
from_date = '1983-11-22'
to_date = '2018-04-13'
dtl_method = 'trend'




#############################################

sites_out1 = ws.site_list(base_url, hts)

sites_out2 = ws.site_list(base_url, hts, location=True)

meas_df = ws.measurement_list(base_url, hts, site)

tsdata = ws.get_data(base_url, hts, site, measurement, from_date=from_date, to_date=to_date)

tsdata1 = ws.get_data(base_url, hts, site, measurement, from_date=from_date, to_date=to_date, dtl_method=dtl_method)

tsdata2, extra2 = ws.get_data(base_url, hts, site, measurement, parameters=True)

tsdata3 = ws.get_data(base_url, hts, site, 'WQ Sample')




d_list = []

for s in sq_list:

#    mtypes = web_service.measurement_list(base_url, hts, s)

    d1 = web_service.get_data(base_url, hts, s, mtype, dtl_method='half')
    d_list.append(d1)

d2 = pd.concat(d_list).reset_index()
