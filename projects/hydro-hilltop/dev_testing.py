# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 11:35:43 2018

@author: MichaelEK
"""
#import sys
#base_dir = r'\\fs02\TestManagedShares\Executables\Hilltop'
#
#sys.path.insert(0, base_dir)
#sys.path.remove('H:\\')
#sys.path.remove('H:\\x64')

#import Hilltop
import pandas as pd
from hilltoppy import web_service as ws

import os

###########################################
### Parameters

#dsn = 'HydroAtECan_flow.dsn'

#full_path = os.path.join(base_dir, dsn)

base_url = 'http://testwateruse.ecan.govt.nz'
hts = 'AquiferManualEcanDaily.hts'
hts = 'RiverManualEcanDaily.hts'
hts = 'AbstractionDaily.hts'

site = '69607'

###########################################

#sm = hilltop.get_sites_mtypes(full_path)

#dfile1 = Hilltop.Connect(os.path.join(base_dir, dsn))
#
#site_list = Hilltop.SiteList(dfile1)

#site1 = sm.iloc[0:2]
#
#tsdata1 = hilltop.get_tsdata(full_path, site1.site.tolist(), site1.Measurement.tolist())

s1 = ws.site_list(base_url, hts)
m1 = ws.measurement_list(base_url, hts, site)
for i in range(100):
    ts1 = ws.get_data(base_url, hts, site, m1.index[0][1], '2000-01-01', '2001-01-01')

ws.build_url(base_url, hts, 'GetData', site, m1.index[0][1], '2000-01-01', '2001-01-01')
ws.build_url(base_url, hts, 'MeasurementList', site)







