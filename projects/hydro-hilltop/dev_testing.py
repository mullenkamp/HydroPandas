# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 11:35:43 2018

@author: MichaelEK
"""
import sys
base_dir = r'\\fs02\DevManagedShares\Executables\Hilltop'

sys.path.insert(0, base_dir)
sys.path.remove('H:\\')
sys.path.remove('H:\\x64')

import Hilltop
import pandas as pd
from hilltoppy import hilltop, web_service

import os

###########################################
### Parameters

dsn = 'HydroAtECan_flow.dsn'

full_path = os.path.join(base_dir, dsn)

sm = hilltop.get_sites_mtypes(full_path)

#dfile1 = Hilltop.Connect(os.path.join(base_dir, dsn))
#
#site_list = Hilltop.SiteList(dfile1)

site1 = sm.iloc[0:2]

tsdata1 = hilltop.get_tsdata(full_path, site1.site.tolist(), site1.Measurement.tolist())
