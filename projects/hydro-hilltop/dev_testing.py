# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 11:35:43 2018

@author: MichaelEK
"""
import pandas as pd
from hilltoppy import hilltop, web_service
import sys
import os

###########################################
### Parameters

base_dir = r'\\fs02\DevManagedShares\Executables\Hilltop'

sys.path.insert(0, base_dir)

import Hilltop

dsn = 'HydroAtECan.dsn'

sm = hilltop.get_sites_mtypes(os.path.join(base_dir, dsn))

dfile1 = Hilltop.Connect(os.path.join(base_dir, dsn))

site_list = Hilltop.SiteList(dfile1)
