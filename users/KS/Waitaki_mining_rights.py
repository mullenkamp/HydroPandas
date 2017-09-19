# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 12:49:27 2017

@author: KateSt
"""

#### Import hydro class
from core.classes.hydro import hydro, all_mtypes

mtypes1 = 'flow'
mtypes2 = 'flow_m'

sites1 = [71106, 71102, 71167, 71178]
sites2 = [1841, 2367, 1711435, 71105, 71111, 171159, 171158]

qual_codes = [10, 18, 20, 30, 40]

h1 = hydro()._rd_hydstra(sites = [71106], start_time=0, end_time=0, datasource='A', data_type='mean', varfrom=140, varto=140, interval='day', multiplier=1)
h2 = hydro().get_data(mtypes=mtypes2, sites=sites2, qual_codes=qual_codes)
h2 = h1.add_data(mtypes=mtypes2, sites=sites2, qual_codes=qual_codes)