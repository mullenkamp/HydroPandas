# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 14:31:43 2017

@author: MichaelEK
"""

from core.ecan_io import rd_squalarc
from pandas import read_csv, concat

#### Parameters
sites_csv = r'E:\ecan\local\Projects\requests\zeb\2017-06-22\AllWellsCentralCanty.csv'

out_path = r'E:\ecan\local\Projects\requests\zeb\2017-06-22\isotope_data.csv'

#### Extract data
sites = read_csv(sites_csv)['site']
sites2 = [sites[i::3] for i in xrange(3)]

t1 = rd_squalarc(sites)

t5 = t1[t1.source == 'isotope']

t5.to_csv(out_path, encoding='utf-8', index=False)


























