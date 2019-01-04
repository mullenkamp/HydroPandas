# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 16:29:58 2018

@author: MichaelEK
"""
import xarray as xr
from nasadap import Nasa


###################################################
### Parameters

cache_dir = r'\\fs02\GroundWaterMetData$\nasa\cache\nz'

username = 'Dryden'
password = 'NasaData4me'

param_dict = {
        'trmm': {
                '3B42': 'precipitation'
                },
        'gpm': {
                '3IMERGHHL': 'precipitationCal',
                '3IMERGHH': 'precipitationCal'
                }
            }

#start_date = {'trmm': '1998-01-01', 'gpm': '2014-03-12'}
#end_date = {'trmm': '2018-05-30', 'gpm': '2018-05-31'}
#end_date = {'trmm': '2017-12-31', 'gpm': '2017-12-31'}
min_lat=-49
max_lat=-33
min_lon=165
max_lon=180
dl_sim_count = 60

#####################################################
### get data

for m in param_dict:
    print(m)
    ge = Nasa(username, password, m, cache_dir)
    products = param_dict[m]
    for p in products:
        print(p)
        ds1 = ge.get_data(p, products[p], min_lat=min_lat, max_lat=max_lat, min_lon=min_lon, max_lon=max_lon, dl_sim_count=dl_sim_count)
    ge.close()
