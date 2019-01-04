# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 16:29:58 2018

@author: MichaelEK
"""
import os
import numpy as np
import pandas as pd
import xarray as xr
from nasadap import agg
from datetime import datetime


###################################################
### Parameters

cache_dir = r'\\fs02\GroundWaterMetData$\nasa\cache\nz'
save_dir = r'\\fs02\GroundWaterMetData$\nasa\precip'

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

min_lat=-49
max_lat=-33
min_lon=165
max_lon=180
dl_sim_count = 30
tz_hour_gmt = 12

log_file = 'nasa_precip_agg_log.csv'


def write_log(text, file):
    f = open(file, 'a')
    f.write("{}\n".format(text))

####################################################
### get and save data

now1 = str(datetime.now())

try:
    agg.year_combine(param_dict, save_dir, username, password, cache_dir, tz_hour_gmt, min_lat, max_lat, min_lon, max_lon, dl_sim_count)
    write_log(', '.join([now1, 'success']), os.path.join(save_dir, log_file))

except Exception as err:
    write_log(', '.join([now1, str(err)[:299]]), os.path.join(save_dir, log_file))





