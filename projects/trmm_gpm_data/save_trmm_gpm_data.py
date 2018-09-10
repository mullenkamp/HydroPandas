# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 16:29:58 2018

@author: MichaelEK
"""
import os
import pandas as pd
from hydrointerp.io import Gesdisc
import xarray as xr
from re import search, IGNORECASE, findall


def rd_dir(data_dir, ext, file_num_names=False, ignore_case=True):
    """
    Function to read a directory of files and create a list of files associated with a spcific file extension. Can also create a list of file numbers from within the file list (e.g. if each file is a station number.)
    """

    if ignore_case:
        files = [filename for filename in os.listdir(data_dir) if search('.' + ext + '$', filename, IGNORECASE)]
    else:
        files = [filename for filename in os.listdir(data_dir) if search('.' + ext + '$', filename)]

    if file_num_names:
        site_names = [int(findall("\d+", fi)[0]) for fi in files]
        return files, site_names
    else:
        return files


###################################################
### Parameters

base_dir = r'E:\ecan\shared\base_data\nasa'

username = 'Dryden' # Need to change for test
password = 'NasaData4me' # Need to change for test

missions = {'trmm': 'precipitation', 'gpm': 'precipitationCal'}
#missions = {'trmm': 'precipitation'}

start_date = {'trmm': '1998-01-01', 'gpm': '2014-03-12'}
#end_date = {'trmm': '2018-05-30', 'gpm': '2018-05-31'}
end_date = {'trmm': '2017-12-31', 'gpm': '2017-12-31'}
min_lat=-48
max_lat=-34
min_lon=166
max_lon=179

#####################################################
### get data

for m in missions:
    file_name = rd_dir(os.path.join(base_dir, m), 'nc')[0]
    next_date = pd.Timestamp(file_name[-22:-12]) + pd.Timedelta('1 day')
    ge = Gesdisc(username, password, m)
    ds1 = ge.get_data(missions[m], str(next_date.date()), end_date[m], min_lat, max_lat, min_lon, max_lon)

#####################################################
### Save data

    file_name = '{m}_{start}_{end}_daily_nz.nc'.format(m=m, start=start_date[m], end=end_date[m])
    ds1.to_netcdf(os.path.join(base_dir, m, file_name))




































































