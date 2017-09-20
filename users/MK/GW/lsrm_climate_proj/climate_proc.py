# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 13:57:44 2017

@author: Michaelek
"""

from xarray import open_dataset, concat, Dataset
from os import path, walk, makedirs
from pandas import to_datetime

###############################################
### Parameters

## Input data

proj_dir = r'D:\niwa_data\climate_projections'












































###################################################
### Testing

nc1 = r'D:\niwa_data\climate_projections\RCP2.6\BCC-CSM1.1\TotalPrecipCorr_VCSN_BCC-CSM1.1_RCP2.6_2006_2120_south-island_p05_daily_ECan.nc'

ds1 = open_dataset(nc1)

































