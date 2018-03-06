# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 08:17:34 2018

@author: MichaelEK
"""
from os import path
import numpy as np
import pandas as pd
from core.allo_use.ros import restr_days

######################################
### Parameters

base_dir = r'E:\ecan\local\Projects\requests\suz\2018-03-05'

sites = [63101, 63001]

export_csv = 'restr_days.csv'

#######################################
### Run

restr = restr_days(sites, months=[10, 11, 12, 1, 2, 3, 4], export_path=path.join(base_dir, export_csv))
















































