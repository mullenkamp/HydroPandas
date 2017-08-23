# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 23/08/2017 12:09 PM
"""

from __future__ import division
from core import env
import pandas as pd
import numpy as np
base_path = env.sci("Groundwater/Rakaia_Ashburton/Climate")
alldata  = pd.read_excel("{}/NIWA stations (1900-present)r.xls".format(base_path),na_values=['-',' -', '- '],sheetname=1)

for code,parameter in zip([0,19,20,34,35,36],['rainfall','et_sunken_pan','et_raised_pan','et_penman_potential','priestley_potential','penman_open_evap']):
    data = alldata.loc[np.isclose(alldata.Stats_Code,code)]
    if data.duplicated(['Station','Year']).any():
        raise ValueError('duplicates in {}'.format(parameter))
    data = data.drop('Stats_Code',axis=1)
    data = data.melt(['Station','Year'],var_name='month',value_name='rain')
    data = data.replace({'month':{'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}})

    data = data.groupby(['Year', 'month','Station']).sum().unstack('Station').reset_index()
    data.to_csv('{}/niwa_data_pivoted_{}.csv'.format(base_path,parameter))

