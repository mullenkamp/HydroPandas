# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 08:16:44 2019

@author: michaelek
"""

import os
import pandas as pd
from pdsql import mssql
from allotools import AlloUsage
from datetime import datetime

pd.options.display.max_columns = 10

#######################################
### Parameters

from_date = '2013-07-01'
to_date = '2018-06-30'

crc = {'crc': ['CRC962531.3', 'CRC010985.2', 'CRC010350.3', 'CRC140929']}
waps = {'wap': ['L36/1458', 'L36/1244', 'L36/2024', 'L36/2025', 'L36/1460', 'L36/1459']}


freq = 'A-JUN'

groupby = ['crc', 'wap', 'date']
#groupby = ['crc', 'date']

export_path = r'E:\ecan\local\Projects\requests\cpwl\2019-02-22'
export1 = 'cpwl_crc_wap_2019-02-22.csv'

#######################################
### Extract

a2 = AlloUsage(from_date, to_date, crc_filter=crc, in_allo=False)
#a2 = AlloUsage(from_date, to_date, crc_wap_filter=waps, in_allo=False)

#usage1 = a2.get_usage_ts(freq, groupby)

combo1 = a2.get_ts(['allo', 'usage'], freq, groupby)[['total_allo', 'total_usage']].round()

combo1.to_csv(os.path.join(export_path, export1))

















