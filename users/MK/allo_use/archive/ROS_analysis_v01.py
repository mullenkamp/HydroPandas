# -*- coding: utf-8 -*-
"""
Script to process the low flows restiction data for reliability of supply.
"""

from pandas import merge, to_datetime, read_csv, concat
from misc_fun import printf
from import_fun import rd_sql
from allo_use_fun import restr_days

########################################
### Parameters

select = 'C:\\ecan\\local\\Projects\\Waimakariri\\GIS\\vector\\waimak_sub_regional.shp'

export_path = 'C:\\ecan\\local\\Projects\\Waimakariri\\analysis\\allo_use\\min_flow_restr_days.csv'

########################################
### Run function yo

restr = restr_days(select, export_path=export_path)









