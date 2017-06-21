# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 14:39:29 2016

@author: MichaelEK
"""

from pandas import merge, read_csv
from query_use_allo_fun import w_query

#################################
### Parameters

series_csv = 'C:/ecan/base_data/usage/usage_takes_series_sw_up_with_cav.csv'
allo_csv = 'C:/ecan/base_data/usage/takes_results_with_cav.csv'
new_site_csv = 'C:/ecan/Projects/requests/top_50/CRC151408_L36_2352.csv'
fereday_csv = 'C:/ecan/Projects/requests/top_50/fereday.csv'
browns_rock_csv = 'C:/ecan/Projects/requests/top_50/browns_rock.csv'

allo_cols = ['crc', 'wap', 'take_type', 'catchment', 'irr_area', 'gw_zone', 'sw_zone', 'use_type', 'catchment_num', 'cwms_zone']
#cwms_zone = ['Orari-Temuka-Opihi-Pareora']
#take_type = ['Take Groundwater']
waps = ['L36/2130', 'L36/2072', 'L36/2131']
year = [2015]

export_path = 'C:/ecan/Projects/requests/top_50/CRC151408_use.csv'

#crc_rem = ['CRC091757', 'CRC151081']

allo_col = ['ann_allo_m3']
#allo_col = ['ann_allo_m3']

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]

allo_use1 = merge(series, allo, on=['crc', 'wap'])

### Read in input data for adding to the total usage

new_site = read_csv(new_site_csv, index_col=0, parse_dates=True, infer_datetime_format=True)

ann_sum = new_site.resample('A-JUN').sum()

### Read in fereday for yearly estimate

fereday = read_csv(fereday_csv, index_col=0, parse_dates=True, infer_datetime_format=True)

#fd_day = fereday.resample('D').mean()
fd_ann = fereday.resample('A-JUN').sum()

### Read in browns rock for yearly estimate

br1 = read_csv(browns_rock_csv, index_col=0, dayfirst=True, parse_dates=[[0,1]], infer_datetime_format=True)
br1.index.name = 'date'

br2 = br1 * 60*5

br_ann = br2.resample('A-JUN').sum()

#################################
### Query data

q1 = w_query(allo_use1, grp_by=['dates'], allo_col=allo_col, wap=waps, years=year, export_path=export_path, export=False)

q1['usage_m3'][0] + ann_sum['2015']

