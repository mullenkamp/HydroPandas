# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d, where, any, nan
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame, to_numeric, date_range, concat
from allo_use_fun import w_query
from allo_use_plot_fun import allo_plt, allo_multi_plot, allo_stacked_plt
from misc_fun import printf
import seaborn

#################################
### Parameters

#plt.ioff()
#plt.ion()

## import parameters
series_csv = 'S:/Surface Water/shared/base_data/usage/usage_takes_series_sw_up_with_cav.csv'
allo_csv = 'S:/Surface Water/shared/base_data/usage/takes_results_with_cav.csv'
zones_csv = 'S:/Surface Water/shared/base_data/usage/WAPs_catch_zones_cwms.csv'
waimak_grps_csv = 'S:/Surface Water/backups/MichaelE/Projects/requests/waimak_report/waimak_groups.csv'

allo_cols = ['crc', 'wap', 'take_type', 'irr_area', 'gw_zone', 'use_type']
zone_cols= ['wap', 'NZTMX', 'NZTMY', 'NIWA_catch', 'catchment', 'g_catch_num', 'g_catch', 'catchment_num', 'swaz_grp', 'swaz', 'swaz_num', 'cwms_zone']

## query parameters
cwms_zone = ['Waimakariri']
#zone_grp_opihi = ['Opihi']
#zone_grp_orari = ['Orari']
#zone_grp_par = ['Pareora']
allo_col = ['ann_allo_m3', 'ann_up_allo_m3']
#out1 = 'Rangitata Water Conservation Order'
#minor_swaz = ['Levels and Seadown Plains', 'Saltwater Creek-Otipua', 'Washdyke']

take_type = ['Take Surface Water']

## output parameters
export_results_path = r'S:\Surface Water\shared\projects\waimak\reports\current_state\figures\allo_use'
export_path = r'S:\Surface Water\shared\projects\waimak\reports\current_state\figures\allo_use'
swaz_path = 'SWAZ'
use_path = 'use'
use_type_path = 'use_type'
name4 = 'waimak'

ex_name = 'allo_use_restr_v03.png'
ex_name_type = 'tot_allo_type_v03.png'
ex_name_results = 'waimak_allo_results_v03.csv'
ex_name_data = 'waimak_allo_data_v03.csv'

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]
sw_zones = read_csv(zones_csv).drop(['FID','take_type'], axis=1)
sw_zones.columns = zone_cols
waimak_grps = read_csv(waimak_grps_csv)[['ZONE_GROUP', 'ZONE_NUMBE']]
waimak_grps.columns = ['swaz_grp2', 'swaz_num']
sw_zones.swaz_num = to_numeric(sw_zones.swaz_num, errors='coerce')

sw_zones2 = merge(sw_zones, waimak_grps, on=['swaz_num'])

sw_zones2['swaz'] = sw_zones2.swaz.str.replace('/', '-')
sw_zones2['swaz_grp'] = sw_zones2.swaz_grp.str.replace('/', '-')

allo_use1 = merge(series, allo, on=['crc', 'wap'])
allo_use2 = merge(allo_use1, sw_zones2, on=['wap'])

##################################
### fix incorrect use type for selected consents

crc_fix1 = 'CRC000585.6'
allo_fix1 = 2.7e+08
date_fix1 = '2010-06-30'

crc_fix2 = 'CRC000585.3'
allo_fix2 = 4.39738e+08
date_fix2 = '2002-06-30'

#crc_fix3 = 'CRC000585.9'
#allo_fix3 = 57100100

allo_use2.loc[(allo_use2.crc == crc_fix1) & (allo_use2.dates == date_fix1), ['ann_allo_m3', 'ann_up_allo_m3']] = allo_fix1
allo_use2.loc[(allo_use2.crc == crc_fix2) & (allo_use2.dates == date_fix2), ['ann_allo_m3', 'ann_up_allo_m3']] = allo_fix2
#allo_use2.loc[(allo_use2.crc == crc_fix3), ['ann_allo_m3', 'ann_up_allo_m3']] = allo_fix3

#consents = ['CRC000585.9', 'CRC000585.6', 'CRC000585.5', 'CRC000585.3', 'CRC000585.2', 'CRC000585.1', 'CRC000585']
#
#condition = allo_use2.crc.isin(consents)
#
#allo_use2['use_type'] = where(condition, 'irrigation', allo_use2['use_type'])
#
#check = allo_use2[allo_use2.crc.isin(consents)]
#check['use_type']
#
#
WDC_crc = ['CRC133965', 'CRC012907', 'CRC952569']
#
#allo_use2.loc[in1d(allo_use2.crc, WDC_crc), 'use_type'] = 'stockwater'

allo_use2 = allo_use2.loc[~in1d(allo_use2.crc, WDC_crc), :]
allo_use2.loc[allo_use2.use_type == 'stockwater', 'use_type'] = 'irrigation'


req_fields = ['crc', 'dates', 'ann_allo_m3', 'ann_up_allo_m3', 'usage_m3', 'use_type', 'take_type', 'swaz_grp2', 'cwms_zone']
dict1 = {'CRC952569': 47300000, 'CRC012907': 66200000, 'CRC133965': 66200000}
dates1 = date_range(start='1997-06-30', end='2015-06-30', freq='A-JUN')
crc_lst = ['CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC133965', 'CRC133965']
ann_allo1 = [dict1[f] for f in crc_lst]
usage1 = [nan] * len(dates1)
use_type1 = ['stockwater'] * len(dates1)
take_type1 = ['Take Surface Water'] * len(dates1)
swaz_grp1 = ['WRRP'] * len(dates1)
cwms_zone1 = ['Waimakariri'] * len(dates1)

wdc_df = DataFrame([crc_lst, dates1.format(), ann_allo1, ann_allo1, usage1, use_type1, take_type1, swaz_grp1, cwms_zone1]).T
wdc_df.columns = req_fields

dict2 = {'CRC990421': 50000000}
dates2 = date_range(start='1993-06-30', end='2015-06-30', freq='A-JUN')
crc_lst2 = ['CRC990421'] * len(dates2)
ann_allo2 = [dict2[f] for f in crc_lst2]
usage2 = [nan] * len(dates2)
use_type2 = ['irrigation'] * len(dates2)
take_type2 = ['Take Surface Water'] * len(dates2)
swaz_grp5 = ['WRRP'] * len(dates2)
cwms_zone2 = ['Waimakariri'] * len(dates2)

claxby_df = DataFrame([crc_lst2, dates2.format(), ann_allo2, ann_allo2, usage2, use_type2, take_type2, swaz_grp5, cwms_zone2]).T
claxby_df.columns = req_fields

dict2 = {'CRC000585': 331000000}
dates2 = date_range(start='1998-06-30', end='2001-06-30', freq='A-JUN')
crc_lst2 = ['CRC000585'] * len(dates2)
ann_allo2 = [dict2[f] for f in crc_lst2]
usage2 = [nan] * len(dates2)
use_type2 = ['irrigation'] * len(dates2)
take_type2 = ['Take Surface Water'] * len(dates2)
swaz_grp5 = ['WRRP'] * len(dates2)
cwms_zone2 = ['Waimakariri'] * len(dates2)

wil_df = DataFrame([crc_lst2, dates2.format(), ann_allo2, ann_allo2, usage2, use_type2, take_type2, swaz_grp5, cwms_zone2]).T
wil_df.columns = req_fields


allo_use2 = concat([allo_use2, wdc_df, claxby_df, wil_df])


### Read in input data to be used in the query

#################################
### Query data

results1 = w_query(allo_use2, grp_by=['dates', 'swaz_grp2', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, take_type=take_type, export_path=path.join(export_results_path, ex_name_results), export=True)

results1_data = w_query(allo_use2, grp_by=['dates', 'swaz_grp2', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, take_type=take_type, export_path=path.join(export_results_path, ex_name_data), export=True, debug=True)

#################################
### Make directories if necessary

#otop1.index.levels[1]
#
#for i in otop1.index.levels[1]:
#    if not path.exists(path.join(export_path, i)):
#        makedirs(path.join(export_path, i))
#    if not path.exists(path.join(export_path, i, swaz_path)):
#        makedirs(path.join(export_path, i, swaz_path))


################################
### plot the summaries

## Single sets
# Allo and usage
allo_plt(results1, export_path=export_path, export_name=name4 + '_' + ex_name)

# Use types
allo_stacked_plt(results1, start='1997', agg_level=[0, 2], export_path=export_path, export_name=name4 + '_' + ex_name_type)

## Multi-sets
# SWAZ groups totals of allo and usage
allo_multi_plot(results1, agg_level=[0, 1], export_path=path.join(export_path, swaz_path, use_path), export_name=name4 + '_' + ex_name)

# SWAZ groups totals of allo over long period
#allo_multi_plot(otop_use_type2, agg_level=[0, 1], start='1990', cat=['tot_allo'], export_path=export_path, export_name=name5 + '_' + ex_name)

# SWAZ groups stacked by use type
allo_multi_plot(results1, agg_level=[0, 1, 2], index_level=1, plot_fun=allo_stacked_plt, export_path=path.join(export_path, swaz_path, use_type_path), export_name=name4 + '_' + ex_name_type, start='1997')


#############################
### Check oddities

crcs = ['CRC166677', 'CRC144253', 'CRC000585.9', 'CRC169707', 'CRC152927', 'CRC133965']
cols1 = ['crc', 'dates', 'ann_allo_m3', 'ann_up_allo_m3', 'usage_m3', 'use_type', 'swaz_grp2', 'cwms_zone']
cols2 = ['crc', 'ind_ann_allo', 'from_date', 'to_date', 'use_type']

allo_use2.loc[in1d(allo_use2.crc, crcs), cols1]

allo.loc[in1d(allo.crc, crcs), cols2]
series[in1d(series.crc, crcs)]


allo_use2.loc[in1d(allo_use2.crc, WDC_crc), cols1]

allo.loc[in1d(allo.crc, WDC_crc), cols2]
series[in1d(series.crc, WDC_crc)]



req_fields = ['crc', 'dates', 'ann_allo_m3', 'ann_up_allo_m3', 'usage_m3', 'use_type', 'take_type', 'swaz_grp2', 'cwms_zone']

dict1 = {'CRC952569': 47300000, 'CRC012907': 66200000, 'CRC133965': 66200000}


dates1 = date_range(start='1997-06-30', end='2015-06-30', freq='A-JUN')

crc_lst = ['CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC952569', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC012907', 'CRC133965', 'CRC133965']

ann_allo1 = [dict1[f] for f in crc_lst]

usage1 = [nan] * len(dates1)

use_type1 = ['stockwater'] * len(dates1)

take_type1 = ['Take Surface Water'] * len(dates1)

swaz_grp1 = ['WRRP'] * len(dates1)

cwms_zone1 = ['Waimakariri'] * len(dates1)


wdc_df = DataFrame([crc_lst, dates1.format(), ann_allo1, ann_allo1, usage1, use_type1, take_type1, swaz_grp1, cwms_zone1]).T
wdc_df.columns = req_fields


results1_data[results1_data.dates == '1998-06-30']

data4 = results1_data[results1_data.dates == '2010-06-30']
data4a = data4.sort_values('ann_allo_m3', ascending=False)[:10]

data4a.loc[:, cols1]

data5 = results1_data[results1_data.dates == '2009-06-30']
data5a = data5.sort_values('ann_allo_m3', ascending=False)[:10]

data5a.loc[:, cols1]

data6 = results1_data[results1_data.dates == '2011-06-30']
data6a = data6.sort_values('ann_allo_m3', ascending=False)[:10]

data6a.loc[:, cols1]


data7 = results1_data[results1_data.dates == '1998-06-30']
data7a = data7.sort_values('ann_allo_m3', ascending=False)[:10]

data7a.loc[:, cols1]




'CRC990421.1'

allo_use2.loc[allo_use2.crc == 'CRC000585.9', cols1]
allo_use2.loc[allo_use2.crc == 'CRC000585.6', cols1]













