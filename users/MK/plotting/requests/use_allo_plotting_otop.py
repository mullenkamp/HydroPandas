# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 15:22:17 2016

@author: MichaelEK
"""

from os import path, makedirs
from numpy import in1d
from matplotlib import pyplot as plt
from pandas import merge, read_csv, DataFrame
from allo_use_fun import w_query
from allo_use_plot_fun import allo_plt, allo_multi_plot, allo_stacked_plt
from misc_fun import printf

#################################
### Parameters

#plt.ioff()
#plt.ion()

## import parameters
series_csv = 'C:/ecan/shared/base_data/usage/usage_takes_series_sw_up_with_cav.csv'
allo_csv = 'C:/ecan/shared/base_data/usage/takes_results_with_cav.csv'
zones_csv = 'C:/ecan/shared/base_data/usage/WAPs_catch_zones_cwms.csv'

allo_cols = ['crc', 'wap', 'take_type', 'irr_area', 'gw_zone', 'use_type']
zone_cols= ['wap', 'NZTMX', 'NZTMY', 'NIWA_catch', 'catchment', 'g_catch_num', 'g_catch', 'catchment_num', 'swaz_grp', 'swaz', 'swaz_num', 'cwms_zone']

## query parameters
cwms_zone = ['Orari-Temuka-Opihi-Pareora']
zone_grp_opihi = ['Opihi']
#zone_grp_orari = ['Orari']
#zone_grp_par = ['Pareora']
allo_col = ['mon_vol']
out1 = 'Rangitata Water Conservation Order'

pareora_swaz_grp = ['Pareora']
minor_swaz_grp = ['Levels and Seadown Plains', 'Saltwater Creek-Otipua', 'Washdyke']
opihi_swaz = ['North Opuha', 'Opihi Rockwood', 'Opihi SH1', 'Opihi Saleyards', 'South Opuha', 'Te Nga Wai']
temuka_swaz = ['Temuka']
Orakipaoa_swaz = ['Orakipaoa']

take_type = ['Take Surface Water']

## output parameters
export_results_path = r'C:\ecan\local\Projects\requests\test\2017-03-03'
export_path = r'C:\ecan\local\Projects\requests\test\2017-03-03\figures'
swaz_path = 'SWAZ'
name4 = 'otop'
name5 = 'otop_past'
name7 = 'minor_swaz'
opihi_name = 'opihi'
opihi_path = 'opihi_swaz'
temuka_name = 'temuka'
Orakipaoa_name = 'Orakipaoa'
pareora_name = 'pareora'

ex_name = 'allo_use_restr_01.png'
ex_name_type = 'tot_allo_type_v01.png'
ex_name_opihi_results = 'opihi_allo_results_v01.csv'
ex_name_opihi_data = 'opihi_allo_data_v01.csv'

## Create directories if needed
if not path.exists(export_results_path):
    makedirs(export_results_path)
if not path.exists(export_path):
    makedirs(export_path)

#################################
### Read in allocation and usage data and merge data

series = read_csv(series_csv)
allo = read_csv(allo_csv)[allo_cols]
sw_zones = read_csv(zones_csv).drop(['FID','take_type'], axis=1)
sw_zones.columns = zone_cols
sw_zones['swaz'] = sw_zones.swaz.str.replace('/', '-')
sw_zones['swaz_grp'] = sw_zones.swaz_grp.str.replace('/', '-')

allo_use1 = merge(series, allo, on=['crc', 'wap'])
allo_use2 = merge(allo_use1, sw_zones, on=['wap'])

### Read in input data to be used in the query

#################################
### Query data

otop1 = allo_query(grp_by=['dates', 'swaz_grp', 'swaz', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, take_type=take_type, export_path=export_results_path, export=False)

index1 = otop1.index.levels[1][~in1d(otop1.index.levels[1], out1)].values
otop2 = otop1.loc[(slice(None), index1, slice(None)), :]

#otop_use_type1 = w_query(allo_use2, grp_by=['dates', 'swaz_grp', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, take_type=take_type, export_path=export_results_path, export=False)
#
#index1 = otop_use_type1.index.levels[1][~in1d(otop_use_type1.index.levels[1], out1)].values
#otop_use_type2 = otop_use_type1.loc[(slice(None), index1, slice(None)), :]
#
#opihi1 = w_query(allo_use2, grp_by=['dates', 'swaz', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, swaz_grp=zone_grp_opihi, take_type=take_type, export_path=path.join(export_results_path, ex_name_opihi_results), export=True)

opihi_data = w_query(allo_use2, grp_by=['dates', 'swaz', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, swaz_grp=zone_grp_opihi, take_type=take_type, export_path=path.join(export_results_path, ex_name_opihi_data), export=True, debug=True)

## Select swazs to be lumped

pareora1 = otop2.loc[(slice(None), pareora_swaz_grp, slice(None), slice(None)), :]
minor1 = otop2.loc[(slice(None), minor_swaz_grp, slice(None), slice(None)), :]
opihi1 = otop2.loc[(slice(None), slice(None), opihi_swaz, slice(None)), :]
temuka1 = otop2.loc[(slice(None), slice(None), temuka_swaz, slice(None)), :]
Orakipaoa1 = otop2.loc[(slice(None), slice(None), Orakipaoa_swaz, slice(None)), :]


################################
#### plot the summaries

### Single sets
## Allo and usage
# All of OTOP
allo_plt(otop2, export_path=export_path, export_name=name4 + '_' + ex_name)

# Minor SWAZs between Opihi and Pareora
allo_plt(minor1, export_path=export_path, export_name=name7 + '_' + ex_name)

# Opihi SWAZs
allo_plt(opihi1, export_path=export_path, export_name=opihi_name + '_' + ex_name)
allo_plt(temuka1, export_path=export_path, export_name=temuka_name + '_' + ex_name)
allo_plt(Orakipaoa1, export_path=export_path, export_name=Orakipaoa_name + '_' + ex_name)

# Pareora
allo_plt(pareora1, export_path=export_path, export_name=pareora_name + '_' + ex_name)

## Use types
# All of OTOP
allo_stacked_plt(otop2, agg_level=[0, 3], export_path=export_path, export_name=name4 + '_' + ex_name_type)

# Minor SWAZs between Opihi and Pareora
allo_stacked_plt(minor1, agg_level=[0, 3], export_path=export_path, export_name=name7 + '_' + ex_name_type)

# Opihi SWAZs
allo_stacked_plt(opihi1, agg_level=[0, 3], export_path=export_path, export_name=opihi_name + '_' + ex_name_type)
allo_stacked_plt(temuka1, agg_level=[0, 3], export_path=export_path, export_name=temuka_name + '_' + ex_name_type)
allo_stacked_plt(Orakipaoa1, agg_level=[0, 3], export_path=export_path, export_name=Orakipaoa_name + '_' + ex_name_type)

# Pareora
allo_stacked_plt(pareora1, agg_level=[0, 3], export_path=export_path, export_name=pareora_name + '_' + ex_name_type)

### Multi-sets
## SWAZ groups allo and usage
#allo_multi_plot(otop_use_type2, agg_level=[0, 1], export_path=export_path, export_name=name4 + '_' + ex_name)
#allo_multi_plot(opihi1, agg_level=[0, 1], export_path=path.join(export_path, opihi_path), export_name=opihi_name + '_' + ex_name)
#
## SWAZ groups totals of allo over long period
#allo_multi_plot(otop_use_type2, agg_level=[0, 1], start='1990', cat=['tot_allo'], export_path=export_path, export_name=name5 + '_' + ex_name)
#
## SWAZ groups stacked by use type
#allo_multi_plot(otop_use_type2, agg_level=[0, 1, 2], index_level=1, plot_fun=allo_stacked_plt, export_path=export_path, export_name=name4 + '_' + ex_name_type)
#allo_multi_plot(opihi1, agg_level=[0, 1, 2], index_level=1, plot_fun=allo_stacked_plt, export_path=path.join(export_path, opihi_path), export_name=opihi_name + '_' + ex_name_type)


#############################
### Check oddities

otop10 = w_query(allo_use2, grp_by=['dates'], allo_col=allo_col, years=[2015], export=False)

otop11 = otop2[otop2.usage_m3.notnull()]

PARENT_B1_ALT_ID='CRC951874.1'

par1 = otop_use_type2.loc(axis=0)[:, 'Pareora']

par1_sum = par1.sum(axis=0, level=0)


otop5 = w_query(allo_use2, grp_by=['dates', 'swaz_grp', 'use_type'], allo_col=allo_col, cwms_zone=cwms_zone, take_type=take_type, export_path=export_results_path, export=False, debug=True)

pareora5 = w_query(allo_use2, grp_by=['dates'], use_type=['irrigation'], years=[2009, 2010], allo_col=allo_col, cwms_zone=cwms_zone, take_type=take_type, swaz_grp=['Pareora'], export_path=export_results_path, export=False, debug=True)


minor1['2015':]
minor1['2015':].sum(axis=0, level=[0,1])
minor1['2015':].sum(axis=0, level=[0])

minor3 = w_query(allo_use2, grp_by=['dates'], use_type=['other'], years=[2015], allo_col=allo_col, cwms_zone=cwms_zone, take_type=take_type, swaz_grp=['Levels and Seadown Plains'], export_path=export_results_path, export=False, debug=True)

opihi2 = w_query(allo_use2, grp_by=['dates', 'swaz', 'use_type'], allo_col=allo_col, years=[2015], cwms_zone=cwms_zone, swaz_grp=zone_grp_opihi, swaz=['South Opuha'], take_type=take_type, export_path=export_results_path, export=False, debug=True)

opihi2[['crc', 'wap', 'ann_allo_m3', 'usage_m3', 'band_num', 'band_restr', 'gauge_num']]


opihi1.xs(key=('2015-06-30', 'Opihi Saleyards'), axis=0, level=[0,1])

swaz_name = 'Orakipaoa'

sum1 = opihi1.xs(key=('2015-06-30', swaz_name), axis=0, level=[0,1]).sum()['tot_ann_allo_m3'].round()

ind1 = (opihi1.xs(key=('2015-06-30', swaz_name), axis=0, level=[0,1])['tot_ann_allo_m3']/sum1).round(2)

print(sum1)
print(ind1)


