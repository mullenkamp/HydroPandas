# -*- coding: utf-8 -*-
"""
Script to adjust the leaching rate of specific polygons within otop.
"""

from geopandas import read_file
from os.path import join
from pandas import DataFrame, concat

############################################
### Parameters

base_path = r'E:\ecan\local\Projects\otop\GIS\vector\leaching_land_use'
gw_areas_shp = 'GW_provinces.shp'
no3_leach_shp = 'mgm_otop_nloss_25July_wdrain.shp'

no3_fac = 0.1
gw_areas_col = 'GW_Area'
no3_conc_col = 'Conc_mgL'

red_dict = {'Ashwick Flat': [91], 'Opihi': [68], 'Rangitata Orton': [47]}

export_shp = 'mgm_otop_nloss_25July_wdrain_reduced2.shp'

###########################################
### Function

## Read data
gw_areas = read_file(join(base_path, gw_areas_shp))[[gw_areas_col, 'geometry']]
gw_areas.set_index(gw_areas_col, inplace=True)
no3_leach = read_file(join(base_path, no3_leach_shp))
red_df = DataFrame(red_dict).T * 0.01
red_df.columns = ['reduction']

## Adjust no3 leaching conc / calc conc
no3_leach.loc[:, no3_conc_col] = (no3_leach['nload_kgha'] / no3_leach['drain_mm'] * 100).round(3)

#no3_leach.loc[:, no3_conc_col] = no3_leach.loc[:, no3_conc_col] * no3_fac

## select required gw areas
gw_areas2 = gw_areas[gw_areas.index.isin(red_dict.keys())]
gw_areas3 = concat([gw_areas2, red_df], axis=1)

## Apply reductions

for i in gw_areas3.index:
    sel1 = gw_areas3.loc[i, :]
    no3_leach.loc[no3_leach.within(sel1['geometry']), no3_conc_col] = no3_leach.loc[no3_leach.within(sel1['geometry']), no3_conc_col] * sel1['reduction']

no3_leach.loc[:, no3_conc_col] = no3_leach.loc[:, no3_conc_col].round(3)

## Export
no3_leach.to_file(join(base_path, export_shp))








################################################
### Testing

nload_kgha = 7.46
ha_m2 = 0.0001
drain_mm = 259
mm_m = 0.001
area_m2 = 1371









































































