# -*- coding: utf-8 -*-
"""
Modelling change in irrigation in upper OTOP.
"""

from numpy import random, array, cumsum, abs, min, nan
from geopandas import read_file, GeoDataFrame, overlay
from os.path import join
from pandas import concat, Series, DataFrame
from core.spatial import random_areas


########################################
#### Parameters

base_path = r'P:\OTOP_scenarios\irrigation_schemes\opihi'

exist_irr_shp = 'Opihi_demand_survey_New_irrigation.shp'
prop_irr_shp = 'Opihi_demand_survey_New_irrigation.shp'
#comm_area_shp = 'pareora_comm_area.shp'
#comm_area_name_col = 'Catchmen_1'

change_area = {'opihi': (1826, 1826)}

land_change_perc = {'opihi': (70, 10, 10, 10, 0)}
land_change_names = ['dairy', 'sheep/beef', 'dairy support/wintering', 'arable/hort', 'deer']

export_exist = 'opihi_proposed_irr_demand_new.shp'
#export_prop = 'pareora_proposed_irr_demand_new.shp'

#######################################
#### Prep tables

change_area_df = DataFrame(change_area, index=['prop_area', 'exist_area']).T
land_change_df = DataFrame(land_change_perc, index=land_change_names).T


########################################
#### Read in data and merge areas

exist_irr = read_file(join(base_path, exist_irr_shp))[['geometry']]
#prop_irr = read_file(join(base_path, prop_irr_shp))[['geometry']]
#new_area = read_file(join(base_path, comm_area_shp))[[comm_area_name_col, 'geometry']]
exist_irr['name'] = 'opihi'

#### Remove errors in the shapefile
#t2 = exist_irr.simplify(1, False)
#t3 = t2[~t2.isnull()]
#exist_irr = GeoDataFrame(geometry=t3)

#######################################
#### Clip irrigation to study area

#exist_irr2 = overlay(exist_irr, new_area, how='intersection')
#prop_irr2 = overlay(prop_irr, new_area, how='intersection')
#
#exist_irr2.plot()
#prop_irr2.plot()
#
### Fix an odd problem
##prop_irr2 = prop_irr2.drop(5, axis=0)
#
### remove already irrigated areas
#prop_irr3 = overlay(prop_irr2, exist_irr2[['geometry']], how='difference')
#prop_irr3.plot()
#
### Reassign the crs
#exist_irr2.crs = exist_irr.crs
#prop_irr3.crs = prop_irr.crs

#####################################
#### Select sites within a max change area

exist_sel1 = random_areas(exist_irr, 'name', change_area_df['exist_area'], land_change_df, tol=0.05)
#prop_sel1 = random_areas(prop_irr3, comm_area_name_col, change_area_df['prop_area'], land_change_df, tol=0.05)


#########################################
#### Export

exist_sel1.to_file(join(base_path, export_exist))
#prop_sel1.to_file(join(base_path, export_prop))


#######################################
#### Testing

t2 = exist_irr.simplify(1, False)
t3 = t2[~t2.isnull()]
t4 = GeoDataFrame(geometry=t3)
exist_irr2 = overlay(t4, new_area, how='intersection')
















