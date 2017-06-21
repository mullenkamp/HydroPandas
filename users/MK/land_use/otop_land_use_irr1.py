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

base_path = r'P:\OTOP current state\Irrigation Files'

exist_irr_shp = ['Opihi_Irrigation_Clip.shp', 'Pareora_Irrigation_Clip.shp', 'Seadown_Drain_Irrigation_Clip.shp']
prop_irr_shp = ['Opihi_IrrigibleArea_Clip.shp', 'Pareora_IrrigibleArea_Clip.shp', 'Seadown_Drain_IrrigibleArea_Clip.shp']

new_area_shp = r'C:\ecan\local\Projects\otop\GIS\vector\land_use\Design_Node.shp'

change_area = {'TEKAPO': (590, 0), 'SMART MUNRO / LEVELS VALLEY': (2400, 225), 'ROLLESBY DOWNS': (450, 0), 'MT NESSING': (1250, 0), 'FAIRLIE': (2400, 525), 'CANNINGTON': (1080, 450), 'ASHWICK FLATS': (2400, 1650), 'ALBURY': (1250, 0)}

change_area_df = DataFrame(change_area, index=['prop_area', 'exist_area']).T

land_change_perc = {'TEKAPO': (0, 30, 50, 20, 0), 'SMART MUNRO / LEVELS VALLEY': (55, 20, 10, 10, 5), 'ROLLESBY DOWNS': (0, 30, 50, 20, 0), 'MT NESSING': (55, 20, 10, 10, 5), 'FAIRLIE': (55, 20, 10, 10, 5), 'CANNINGTON': (55, 20, 10, 10, 5), 'ASHWICK FLATS': (70, 15, 10, 5, 0), 'ALBURY': (55, 20, 10, 10, 5)}
land_change_names = ['dairy', 'sheep/beef', 'dairy support/wintering', 'arable/hort', 'deer']

land_change_df = DataFrame(land_change_perc, index=land_change_names).T

export_exist = r'C:\ecan\local\Projects\otop\GIS\vector\land_use\otop_existing_irr_demand_new_v02.shp'
export_prop = r'C:\ecan\local\Projects\otop\GIS\vector\land_use\otop_proposed_irr_demand_new_v02.shp'

########################################
#### Read in data and merge areas

exist_irr = GeoDataFrame()
for i in exist_irr_shp:
    gpd1 = read_file(join(base_path, i))[['geometry']]
    exist_irr = concat([exist_irr, gpd1])
exist_irr.reset_index(drop=True, inplace=True)

prop_irr = GeoDataFrame()
for i in prop_irr_shp:
    gpd1 = read_file(join(base_path, i))[['geometry']]
    prop_irr = concat([prop_irr, gpd1])
prop_irr.reset_index(drop=True, inplace=True)

new_area = read_file(new_area_shp)[['NAME', 'geometry']]
#new_area.set_index('NAME', inplace=True)
#new_area.index.name = 'site'

#######################################
#### Clip irrigation to study area

exist_irr2 = overlay(exist_irr, new_area, how='intersection')
prop_irr2 = overlay(prop_irr, new_area, how='intersection')

prop_irr3 = overlay(prop_irr2, exist_irr2[['geometry']], how='difference')

## Reassign the crs
exist_irr2.crs = exist_irr.crs
prop_irr3.crs = prop_irr.crs

#####################################
#### Select sites within a max change area

exist_sel1 = random_areas(exist_irr2, 'NAME', change_area_df['exist_area'], land_change_df)
prop_sel1 = random_areas(prop_irr3, 'NAME', change_area_df['prop_area'], land_change_df)


########################################
#### Assign land use to each

#
#def assign_land_use(poly, name_col, change_names, perc_dict):
#    from numpy import array
#    from pandas import concat
#
#    loc_names = poly[name_col].unique()
#
#    s1 = Series()
#    for j in loc_names:
#        prob1 = array(perc_dict[j])/100.0
#        index = poly[poly[name_col] == j].index
#
#        g1 = random.choice(change_names, len(index), p=prob1)
#        s1 = s1.append(Series(g1, index=index))
#
#    s1.name = 'land_use'
#    out1 = concat([poly, s1], axis=1)
#    return(out1)
#
#
#exist_sel2 = assign_land_use(exist_sel1, 'NAME', land_change_names, land_change_perc)
#prop_sel2 = assign_land_use(prop_sel1, 'NAME', land_change_names, land_change_perc)


#########################################
#### Export

exist_sel1.to_file(export_exist)
prop_sel1.to_file(export_prop)



#######################################
#### Testing

set1 = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

s1 = array(land_change_perc['FAIRLIE'])/100.0
random.choice(land_change_name, 1, replace=False, p=s1)

index1 = exist_irr2.index.tolist()
random.shuffle(index1)

exist_irr_area.iloc[index1]



grp = exist_irr_area2.groupby('NAME')['cumsum']


prop_irr3.crs = new_area.crs
prop_irr3.to_file(r'C:\ecan\local\Projects\otop\GIS\vector\land_use\prop_test1.shp')

prop_irr.to_file(r'C:\ecan\local\Projects\otop\GIS\vector\land_use\otop_irrigible_area.shp')
exist_irr.to_file(r'C:\ecan\local\Projects\otop\GIS\vector\land_use\otop_irrigated_area.shp')

prop_irr3.to_file(r'C:\ecan\local\Projects\otop\GIS\vector\land_use\study_irrigible_area.shp')
exist_irr2.to_file(r'C:\ecan\local\Projects\otop\GIS\vector\land_use\study_irrigated_area.shp')


change_area_df['prop_area'].to_csv(r'C:\ecan\local\Projects\otop\report\land_use_tool\new_irr_area.csv', header=True)

land_change_df.to_csv(r'C:\ecan\local\Projects\otop\report\land_use_tool\new_land_use.csv', header=True)





















