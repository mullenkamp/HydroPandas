# -*- coding: utf-8 -*-
"""
Created on Tue Jul 05 10:49:10 2016

@author: MichaelEK
"""

from pandas import concat, merge, read_csv

#######################
#### ArcGIS
"""
The landuse and farm id shapefiles must be spatially joined in ArcGIS for the
input to the script. The 'spatial join' tool with the 'one_to_one' and
'Have_center_in' parameters.
"""

#######################
#### Parameters

crop_grp_csv = 'C:/ecan/Projects/otop/GIS/tables/lcr_crop_groups.csv'

summer_csv = 'C:/ecan/Projects/otop/GIS/tables/temuka_land_use_farm_id_summer2.txt'
winter_csv = 'C:/ecan/Projects/otop/GIS/tables/temuka_land_use_farm_id_winter2.txt'

#export_summer_old_csv = 'C:/ecan/Projects/otop/GIS/tables/temuka_crop_farm_summer_old.csv'
#export_summer_new_csv = 'C:/ecan/Projects/otop/GIS/tables/temuka_crop_farm_summer_new.csv'

#export_winter_old_csv = 'C:/ecan/Projects/otop/GIS/tables/temuka_crop_farm_winter_old.csv'
#export_winter_new_csv = 'C:/ecan/Projects/otop/GIS/tables/temuka_crop_farm_winter_new.csv'

export_all_csv = 'C:/ecan/Projects/otop/GIS/tables/temuka_crop_farm_all.csv'

######################
#### Analysis

### Processes new crop groups

crop_grp = read_csv(crop_grp_csv)

summer_crop = crop_grp[crop_grp.season == 'summer']
winter_crop = crop_grp[crop_grp.season == 'winter']

### summer
## Read data and rearrange

data = read_csv(summer_csv)

data.loc[data.farm_id.isnull(), 'farm_id'] = 'missing'
data.loc[data.Map_CropGr == ' ', 'Map_CropGr'] = 'missing'

## Merge with new crop groups

data2 = merge(data, summer_crop[['field_value', 'newCropGroup']], left_on='Map_CropGr', right_on='field_value', how='left')

data2.loc[data2.newCropGroup == ' ', 'newCropGroup'] = 'missing'
data2.loc[data2.newCropGroup.isnull(), 'newCropGroup'] = 'missing'

## Aggregate

#data2_grp1 = data2.groupby(['farm_id', 'Map_CropGr'])
data2_grp2 = data2.groupby(['farm_id', 'newCropGroup'])

#data3_old = data2_grp1['area_m2'].sum().reset_index()
data3_new = data2_grp2['area_m2'].sum().reset_index()

#data4_old = data3_old.pivot('farm_id', 'Map_CropGr', 'area_m2')
data4_new = data3_new.pivot('farm_id', 'newCropGroup', 'area_m2')

#data4_old.to_csv(export_summer_old_csv)
#data4_new.to_csv(export_summer_new_csv)

summer_data = data4_new

### winter
## Read data and rearrange

data = read_csv(winter_csv)

data.loc[data.farm_id.isnull(), 'farm_id'] = 'missing'
data.loc[data.MapClass == ' ', 'MapClass'] = 'missing'

## Merge with new crop groups

data2 = merge(data, winter_crop[['field_value', 'newCropGroup']], left_on='MapClass', right_on='field_value', how='left')

data2.loc[data2.newCropGroup == ' ', 'newCropGroup'] = 'missing'
data2.loc[data2.newCropGroup.isnull(), 'newCropGroup'] = 'missing'

## Aggregate

#data2_grp1 = data2.groupby(['farm_id', 'MapClass'])
data2_grp2 = data2.groupby(['farm_id', 'newCropGroup'])

#data3_old = data2_grp1['area_m2'].sum().reset_index()
data3_new = data2_grp2['area_m2'].sum().reset_index()

#data4_old = data3_old.pivot('farm_id', 'MapClass', 'area_m2')
data4_new = data3_new.pivot('farm_id', 'newCropGroup', 'area_m2')

#data4_old.to_csv(export_winter_old_csv)
#data4_new.to_csv(export_winter_new_csv)

winter_data = data4_new

### Combine seasons and convert to hectares

data5 = concat([summer_data, winter_data], axis=1)
summer_labels = [i + '_summer' for i in summer_data.columns.tolist()]
winter_labels = [i + '_winter' for i in winter_data.columns.tolist()]
summer_labels.extend(winter_labels)
data5.columns = summer_labels

data6 = (data5 * 0.0001).round(2)
data6.to_csv(export_all_csv)

#################################
#### Test farm id

id1 = 'TI03783'

data7 = data6.reset_index()
data7[data7.farm_id == id1]

summer_data2 = summer_data.reset_index()

summer_data2[summer_data2.farm_id == id1]



