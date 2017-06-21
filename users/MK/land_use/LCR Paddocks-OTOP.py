
# coding: utf-8

# In[1]:

#
# Group paddock-scale data from Landcare Crop tables 
# and aggregate to AgriBase farms.
#


# In[10]:

import arcpy
from arcpy import env
import os

import csv
import pandas as pd
import numpy as np

waimak_lcr_folder = r'E:\\GIS\\Temuka_LandUse\\temuka_shapefiles'
env.workspace = waimak_lcr_folder
env.overwriteOutput = True

# for shp in arcpy.ListFeatureClasses():
#     print shp, [f.name for f in arcpy.ListFields(shp)]
#["landuse_sum13-14.shp", "landuse_winter13.shp", "landuse_sum12-13.shp"]

agribase = 'P:\2015_2016\Land\AgriMGM\otop\Data.gdb\primary_parcels_agbase_dissolve'
union_temp_fc = os.path.join(os.path.split(agribase)[0],'agribase_lcr_temuka_paddocks')
print union_temp_fc
shps = arcpy.ListFeatureClasses("*.shp")
print shps

# cat: paddock_id
# summer: using Map_CropGr
# winter: using MapClass


# In[12]:

#
# export data to csv (to do: add a size field)
#
otop_flds = {'summer' : ["cat", "Validity", 'Map_Timing', u'Map_CropGr'],
             'winter' : ["cat", "Validity", "MapClass"]}
for shp in shps:
    print shp
    if shp.find("jun") > -1:
        flds = otop_flds['winter']
    else:
        flds = otop_flds['summer']
    shp_in = os.path.join(env.workspace, shp)
    arcpy.AddField_management(shp_in, "area_ha", "DOUBLE")
    with arcpy.da.UpdateCursor(shp_in, ["area_ha", "SHAPE@AREA"]) as cursor:
        for row in cursor:
            row[0] = row[1] / 10000.0
            cursor.updateRow(row)
    csv_out = os.path.join(env.workspace, os.path.splitext(shp)[0] + ".csv")
    with open(csv_out, "wb") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(flds + ["area_ha"])
        with arcpy.da.SearchCursor(shp_in, flds + ["area_ha"]) as cursor:
            for row in cursor:
                csv_writer.writerow(row)


# In[20]:

# select valid paddocks (Validity == 0)
# and union the resulting layer with agribase data



valid_lcr_paddocks = arcpy.MakeFeatureLayer_management(os.path.join(waimak_lcr_folder, shps[0]),
                                                       "valid_lcr_paddocks_lyr",
                                                       '"Validity" IN (0)')


paddock_farm_id_union = arcpy.Union_analysis([agribase, valid_lcr_paddocks], union_temp_fc)


# In[3]:

# Exoport to dataframe object
# Select = 
# [ ] need to 

fc_np = arcpy.da.FeatureClassToNumPyArray(union_temp_fc, 
                                          ["farm_id", "cat", "SHAPE@AREA"],
                                          "cat > 0 AND farm_id NOT IN ('', ' ')")
lcr_agb = pd.DataFrame(fc_np)


# In[4]:

# assign 1:1 paddocks & farms
# groupby (paddock), rank by area (descending), select first row in each group.
# checked that paddockid > 0 

def ranker(df):
    df['rank'] = np.arange(len(df)) + 1
    return df

lcr_agb = lcr_agb.sort_values("SHAPE@AREA", ascending = False).groupby("cat").apply(ranker)
farms_paddocks = lcr_agb.loc[lcr_agb["rank"] == 1, ["farm_id", "cat"]] 


# In[73]:

# Test case: FARM_ID TI03783
test_csv = 'E:\\GIS\\Temuka_LandUse\\temuka_shapefiles\\temuka_dec13.csv'
crop_groups_csv = 'P:\\2015_2016\\Land\\AgriMGM\\otop\\data\\lcr_crop_groups.csv'

summer_crop = pd.read_csv(test_csv)
summer_crop.head()

crop_groups = pd.read_csv(crop_groups_csv)
summer_crop_groups = crop_groups.loc[crop_groups["season"] == "summer",["field_value", "newCropGroup"]]
summer_crop_groups = summer_crop_groups.rename(columns = {"field_value" : "Map_CropGr"})
# print summer_crop_groups.head()

summer_crop = pd.merge(farms_paddocks, summer_crop, on = 'cat')
summer_crop = pd.merge(summer_crop, summer_crop_groups, on='Map_CropGr')

print (summer_crop.loc[summer_crop['farm_id'] == "TI03783",:]
       .groupby(by=['farm_id', 'newCropGroup'])
       .apply(lambda x: pd.Series({'area_ha' : x['area_ha'].sum()}))
       .round(1))

