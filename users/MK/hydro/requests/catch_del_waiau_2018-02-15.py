# -*- coding: utf-8 -*-
"""
Created on Thu Sep 07 08:54:54 2017

@author: MichaelEK

Example script to delineate catchments from the REC streams and catchments.
"""
import os
from hydropandas.tools.river.spatial.catch_del import rec_catch_del
from hydropandas.util.misc import select_sites

###################################
#### Parameters

rec_catch_shp = r'E:\ecan\shared\GIS_base\vector\catchments\river-environment-classification-watershed-canterbury-2010.shp'
rec_streams_shp = r'E:\ecan\shared\GIS_base\vector\streams\river-environment-classification-canterbury-2010.shp'

base_dir = r'E:\ecan\local\Projects\requests\dan\2018-03-02'

sites_shp = 'sites.shp'
catch_del_shp = 'catch_del.shp'

####################################
#### Create points shapefile and go into GIS and adjust the points if needed

#sites = select_sites(join(base_dir, sites_csv)).astype('int32').tolist()
#sites_geo = rd_sw_rain_geo(sites).reset_index()
#sites_geo.to_file(join(base_dir, sites_shp))

#### MAKE SURE YOU GO INTO GIS AND ADJUST THE SITE POINTS BEFORE THE NEXT STEP!!!

###############################
#### Catchment delination from the REC streams layers

catch1 = rec_catch_del(os.path.join(base_dir, sites_shp), rec_streams_shp, rec_catch_shp, sites_col='id', catch_output=os.path.join(base_dir, catch_del_shp))






