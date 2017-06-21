# -*- coding: utf-8 -*-
"""
Script example to delineate catchments based on site locations.
"""

from spatial_fun import grid_interp_iter, catch_net, pts_poly_join, grid_catch_agg, flow_sites_to_shp, agg_catch
from geopandas import read_file

###################################
#### Parameters

allo_loc_shp = r'C:\ecan\shared\GIS_base\vector\allocations\allo_loc.shp'
catch_del_shp = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\catch_sites_join.shp'
catch_sites_csv = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\results\catch_sites.csv'
export_sites_shp = 'C:/ecan/local/Projects/otop/GIS/vector/malf_check/flow_sites.shp'
crc_col = 'crc'

################################
#### First define the necessary sites for the delineation
#### This can come from anywhere as long as they are int flow sites

sites_all = [46, 66, 1698, 70105]

## Convert sites to shapefile
sites_geo = flow_sites_to_shp(sites_all, export=True, export_path=export_sites_shp)

###############################
#### The run the arcgis catchment delineation script in arcgis
#### Use the export from sites_geo as the sites_in path and use a polygon shpaefile that defines the desired boundary
#### Define the output working directory and run the script through!

#### Make sure to check that the sites are in the correct location!!!
#### check that after the processing that the delineations make sense!!!

##############################
#### Then aggregate the catchments to get all of the upstream area for each location

catch_del = agg_catch(catch_del_shp, catch_sites_csv)

#############################
#### The result of the last function can be used to extract many types of point data
#### For example to return all abstractions within each catchment

allo_loc = read_file(allo_loc_shp)[[crc_col, 'geometry']]
crc_catch2, catch4 = pts_poly_join(allo_loc, catch_del, 'site', pts_id_col=crc_col)


























