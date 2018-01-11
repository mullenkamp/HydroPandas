# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 08:35:21 2018

@author: MichaelEK
"""
import os
import geopandas as gpd
import shapely
from hydropandas.tools.river.spatial.catch_del import rec_catch_del

###################################
#### Parameters
rec_catch_shp = r'\\fs02\ManagedShares2\Data\Surface Water\shared\GIS_base\vector\catchments\river-environment-classification-watershed-canterbury-2010.shp'
rec_streams_shp = r'\\fs02\ManagedShares2\Data\Surface Water\shared\GIS_base\vector\streams\rec-canterbury-2010.shp'

#rec_catch_shp = {'server': 'SQL2012PROD05', 'database': 'GIS', 'table': 'MFE_NZTM_RECWATERSHEDCANTERBURY', 'col_names': ['NZREACH'], 'geo_col': True}
#rec_streams_shp = {'server': 'SQL2012PROD05', 'database': 'GIS', 'table': 'MFE_NZTM_REC', 'col_names': ['NZREACH', 'NZFNODE', 'NZTNODE'], 'geo_col': True}

base_dir = r'E:\ecan\git\HydroPandas\hydropandas\tests'
sites_shp = 'sites.shp'

catch_del_shp = 'catch_del.shp'

###############################
#### Catchment delination from the REC streams layers


def test_catch_del():
    catch1 = rec_catch_del(sites_shp=os.path.join(base_dir, sites_shp), rec_streams_shp=rec_streams_shp, rec_catch_shp=rec_catch_shp,  sites_col='site', catch_output=os.path.join(base_dir, catch_del_shp))
    assert (len(catch1) == 22) & isinstance(catch1, gpd.GeoDataFrame) & isinstance(catch1.geometry[0], shapely.geometry.Polygon)




