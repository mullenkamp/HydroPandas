import os
import geopandas as gpd
from core.spatial import rec_catch_del
from os.path import join
from core.ecan_io import rd_sw_rain_geo
from core.misc import select_sites


###################################
#### Parameters
rec_catch_shp = r'\\fs02\ManagedShares2\Data\Surface Water\shared\GIS_base\vector\catchments\river-environment-classification-watershed-canterbury-2010.shp'
rec_streams_shp = r'\\fs02\ManagedShares2\Data\Surface Water\shared\GIS_base\vector\streams\rec-canterbury-2010.shp'

rec_catch_dict = {'server': 'SQL2012PROD05', 'database': 'GIS', 'table': 'MFE_NZTM_RECWATERSHEDCANTERBURY', 'col_names': ['NZREACH'], 'geo_col': True}
rec_streams_dict = {'server': 'SQL2012PROD05', 'database': 'GIS', 'table': 'MFE_NZTM_REC', 'col_names': ['NZREACH', 'NZFNODE', 'NZTNODE'], 'geo_col': True}

base_dir = r'E:\ecan\shared\projects\climate_change\waimak_gw_model'

sites_csv = 'flow_sites.csv'
sites_shp = 'sites.shp'

#rec_sites_rec_shp = 'recorder_sites_REC.shp'

catch_del_shp = 'catch_del.shp'
#catch_del_temp_shp = r'catch_del_temp.shp'

####################################
#### Create points shapefile and go into GIS and adjust the points if needed

sites = select_sites(join(base_dir, sites_csv)).astype('int32').tolist()
sites_geo = rd_sw_rain_geo(sites).reset_index()
sites_geo.to_file(join(base_dir, sites_shp))

#### MAKE SURE YOU GO INTO GIS AND ADJUST THE SITE POINTS BEFORE THE NEXT STEP!!!

###############################
#### Catchment delination from the REC streams layers

catch1 = rec_catch_del(sites_shp=join(base_dir, sites_shp), sites_col='site', catch_output=join(base_dir, catch_del_shp))



##############################
### Testing

name1 = 'test_rec.geojson'
name2 = 'test_rec.shp'
rec_dir = os.path.split(rec_streams_shp)[0]
new_path1 = os.path.join(rec_dir, name1)
new_path2 = os.path.join(rec_dir, name2)
vfs = 'zip:////fs02/ManagedShares2/Data/Surface Water/shared/GIS_base/vector/streams/streams.zip'
zip1 = 'streams.zip'

%timeit g1 = gpd.read_file(rec_streams_shp)
# 3 s

%timeit g1.to_file(new_path1, driver='GeoJSON')
%timeit g1.to_file(new_path2)

%timeit g2 = gpd.read_file('/' + name2, vfs=vfs)







