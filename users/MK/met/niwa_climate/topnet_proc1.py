# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 11:38:19 2017

@author: MichaelEK
"""

from xarray import open_dataset, open_mfdataset
from os.path import join
from core.misc import rd_dir
from geopandas import read_file
from core.ecan_io.met import nc_add_gis
from pandas import to_datetime

##############################################
#### Parameters

base_dir = r'I:\niwa_data\topnet\waimak\RCPpast\BCC-CSM1.1'
nc1 = 'streamq_daily_average_1972010200_1985123100_utc_topnet_13036997_strahler3-R1.nc'

nc2 = r'I:\niwa_data\topnet\waimak\RCP2.6\BCC-CSM1.1\streamq_daily_average_2011010100_2020123100_utc_topnet_13036997_strahler3-Q1.nc'

nc3 = r'I:\niwa_data\climate_projections\RCP2.6\BCC-CSM1.1\TotalPrecipCorr_VCSN_BCC-CSM1.1_RCP2.6_2006_2120_south-island_p05_daily_ECan.nc'

rec_shp = r'E:\ecan\shared\GIS_base\vector\MFE_REC_rivers_order_3.shp'
export_shp1 = r'E:\ecan\shared\projects\climate_change\GIS\vector\waimak_rec1.shp'

##############################################
#### Read data

rec1 = read_file(rec_shp)

ds1 = open_mfdataset(join(base_dir, nc1))
ds2 = open_mfdataset(nc2)
ds2.close()

ds1.close()

rchid = ds1.rchid.to_dataframe().rchid


rec2 = rec1[rec1.NZREACH.isin(rchid)]

rec2.to_file(export_shp1)

ds3 = open_dataset(nc3)
ds3.close()


x_coord = 'longitude'
y_coord = 'latitude'

nc_add_gis(nc3, x_coord, y_coord)



ds1['nrch'] = ds1['rchid']
ds2 = ds1.drop(['basin_lat', 'basin_lon', 'end_lat', 'end_lon'])

flow1 = ds2['river_flow_rate_mod'].copy()

flow2 = flow1.sel(nrch=13036997).to_dataframe()

flow3 = flow1.to_dataframe()

flow3.groupby(level='nrch').sum()['river_flow_rate_mod'].sort_values()

### How to convert date and time to just the date
time1 = flow1.time.data
time2 = to_datetime(to_datetime(time1).date)

flow1['time'] = time2.values























































