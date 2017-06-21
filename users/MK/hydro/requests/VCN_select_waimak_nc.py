# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""

from os import path
from ts_stats_fun import w_resample
from import_fun import rd_nc

######################################
### Parameters

base_path = 'Y:/VirtualClimate/projections/2006-2100'

precip_nc = 'RAW/ECan_nTotalPrecipCorr_VCSN_GFDL-CM3_2006-2100_RCP6.0.nc'
precip_uncorr_nc = 'RAW/ECan_nTotalPrecipUnCorr_VCSN_GFDL-CM3_2006-2100_RCP6.0.nc'

pet_nc = 'RAW/ECan_PE_VCSN_GFDL-CM3_2006-2100_RCP6.0.nc'

poly_shp = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/vcsn_shp/Export_Output.shp'
nc_uncorr_rain_path = path.join(base_path, precip_uncorr_nc)
nc_rain_path = path.join(base_path, precip_nc)

nc_pet_path = path.join(base_path, pet_nc)

export_path_rain = path.join(base_path, 'nc_rain.csv')
export_path_uncorr_rain = path.join(base_path, 'nc_uncorr_rain.csv')

export_path_pet = path.join(base_path, 'nc_pet.csv')


#####################################
### Read in RAW data and organize

rain = rd_nc(poly_shp, nc_rain_path, data_col='rain', export_path=export_path_rain)
rain_uncorr = rd_nc(poly_shp, nc_uncorr_rain_path, data_col='precipitation_amount', x_col='lon', y_col='lat', export_path=export_path_uncorr_rain)

pet = rd_nc(poly_shp, nc_pet_path, data_col='pe', export_path=export_path_pet)







