# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 13:12:27 2017

@author: MichaelEK

Script to take the vcsn csv files and repackage them into a proper netcdf file equivelant to NIWA's netcdf files.
"""

from pandas import read_csv, concat, merge, to_numeric
from core.misc import rd_dir
from os import path
from numpy import tile, repeat, in1d
from xarray import Dataset, DataArray, open_dataset
from core.ecan_io.met import sel_niwa_vcsn

######################################
#### Parameters

comp_table_csv = 'Y:/VirtualClimate/VCN_id_comp_table.csv'
data_dir = 'Y:/VirtualClimate/VCN_precip_ET_2016-06-06'
vcsn_sites_csv = r'Z:\Data\VirtualClimate\GIS\niwa_vcsn_wgs84.csv'

data_attr = {'grid_mapping': 'crs'}
nc_crs = {'inverse_flattening': 298.257223563, 'longitude_of_prime_meridian': 0, 'semi_major_axis': 6378137, 'grid_mapping_name': 'latitude_longitude'}
time_attr = {'bounds': 'time_bounds', 'standard_name': 'time', 'axis': 'T', 'long_name': 'time (end of reporting period)'}

pe_test_nc = r'I:\niwa_data\climate_projections\RCPpast\BCC-CSM1.1\PE_VCSN_BCC-CSM1.1_RCPpast_1971_2005_south-island_p05_daily_ECan.nc'
rain_test_nc = r'I:\niwa_data\climate_projections\RCPpast\BCC-CSM1.1\TotalPrecipCorr_VCSN_BCC-CSM1.1_RCPpast_1971_2005_south-island_p05_daily_ECan.nc'

save_nc = 'Y:/VirtualClimate/vcsn_precip_et_2016-06-06a.nc'

#####################################
#### Read data

comp_table = read_csv(comp_table_csv)
vcsn_sites = read_csv(vcsn_sites_csv)[['Network', 'deg_x', 'deg_y']]
vcsn_sites.columns = ['site', 'longitude', 'latitude']
files, num = rd_dir(data_dir, 'csv', True)

pe_test = open_dataset(pe_test_nc)
rain_test = open_dataset(rain_test_nc)

####################################
#### Get and organize data

### Attributes
pe_attr = pe_test.pe.attrs
pe_attr.update(data_attr)

rain_attr = rain_test.rain.attrs
rain_attr.update(data_attr)

lon_attr = pe_test.longitude.attrs
lat_attr = pe_test.latitude.attrs
pe_test.close()
rain_test.close()

### Station names
comp_table2 = comp_table.set_index('ecan_id')
sites = comp_table2.loc[num, 'net_id']

### Get the data
df1 = concat((read_csv(path.join(data_dir, f), index_col=0, parse_dates=True, infer_datetime_format=True) for f in files))

n_data = len(df1)/len(sites)

sites2 = repeat(sites.values, n_data)

df2 = df1.copy().reset_index()
df2['site'] = sites2

df3 = merge(df2, vcsn_sites, on='site', how='left')
df3.rename(columns={'date': 'time', 'precip': 'rain', 'ET': 'pe'}, inplace=True)
df4 = df3.set_index(['longitude', 'latitude', 'time'])

df4.loc[:, 'rain'] = to_numeric(df4['rain'], errors='coerce').values

sites_df = df3[['site', 'longitude', 'latitude']].drop_duplicates().set_index(['longitude', 'latitude'])

ds_sites = sites_df.to_xarray()
ds_sites.site.attrs = {'long_name': 'NIWA station names', 'grid_mapping': 'crs'}

## Careful of the RAM!!!
ds1 = df4[['rain', 'pe']].to_xarray()

## Good again
ds2 = ds1.merge(ds_sites)

ds_crs = DataArray(4326, attrs=nc_crs, name='crs').to_dataset()
ds3 = ds2.merge(ds_crs)

## Add new attributes
ds3.latitude.attrs = lat_attr
ds3.longitude.attrs = lon_attr
ds3.time.attrs = time_attr
ds3.rain.attrs = rain_attr
ds3.pe.attrs = pe_attr
ds3.attrs = {'long_name': 'NIWA gridded VCSN potential ET and precipitation'}

## Save data
ds3.to_netcdf(save_nc)



##########################################
#### Testing

t1 = to_numeric(df4['rain'], errors='coerce')

ds4 = ds3.set_coords('site')

sites3 = ds3.site.to_dataframe().reset_index()
sites4 = sites3[sites3.site.isin(['P160113', 'P160112'])]


ds5 = ds3.sel(longitude=sites4.longitude.values, latitude=sites4.latitude.values)
d1 = ds3.site.where(in1d(ds3.site, ['P160113', 'P160112']).reshape(ds3.site.shape))
d1.loc[d1.notnull()]


sites = ['P160113', 'P160112']
nc_path = 'Y:/VirtualClimate/vcsn_precip_et_2016-06-06.nc'


y1 = sel_niwa_vcsn('precip', ['P160113', 'P160112'], include_sites=True, out_crs=2193)

poly = r'E:\ecan\local\Projects\requests\tim_kerr\2017-06-19\study_area.shp'

y1 = sel_niwa_vcsn('precip', poly, include_sites=True, out_crs=2193)
y1 = sel_niwa_vcsn(['precip', 'PET'], poly, include_sites=True, out_crs=2193)
y1 = sel_niwa_vcsn(['precip', 'PET'], poly, include_sites=False, out_crs=2193)
y1 = sel_niwa_vcsn(['precip', 'PET'], poly, include_sites=False)






