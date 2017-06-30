# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 10:42:42 2017

@author: MichaelEK
"""

from core.ecan_io import rd_niwa_vcsn, rd_sql
from core.ts.met.interp import sel_interp_agg


#############################################
### Parameters

server1 = 'SQL2012PROD05'
db1 = 'GIS'
tab_irr = 'AQUALINC_NZTM_IRRIGATED_AREA_20160629'
tab_paw = 'vLANDCARE_NZTM_Smap_Consents'

irr_cols = ['type']
irr_cols_rename = {'type': 'irr_type'}
paw_cols = ['WeightAvgPAW']
paw_cols_rename = {'WeightAvgPAW': 'paw'}

bound_shp = r'S:\Surface Water\backups\MichaelE\Projects\requests\waimak\2017-06-12\waimak_area.shp'

output_precip = r'S:\Surface Water\shared\projects\lsrm\testing\precip_resample1.nc'


###########################################
### Extract data

irr1 = rd_sql(server1, db1, tab_irr, irr_cols, geo_col=True)
irr1.rename(columns=irr_cols_rename, inplace=True)

paw1 = rd_sql(server1, db1, tab_paw, paw_cols, geo_col=True)
paw1.rename(columns=paw_cols_rename, inplace=True)

precip_et = rd_niwa_vcsn(['precip', 'PET'], bound_shp)

##########################################
### Resample data

new_rain = sel_interp_agg(precip_et, 4326, bound_shp, 1000, 'rain', 'time', 'x', 'y', agg_ts_fun='sum', period='W', output_path=output_precip)
















##########################################
### Testing

precip_crs = 4326
grid_res = 1000
data_col = 'rain'
time_col = 'time'
x_col = 'x'
y_col = 'y'


precip = precip_et.copy()
poly = bound_shp
buffer_dis=10000
interp_fun='multiquadric'
agg_ts_fun='sum'
period='W'
digits=3
agg_xy=False
output_format=None


df = precip2.copy()
from_crs=sites.crs
to_crs=poly_crs1

























