# -*- coding: utf-8 -*-
"""
Script to process geo attribute data.
"""
from pandas import DataFrame, Series, DatetimeIndex, to_datetime, MultiIndex, concat, to_numeric
from numpy import array, ndarray, in1d, unique, append, nan, argmax, where, dtype
from geopandas import GeoDataFrame, GeoSeries, read_file
from core.ecan_io import rd_sql
from core.spatial.vector import xy_to_gpd, sel_sites_poly, pts_sql_join

export_data = r'C:\ecan\local\Projects\python\hydro_class\site_geo_attr.csv'

swaz = 'swaz_gis'
gwaz = 'gwaz_gis'
catch = 'catch_gis'
cwms = 'cwms_gis'
sub_reg = 'sub_region_gis'
naz = 'naz_gis'

all_gis = [swaz, gwaz, catch, cwms, sub_reg, naz]

site_geo = rd_sql('SQL2012PROD05', 'Bgauging', 'RSITES', col_names=['SiteNumber', 'NZTMX', 'NZTMY'])
site_geo.columns = ['site', 'NZTMX', 'NZTMY']
site_geo.loc[:, 'site'] = to_numeric(site_geo.loc[:, 'site'], errors='ignore')
site_geo2 = xy_to_gpd(site_geo, ['site'], 'NZTMX', 'NZTMY')
site_geo3 = site_geo2.loc[site_geo2.site > 0, :]
site_geo3.loc[:, 'site'] = site_geo3.loc[:, 'site'].astype('int32')

site_geo4 = pts_sql_join(site_geo3, all_gis)
site_geo5 = site_geo4.drop('geometry', axis=1)
site_geo5.loc[:, 'niwa_catch'] = to_numeric(site_geo5.loc[:, 'niwa_catch'], errors='coerce')

#bad_wells = ['BX24/1392', 'BX24/1393', 'BX24/1394', 'BX24/1395', 'BX24/1396', 'BX24/1397', 'BX24/1398', 'BX24/1399', 'N37/0003', 'BU18/0001']

site_geo = rd_sql('SQL2012PROD05', 'Wells', 'WELL_DETAILS', ['WELL_NO',  'NZTMX', 'NZTMY'])
site_geo.rename(columns={'WELL_NO': 'site'}, inplace=True)

index1 = (site_geo.NZTMX > 1300000) & (site_geo.NZTMX < 1700000) & (site_geo.NZTMY > 5000000) & (site_geo.NZTMY < 5400000)
site_geo0 = site_geo[index1]

site_geo2 = xy_to_gpd(site_geo0, ['site'], 'NZTMX', 'NZTMY')
site_geo2.loc[:, 'site'] = to_numeric(site_geo2.loc[:, 'site'], errors='ignore')
#site_geo3 = site_geo2[~in1d(site_geo2.site, bad_wells)]

waps_geo = pts_sql_join(site_geo2, all_gis)
waps_geo1 = waps_geo.drop('geometry', axis=1)
waps_geo1.loc[:, 'niwa_catch'] = to_numeric(waps_geo1.loc[:, 'niwa_catch'], errors='coerce')

all_geo = concat([site_geo5, waps_geo1])
all_geo.to_csv(export_data, index=False, encoding='utf-8')






