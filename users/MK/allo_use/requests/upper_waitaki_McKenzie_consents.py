# -*- coding: utf-8 -*-
"""
Created on Tue Oct 04 11:06:18 2016

@author: MichaelEK
"""

from import_fun import rd_sql
from spatial_fun import sel_sites_poly, xy_to_gpd
from geopandas import read_file
from pandas import merge, to_datetime
from numpy import in1d

#######################################
#### Parameters

wap_fields = ['PARENT_B1_ALT_ID', 'WellSwapNo', 'Allocation (l/s)', 'B1_PER_CATEGORY']
wap_loc_fields = ['WELL_NO', 'NZTMX', 'NZTMY']
dates_fields = ['B1_ALT_ID', 'fmDate', 'toDate', 'OriginalLodgement']
takes_fields = ['PARENT_B1_ALT_ID', 'Activity', 'Max Rate (l/s)', 'Water Use (Detail)']

wap_names = ['crc', 'wap', 'max_rate_wap', 'take_type']
wap_loc_names = ['wap', 'x', 'y']
dates_names = ['crc', 'from', 'to', 'app_date']
takes_names = ['crc', 'take_type', 'max_rate_crc', 'use_type']


# crc and WAPs with associated allocated rates

server3 = 'SQL2012PROD03'
database3 = 'DataWarehouse'

wap_table = 'DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterWellsSwaps'

# crc with start and end dates

server4 = 'SQL2012PROD03'
database4 = 'DataWarehouse'

dates_table = 'DataWarehouse.dbo.F_ACC_Permit'

# x and y location for all WAPs

server6 = 'SQL2012PROD05'
database6 = 'Wells'

wap_loc_table = 'Wells.dbo.WELL_DETAILS'

# GW/SW takes with catchment info

server5 = 'SQL2012PROD03'
database5 = 'DataWarehouse'

takes_table = 'DataWarehouse.dbo.D_ACC_ActivityAttribute_TakeWaterCombined'

#### Shapefiles

cwms_shp = 'C:\\ecan\\shared\\GIS_base\\vector\\cwms_zones.shp'
cwms_col = 'ZONE_NAME'
cwms_val = 'Upper Waitaki'
district_shp = 'C:\\ecan\\shared\\GIS_base\\vector\\regional_districts.shp'
district_col = 'TA2013_NAM'
district_val = 'Mackenzie District'

#### Other parameters

start_date = '2003-01-01'
export_path = 'C:\\ecan\\local\\Projects\\requests\\upper_waitaki_mackenzie\\crc_wap_query.csv'

###########################################
#### Load data

takes = rd_sql(server5, database5, takes_table, takes_fields)
wap = rd_sql(server3, database3, wap_table, wap_fields)
dates = rd_sql(server4, database4, dates_table, dates_fields)
wap_loc = rd_sql(server6, database6, wap_loc_table, wap_loc_fields)

takes.columns = takes_names
wap.columns = wap_names
dates.columns = dates_names
wap_loc.columns = wap_loc_names

cwms = read_file(cwms_shp)[[cwms_col, 'geometry']]
district = read_file(district_shp)[[district_col, 'geometry']]

##########################################
#### Extract WAPs in study area

uw_cwms = cwms[cwms[cwms_col] == cwms_val]
mac_district = district[district[district_col] == district_val]

wap_loc2 = xy_to_gpd(wap_loc, 'wap', 'x', 'y')

uw_waps = sel_sites_poly(uw_cwms, wap_loc2)
uw_mac_waps = sel_sites_poly(mac_district, uw_waps)['wap']

#### Table joins!

crc_wap1 = wap[in1d(wap.wap, uw_mac_waps.values)]
crc_wap2 = merge(crc_wap1, takes, on=['crc', 'take_type'])

crc_wap3 = merge(crc_wap2, dates, on='crc').drop_duplicates(subset=['crc', 'wap', 'take_type'])


#### Select crc's after 2003

crc_wap3['from'] = to_datetime(crc_wap3['from'])
crc_wap3['to'] = to_datetime(crc_wap3['to'])

crc_wap4 = crc_wap3[crc_wap3['from'] >= start_date]

###############################################
#### Save data

crc_wap4.to_csv(export_path, index=False)




















