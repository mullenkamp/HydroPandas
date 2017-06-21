# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 15:17:37 2016

@author: MichaelEK
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from misc_fun import printf
from allo_use_fun import stream_nat
from stats_fun import lin_reg
from import_fun import rd_hydstra_csv, flow_import, rd_ts
from geopandas import read_file


############################################
#### Parameters
flow_csv = 'C:/ecan/shared/projects/waimak/data/waimak_data.csv'

end = '2015-05-30'
catch_sites_csv = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/results/catch_sites.csv'
catch_shp = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/catch2.shp'

#norm_area = True

export_rec_shp_path = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/rec_nat_sites1.shp'
export_gauge_shp_path = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/gauge_nat_sites1.shp'
export_shp_path = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/nat_sites1.shp'
export_rec_flow_path = 'C:/ecan/shared/projects/waimak/data/waimak_flow_nat.csv'
export_gauge_flow_path = 'C:/ecan/shared/projects/waimak/data/waimak_gauge_flow_nat.csv'

###########################################
#### Load in data and create shapefile

data = rd_ts(flow_csv)
data.columns = data.columns.astype(int)

rec, gauge = flow_import(rec_sites=data.columns.values, gauge_sites=data.columns.values, end=end, export_shp=True,  export_rec_shp_path=export_rec_shp_path, export_gauge_shp_path=export_gauge_shp_path)

rec_shp = read_file(export_rec_shp_path)
gauge_shp = read_file(export_gauge_shp_path)




###########################################
#### Run stream naturalization

#flow_norm, gaugings_norm, nat_flow_norm, nat_gauge_norm = stream_nat(catch_shp, catch_sites_csv, norm_area=norm_area)

flow, gaugings, nat_flow, nat_gauge = stream_nat(catch_shp, catch_sites_csv, flow_csv=flow_csv, export=True, export_rec_flow_path=export_rec_flow_path, export_gauge_flow_path=export_gauge_flow_path)


#########################################
#### Testing
site = 343
#site2 = 66201

nat_flow[site].dropna()
flow[site].dropna()


#### Linear reg
t1 = lin_reg(nat_gauge, nat_flow)
t2 = [i[i['n-obs'] > 5] for i in t1]
t3 = [i for i in t2 if not i.empty]

t4 = DataFrame(i[i.NRMSE == min(i.NRMSE)].values.flatten() for i in t3)
t4.columns = t3[0].columns


























########################################
### Parameters

sd_path = 'C:/ecan/base_data/usage/sd_est_all_mon_vol.csv'
flow_path = 'C:/ecan/Projects/otop/flow/otop_flow_recorders.CSV'
wap_gauge_path = 'C:/ecan/Projects/otop/GIS/tables/otop_wap_gauge_catch.csv'
gauge_path = 'C:/ecan/Projects/otop/GIS/tables/otop_min_flow_gauges.csv'
gauge_links_path = 'C:/ecan/Projects/otop/GIS/tables/gauge_catch_links.csv'

wap_gauge_cols = ['wap', 'GRIDCODE']
gauge_col = 'SITENUMBER'

export = True

export_rec_flow_path = 'C:/ecan/Projects/otop/flow/rec_flow_nat.csv'
export_gauge_flow_path = 'C:/ecan/Projects/otop/flow/gauge_flow_nat.csv'

########################################
### Naturalize specific gauging sites

flow, gauge, flow_nat, gauge_nat = stream_nat(sd_path, wap_gauge_path, gauge_links_path, flow_path, gauge_path, export_rec_flow_path=export_rec_flow_path, export_gauge_flow_path=export_gauge_flow_path)

#######################################
### Testing section

if ("NIWACatchm" is NULL, NULL, to_int(floor("NIWACatchm" )))
if ( "CatchmentG" is NULL, "niwa_catch", "CatchmentG" )

Con(IsNull("str_raster3"), "Canterbury 8m DEM", "Canterbury 8m DEM" - "str_raster3")

plt1 = regplot(sel1['et'], sel1['irrigation'])
plt2 = regplot(sel1['et'], sel1['other'])

usage_path = 'C:/ecan/base_data/usage/usage_takes_mon_series_all_est_SD_with_cav.csv'

t1 = read_csv(usage_path)

len(t1.wap.unique())

tot_use2.wap.unique()
tot_use3.wap.unique()

sum(sd_et_est1.sd_usage_est.isnull())
sum(sd_est_all_mon_vol.sd_usage_est.isnull())

t1 = wap_gauge[wap_gauge.site.isin([70103])].wap.unique()
t2 = sd1[sd1.wap.isin(t1)]


max_y = 50

site = 69607
site = 70103
site = 70105
site = 69618
site = 69602

t1 = flow[site].dropna()
d1 = flow_nat[site].dropna()

d1.plot(ylim=[0, max_y])
t1.plot(ylim=[0, max_y])



