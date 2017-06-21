# -*- coding: utf-8 -*-
"""
Script to import and process Hydstra flow data and export it as a common csv file. All available data in Hydstra is processed.
"""

from pandas import merge, read_csv, DataFrame, concat
from import_fun import rd_hydstra_csv, rd_hydstra_dir, rd_henry, rd_hydstra_db, flow_import
from ts_stats_fun import flow_stats, malf7d, ts_comp, fre_accrual
from misc_fun import printf
from hydro_plot_fun import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot

#################################
#### Parameters

#### OTOP min flow sites
### import parameters
#rec_sites = gauge_sites = 'C:/ecan/local/Projects/otop/GIS/vector/otop_catchments.shp'
#end = '2016-09-30'
#
### output parameters
#export_flow=True
#export_stats=True
#export_shp=True
#export_rec_path = 'C:/ecan/shared/base_data/flow/otop/otop_min_flow_rec_data.csv'
#export_stats_path = 'C:/ecan/shared/base_data/flow/otop/otop_min_flow_rec_stats.csv'
#export_gauge_path = 'C:/ecan/shared/base_data/flow/otop/otop_min_flow_gauge_data.csv'
#export_rec_shp_path = 'C:/ecan/local/Projects/otop/GIS/vector/otop_min_flow_rec_loc.shp'
#export_gauge_shp_path = 'C:/ecan/local/Projects/otop/GIS/vector/otop_min_flow_gauge_loc.shp'
#min_flow_only = True

#### OTOP min flow sites
### import parameters
#rec_sites = 'C:/ecan/local/Projects/otop/GIS/vector/otop_catchments.shp'
#gauge_sites = 'None'
#end = '2016-09-30'
#
### output parameters
#export_flow=True
#export_stats=True
#export_shp=True
#export_rec_path = 'C:/ecan/shared/base_data/flow/otop/otop_flow_rec_data.csv'
#export_stats_path = 'C:/ecan/shared/base_data/flow/otop/otop_flow_rec_stats.csv'
#export_rec_shp_path = 'C:/ecan/local/Projects/otop/GIS/vector/otop_flow_rec_loc.shp'
#min_flow_only = False


### All recorder sites in Canterbury
## import parameters
rec_sites = 'All'
gauge_sites = 'None'
end = '2017-02-28'

## output parameters
export_flow=True
export_stats=True
export_shp=True
export_rec_path = 'C:/ecan/shared/base_data/flow/all_flow_rec_data.csv'
export_stats_path = 'C:/ecan/shared/base_data/flow/all_flow_rec_stats.csv'
export_rec_shp_path = 'C:/ecan/shared/GIS_base/vector/hydro_sites/all_rec_loc.shp'
min_flow_only = False
export_rec_long = 'C:/ecan/shared/base_data/flow/all_flow_rec_data_long.csv'


#################################
#### Run special function

### Minimum flow sites for OTOP
#rec_flow, gauge_flow = flow_import(rec_sites=rec_sites, gauge_sites=gauge_sites, min_flow_only=min_flow_only, end=end, export_flow=True, export_stats=True, export_shp=True, export_rec_path=export_rec_path, export_gauge_path=export_gauge_path, export_stats_path=export_stats_path, export_rec_shp_path=export_rec_shp_path, export_gauge_shp_path=export_gauge_shp_path)

### All recorder sites in OTOP
#rec_flow = flow_import(rec_sites=rec_sites, gauge_sites=gauge_sites, min_flow_only=min_flow_only, end=end, export_flow=True, export_stats=True, export_shp=True, export_rec_path=export_rec_path, export_stats_path=export_stats_path, export_rec_shp_path=export_rec_shp_path)

### All recorder sites in Canterbury
rec_flow = flow_import(rec_sites=rec_sites, gauge_sites=gauge_sites, min_flow_only=min_flow_only, end=end, export_flow=True, export_stats=True, export_shp=True, export_rec_path=export_rec_path, export_stats_path=export_stats_path, export_rec_shp_path=export_rec_shp_path)


## Reform
flow2 = rec_flow.stack().reset_index()

#flow2['mtype'] = 'flow'
flow2.columns = ['time', 'site', 'data']

flow3 = flow2[['site', 'time', 'data']].sort_values(['site', 'time'])
flow3.to_csv(export_rec_long, index=False)



