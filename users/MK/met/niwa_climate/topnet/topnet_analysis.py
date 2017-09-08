# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 09:23:46 2017

@author: MichaelEK
"""

from xarray import open_dataset, open_mfdataset
from os import path
from core.misc import rd_dir, unarchive_dir, save_df, get_subdir
from geopandas import read_file
from core.ecan_io.met import nc_add_gis
from pandas import to_datetime, DataFrame, concat, read_hdf, merge
from core.ts.met.topnet import proc_topnet_nc
from core.spatial.vector import closest_line_to_pts
from core.ts import grp_ts_agg

#############################################
#### Parameters

base_path = r'E:\ecan\shared\projects\climate_change\waimak_gw_model'

rcppast_hdf = 'rcppast.h5'
rcpproj_hdf = 'rcpproj.h5'
sites_rec_shp = 'sites_rec.shp'

#############################################
#### Read in data

past1 = read_hdf(path.join(base_path, rcppast_hdf))
proj1 = read_hdf(path.join(base_path, rcpproj_hdf))
sites = read_file(path.join(base_path, sites_rec_shp))

#############################################
#### Resample data

past2 = concat([past1 for i in range(4)], axis=1)
past2.columns = proj1.columns
combo1 = concat([past2.reset_index(), proj1.reset_index()])

grp1 = grp_ts_agg(proj1.reset_index(), 'nrch', 'time', '10A-JUN')
proj_mean = grp1.mean()
proj_count = grp1.count()
proj_mean1 = proj_mean[proj_count > 3650].dropna()

past2 = past1.loc['1975-01-01':]
grp2 = grp_ts_agg(past2.reset_index(), 'nrch', 'time', '10A-JUN')
past_mean = grp2.mean()
past_count = grp2.count()
past_mean1 = past_mean[past_count > 3650].dropna()
past_mean2 = concat([past_mean1 for i in range(4)], axis=1)
past_mean2.columns = proj_mean1.columns

combo_mean1 = concat([past_mean2.reset_index(), proj_mean1.reset_index()])

### Normalise to first decade

time1 = combo_mean1.time.unique()[0]
combo_first = combo_mean1[combo_mean1.time == time1].drop('time', axis=1)

#col_names = ['RCP2.6_norm', 'RCP4.5_norm', 'RCP6.0_norm', 'RCP8.5_norm', 'nrch']
#proj_first.columns = proj_first.columns.set_levels(col_names, 0)

combo_first2 = merge(combo_mean1[['nrch', 'time']], combo_first, on='nrch').set_index(['nrch', 'time'])
combo_norm1 = combo_mean1.set_index(['nrch', 'time']) / combo_first2 * 100

############################################
#### Stats

combo_norm1.std()

combo_norm1.std(axis=1)

combo_norm1.describe().T





























