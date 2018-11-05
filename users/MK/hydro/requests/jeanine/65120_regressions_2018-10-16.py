# -*- coding: utf-8 -*-
"""
Created on Mon May 21 09:56:57 2018

@author: MichaelEK
"""
import numpy as np
import pandas as pd
from pdsql import mssql
import os
import geopandas as gpd
from shapely.geometry import Point
from hydrolm.lm import LM
import matplotlib.pyplot as plt
from gistools.vector import sel_sites_poly

plt.ioff()

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_daily_table = 'TSDataNumericDaily'
ts_hourly_table = 'TSDataNumericHourly'
ts_summ_table = 'TSDataNumericDailySumm'
lf_table = 'LowFlowRestrSite'
sites_table = 'ExternalSite'

recent_date = '2018-01-01'
min_date = '2000-01-01'
min_count = 8
search_dis = 50000

dep_loss = 0.04
nobs_loss = 0.002

rec_datasets = [5, 1521, 4503, 4515]
man_datasets = [8, 1637, 4558, 4564, 4570]

qual_codes = [200, 400, 500, 520, 600]

site = '65120'

export_dir = r'E:\ecan\local\Projects\requests\jeanine\2018-10-16'
fig_sub_dir = 'plots'
export_summ1 = 'summary_2018-10-16.csv'

############################################
### Extract summary data and determine the appropriate sites to use

rec_summ_data = mssql.rd_sql(server, database, ts_summ_table, where_col={'DatasetTypeID': rec_datasets})
rec_summ_data.FromDate = pd.to_datetime(rec_summ_data.FromDate)
rec_summ_data.ToDate = pd.to_datetime(rec_summ_data.ToDate)

rec_sites = rec_summ_data[(rec_summ_data.ToDate > recent_date)]

## Get a full list of sites to retrieve site data for

all_sites = set(rec_sites.ExtSiteID.tolist())
all_sites.update([site])

rec_sites = all_sites.copy()
rec_sites.remove(site)

###########################################
### Get site data

site_xy = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'ExtSiteName', 'NZTMX', 'NZTMY'], where_col={'ExtSiteID': list(all_sites)})

geometry = [Point(xy) for xy in zip(site_xy['NZTMX'], site_xy['NZTMY'])]
site_xy1 = gpd.GeoDataFrame(site_xy['ExtSiteID'], geometry=geometry, crs=2193)

gauge_xy = site_xy1[site_xy1.ExtSiteID.isin([site])].set_index('ExtSiteID').copy()
rec_xy = site_xy1[site_xy1.ExtSiteID.isin(rec_sites)].set_index('ExtSiteID').copy()

###########################################
### Iterate through the low flow sites

results_dict = {}
for g in gauge_xy.index:
    print(g)

    ## Determine recorder sites within search distance
    gauge_loc = gauge_xy.loc[[g]].buffer(search_dis)
    near_rec_sites = sel_sites_poly(rec_xy, gauge_loc)
    print('There are ' + str(len(near_rec_sites)) + ' recorder sites within range')

    ## Extract all ts data
    g_data = mssql.rd_sql(server, database, ts_daily_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'DatasetTypeID': man_datasets, 'ExtSiteID': [g], 'QualityCode': qual_codes}, from_date=min_date, date_col='DateTime')
    g_data.DateTime = pd.to_datetime(g_data.DateTime)
    g_data.loc[g_data.Value <= 0, 'Value'] = np.nan
    r_data = mssql.rd_sql(server, database, ts_daily_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'DatasetTypeID': rec_datasets, 'ExtSiteID': near_rec_sites.index.tolist(), 'QualityCode': qual_codes}, from_date=min_date, date_col='DateTime')
    r_data.DateTime = pd.to_datetime(r_data.DateTime)
    r_data.loc[r_data.Value <= 0, 'Value'] = np.nan

    ## Re-organise the datasets
    g_data1 = g_data.pivot_table('Value', 'DateTime', 'ExtSiteID')
    r_data1 = r_data.pivot_table('Value', 'DateTime', 'ExtSiteID')

    ## Filter the recorder data by guagings
    set1 = pd.concat([g_data1, r_data1], join='inner', axis=1)
    if len(set1) < min_count:
        continue

    ## regressions!
    lm1 = LM(r_data1, g_data1)
    ols1_log = lm1.run('ols', 1, x_transform='log', y_transform='log', min_obs=min_count)
    ols2_log = lm1.run('ols', 2, x_transform='log', y_transform='log', min_obs=min_count)
    ols3_log = lm1.run('ols', 3, x_transform='log', y_transform='log', min_obs=min_count)

    ## Save
    results_dict.update({g: {1: ols1_log, 2: ols2_log, 3: ols3_log}})


### Produce summary table

cols = ['site', 'nrmse', 'Adj R2', 'nobs', 'y range', 'f value', 'f p value', 'dep sites', 'reg params']

res_list_df = []

for s in range(1, 4):
    res_site = pd.DataFrame()
    res_list = []
    for i in results_dict:
        model1 = results_dict[i][s]
        if model1 is not None:
            if model1.sm_xy:
                nrmse1 = model1.nrmse()[i]
                adjr2 = round(model1.sm[i].rsquared_adj, 3)
                nobs = model1.sm[i].nobs
                y_range = model1.sm_xy[i]['y_orig'].max() - model1.sm_xy[i]['y_orig'].min()
                dep_sites = model1.sm_xy[i]['x_orig'].columns.tolist()
                fvalue = round(model1.sm[i].fvalue, 3)
                fpvalue = round(model1.sm[i].f_pvalue, 3)
                params1 = model1.sm[i].params.round(5).tolist()

                site_res = [i, nrmse1, adjr2, nobs, y_range, fvalue, fpvalue, dep_sites, params1]
                res_list.append(site_res)

                ## Plots
                fig = model1.plot_ccpr_grid(i)
                fig.savefig(os.path.join(export_dir, fig_sub_dir, i + '_' + str(s) + '.png'), bbox_inches='tight')
                plt.close(fig)

    res_site1 = pd.DataFrame(res_list, columns=cols).set_index('site')
    res_list_df.append(res_site1)

res_df = pd.concat(res_list_df, axis=1, keys=range(1, len(res_list_df) + 1))


### Categorize the results and assign which is best

levels1 = res_df.columns.levels[0].tolist()

f1 = res_df.loc[:, (slice(None), 'f value')]
f1.columns = f1.columns.droplevel(1)

winner = f1.idxmax(axis=1)
winner.name = 'winner'

res_df['winner'] = winner


### Save data
file_path = os.path.join(export_dir, export_summ1)
res_df.reset_index().to_csv(file_path, index=False)




##################################################
### Testing




