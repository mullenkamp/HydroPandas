# -*- coding: utf-8 -*-
"""
Created on Mon May 21 09:56:57 2018

@author: MichaelEK
"""
import numpy as np
import pandas as pd
from pdsql import mssql
import os
from hydropandas.tools.general.spatial.vector import xy_to_gpd, sel_sites_poly
import geopandas as gpd
from shapely.geometry import Point
import pickle
from hydrolm.lm import LM
from seaborn import regplot
import matplotlib.pyplot as plt

plt.ioff()

############################################
### Parameters

server = 'sql2012dev01'
database = 'hydro'
ts_daily_table = 'TSDataNumericDaily'
ts_hourly_table = 'TSDataNumericHourly'
ts_summ_table = 'TSDataNumericDailySumm'
lf_table = 'LowFlowRestrSite'
sites_table = 'ExternalSite'

lf_date = '2018-05-28'
recent_date = '2018-01-01'
min_date = '2000-01-01'
min_count = 10
search_dis = 50000

dep_loss = 0.04
nobs_loss = 0.002

datasets = [5, 8]

qual_codes = [200, 400, 500, 520, 600]

export_dir = r'E:\ecan\shared\projects\low_flow_regressions'
fig_sub_dir = 'plots'
export_summ1 = 'summary_2018-06-29.csv'

############################################
### Extract summary data and determine the appropriate sites to use

summ_data = mssql.rd_sql(server, database, ts_summ_table, where_col={'DatasetTypeID': datasets})

rec_sites = summ_data[(summ_data.DatasetTypeID == 5) & (summ_data.ToDate > recent_date)]

### Extract low flow sites

lf_sites = mssql.rd_sql(server, database, lf_table, ['site', 'flow_method', 'min_trig', 'max_trig'], where_col={'date': [lf_date], 'site_type': ['LowFlow'], 'flow_method': ['Correlated from Telem', 'Gauged', 'Manually Calculated', 'Visually Gauged']})
lf_sites.rename(columns={'site': 'ExtSiteID'}, inplace=True)

lf_gauge_sites = pd.merge(summ_data[(summ_data.DatasetTypeID == 8)], lf_sites, on='ExtSiteID')

gauge_sites1 = lf_gauge_sites[lf_gauge_sites.Count >= min_count].copy()

## Remove sites that are also recorders

gauge_sites = gauge_sites1[~gauge_sites1.ExtSiteID.isin(rec_sites.ExtSiteID.unique())]

## Summaries of bad sites

mis_lf_gauge_sites = lf_sites[~lf_sites.ExtSiteID.isin(summ_data.ExtSiteID)]
less_than_10_lf_sites = lf_gauge_sites[lf_gauge_sites.Count < min_count]

## Get a full list of sites to retrieve site data for

all_sites = set(rec_sites.ExtSiteID.tolist())
all_sites.update(gauge_sites.ExtSiteID.tolist())

###########################################
### Get site data

site_xy = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY'], where_col={'ExtSiteID': list(all_sites)})

geometry = [Point(xy) for xy in zip(site_xy['NZTMX'], site_xy['NZTMY'])]
site_xy1 = gpd.GeoDataFrame(site_xy['ExtSiteID'], geometry=geometry, crs=2193)

gauge_xy = site_xy1[site_xy1.ExtSiteID.isin(gauge_sites.ExtSiteID)].set_index('ExtSiteID').copy()
rec_xy = site_xy1[site_xy1.ExtSiteID.isin(rec_sites.ExtSiteID)].set_index('ExtSiteID').copy()

###########################################
### Iterate through the low flow sites

results_dict = {}
for g in gauge_sites.ExtSiteID.tolist():
    print(g)

    ## Max trig
    max_trig = gauge_sites[gauge_sites.ExtSiteID == g].max_trig.tolist()[0]

    ## Determine recorder sites within search distance
    gauge_loc = gauge_xy.loc[[g]].buffer(search_dis)
    near_rec_sites = sel_sites_poly(rec_xy, gauge_loc)
    print('There are ' + str(len(near_rec_sites)) + ' recorder sites within range')

    ## Extract all ts data
    g_data = mssql.rd_sql(server, database, ts_daily_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'DatasetTypeID': [8], 'ExtSiteID': [g], 'QualityCode': qual_codes}, from_date=min_date, date_col='DateTime')
    g_data.DateTime = pd.to_datetime(g_data.DateTime)
    g_data.loc[g_data.Value <= 0, 'Value'] = np.nan
    g_data.loc[g_data.Value > max_trig*3, 'Value'] = np.nan
    r_data = mssql.rd_sql(server, database, ts_daily_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'DatasetTypeID': [5], 'ExtSiteID': near_rec_sites.index.tolist(), 'QualityCode': qual_codes}, from_date=min_date, date_col='DateTime')
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
    ols = LM(r_data1, g_data1)
    ols1 = ols.ols(1, log_x=False, log_y=False)
    ols2 = ols.ols(2, log_x=False, log_y=False)
    ols3 = ols.ols(3, log_x=False, log_y=False)

    ## Save
    results_dict.update({g: {1: ols1, 2: ols2, 3: ols3}})


### Produce summary table

cols = ['site', 'nrmse', 'Adj R2', 'nobs', 'y range', 'f value', 'f p value', 'dep sites']

res_list_df = []

for s in range(1, 4):
    res_site = pd.DataFrame()
    res_list = []
    for i in results_dict:
        if results_dict[i][s] is not None:
            model1 = results_dict[i][s]
            nrmse1 = model1.nrmse()[i]
            adjr2 = round(model1.sm[i].rsquared_adj, 3)
            nobs = model1.sm[i].nobs
            y_range = model1.sm_xy[i]['y'].max() - model1.sm_xy[i]['y'].min()
            dep_sites = model1.sm_xy[i]['x'].columns.tolist()
            fvalue = round(model1.sm[i].fvalue, 3)
            fpvalue = round(model1.sm[i].f_pvalue, 3)

            site_res = [i, nrmse1, adjr2, nobs, y_range, fvalue, fpvalue, dep_sites]
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
res_df.to_csv(file_path)




##################################################
### Testing

#site = '169121'
#
#r1 = results_dict[site][1]
#
#x1 = r1.sm_xy[site]['x']
#y1 = pd.DataFrame(r1.sm_xy[site]['y'])
#
#y_predict = r1.sm_predict[site]
#
#y1['predict'] = y_predict
#
#r1 = results_dict[site][3].sm[site]
#
#regplot(x1, y_predict)
#
#results_dict[site][s].sm_xy[site]['y'].describe()


#p_path = os.path.join(export_dir, export_summ1)
#pickle.dump(results_dict, open(p_path, 'wb'), pickle.HIGHEST_PROTOCOL)
#
#in1 = pickle.load(open(p_path, 'rb'))




#dep2_nrmse_diff = res_df[1]['nrmse'] - res_df[2]['nrmse']
#dep2_loss = (res_df[1]['nobs'] - res_df[2]['nobs']) * nobs_loss + dep_loss
#dep2_tot_bool = (dep2_nrmse_diff - dep2_loss) > 0
#
#dep3_nrmse_diff1 = res_df[1]['nrmse'] - res_df[3]['nrmse']
#dep3_loss1 = (res_df[1]['nobs'] - res_df[3]['nobs']) * nobs_loss + dep_loss*2
#dep3_tot_bool1 = (dep3_nrmse_diff1 - dep3_loss1) > 0
#
#dep3_nrmse_diff2 = res_df[2]['nrmse'] - res_df[3]['nrmse']
#dep3_loss2 = (res_df[2]['nobs'] - res_df[3]['nobs']) * nobs_loss + dep_loss
#dep3_tot_bool2 = (dep3_nrmse_diff2 - dep3_loss2) > 0
#
#dep2_wins = dep2_tot_bool & ~dep3_tot_bool2
#dep3_wins = (dep3_tot_bool1 & ~dep2_tot_bool) | (dep2_tot_bool & dep3_tot_bool2)
#dep1_wins = ~dep2_wins & ~dep3_wins
#
#res_df['winner'] = 1
#res_df.loc[dep2_wins, 'winner'] = 2
#res_df.loc[dep3_wins, 'winner'] = 3





