# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 11:44:15 2018

@author: michaelek
"""
import os
import pyhydllp as pyhy
import pandas as pd
import numpy as np
from seaborn import set_style, despine, set_context, color_palette, barplot
import matplotlib.pyplot as plt
from hydropandas.tools.river.ts.stats import flow_stats
from hydropandas.tools.general.ts.misc import tsreg
from hydropandas.tools.general.ts.resampling import grp_ts_agg

##############################################################
### Parameters

ini_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd'
dll_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd\sys\run'

server = 'sql2012prod03'
database = 'Hydstra'

export_gauging_dis = 'gauging_distributions.png'
export_qual_days = 'qual_code_days_by_year.csv'
export_qual_ratio = 'qual_code_ratio_by_year.csv'
export_qual_site_days = 'qual_code_days_by_site.csv'
export_qual_site_ratio = 'qual_code_ratio_by_site.csv'
export_deviations = 'gauging_deviations.csv'

## Modify
sites = [70105, 69607]

base_dir = r'E:\ecan\shared\projects\sw_audits'


############################################################
### Pull out data from Hydstra

## Pull out recorder data
hyd1 = pyhy.hyd(ini_path, dll_path)

ts1 = hyd1.get_ts_data(sites, qual_codes=[50, 40, 30, 20, 10, 11, 21, 18, 31])

## Reformat and assign missing data codes
ts1a = ts1.reset_index('site')
ts2 = ts1a.groupby('site').resample('D')['data', 'qual_code'].first()
ts2.loc[ts2.qual_code.isnull(), 'data'] = 0
ts2.loc[ts2.qual_code.isnull(), 'qual_code'] = 60

## pull out gauging data
g1 = pyhy.sql.gaugings(server, database, sites, mtypes=['flow', 'deviation'])
g1.loc[g1.deviation > 1000, 'deviation'] = np.nan

############################################################
### Run the data quality summary stats

grp1 = ts2.groupby([pd.Grouper(level='site'), pd.Grouper(level='time', freq='A')]).data
grp2 = ts2.groupby([pd.Grouper(level='site'), pd.Grouper(level='time', freq='A'), pd.Grouper('qual_code')]).data

## By year
tot_days = grp1.count()
qual_days = grp2.count().unstack(2).sort_index(axis=1)

qual_ratio = qual_days.div(tot_days.values, axis=0).round(3).sort_index(axis=1)

## Complete record
tot_site_days = tot_days.sum(level='site')
qual_site_days = qual_days.sum(level='site').sort_index(axis=1)

qual_site_ratio = qual_site_days.div(tot_site_days.values, axis=0).round(3).sort_index(axis=1)

## Save results
qual_days.to_csv(os.path.join(base_dir, export_qual_days))
qual_ratio.to_csv(os.path.join(base_dir, export_qual_ratio))
qual_site_days.to_csv(os.path.join(base_dir, export_qual_site_days))
qual_site_ratio.to_csv(os.path.join(base_dir, export_qual_site_ratio))

############################################################
### Gauging distribution

ts3 = ts1.data.unstack(0)

stats1 = ts3.describe()
stats1.loc['95%'] = ts3.quantile(.95)

cat_names = ['min to median', 'median to mean', 'mean to 2 * mean', '2 * mean to 95%', '95% to max']

gname = 'Gaugings count'
cat_name = 'Flow Category'
plt.ioff()

for i in stats1.columns:

    bins2 = [stats1[i]['min'], stats1[i]['50%'], stats1[i]['mean'], stats1[i]['mean']*2, stats1[i]['95%'], stats1[i]['max']]

    cat1 = pd.cut(g1.loc[i].flow, bins2, labels=cat_names)

    cat2 = g1.loc[i].flow.groupby(cat1).count()
    cat2.name = gname
    cat3 = pd.DataFrame(cat2)
    cat3.index.name = cat_name
    cat3 = cat3.reset_index()
    fig, ax1 = plt.subplots(figsize=(12, 8))
    barplot(x=cat_name, y=gname, data=cat3, palette='Blues_d', ax=ax1)
    despine()
    plt.tight_layout()
    plot = ax1.get_figure()
    plot.savefig(os.path.join(base_dir, i + '_' + export_gauging_dis))

############################################################
### Gauging deviations

dev1 = g1.deviation

pos_count1 = (dev1 > 0).sum(level=0)
neg_count1 = (dev1 < 0).sum(level=0)
zero_count1 = (dev1 == 0).sum(level=0)

eight_count1 = ((dev1 <= 8) & (dev1 >= -8)).sum(level=0)
eight_ratio1 = (eight_count1/(pos_count1 + neg_count1 + zero_count1)).round(3)

dev_res1 = pd.DataFrame([pos_count1, neg_count1, zero_count1, eight_count1, eight_ratio1], index=['positive deviation count', 'negative deviation count', 'zero deviation count', 'count within +/- 8%', 'ratio within +/- 8%'])

dev_res1.to_csv(os.path.join(base_dir, export_deviations))

