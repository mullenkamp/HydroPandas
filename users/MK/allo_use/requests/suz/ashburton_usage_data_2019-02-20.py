# -*- coding: utf-8 -*-
"""
Created on Thu Aug 23 15:47:54 2018

@author: MichaelEK
"""
import os
import pandas as pd
from pdsql import mssql
from allotools import AlloUsage
#from hilltoppy import web_service as wb
#import geopandas as gpd
#import gistools.vector as vec
from datetime import datetime

pd.options.display.max_columns = 10


############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
sites_table = 'ExternalSite'

catch_group = ['Ashburton River']
#cwms = ['Selwyn - Waihora']
#rdr_site = 'J36/0016-M1'
summ_col = 'SwazName'

crc_filter = {'use_type': ['stockwater', 'irrigation']}

#base_url = 'http://wateruse.ecan.govt.nz'
#hts = 'WaterUse.hts'

datasets = ['allo', 'metered_allo', 'restr_allo', 'metered_restr_allo', 'usage']

freq = 'A-JUN'

from_date = '2012-07-01'
to_date = '2018-06-30'

export_dir = r'E:\ecan\shared\projects\ashburton\2019-02-21'
export_dir = r'E:\ecan\local\Projects\requests\suz\2018-12-17'
plot_dir = 'plots'
#export1 = 'crc_usage_summary_2019-02-15.csv'
export2 = 'swaz_allo_usage_2019-02-25.csv'
export3 = 'swaz_allo_usage_pivot_2019-02-25.csv'
#export4 = 'all_the_usage_2019-02-15.csv'

now1 = str(datetime.now().date())

############################################
### Extract data

sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'CatchmentGroupName', summ_col], where_in={'CatchmentGroupName': catch_group})

site_filter = {'SwazName': sites1.SwazName.unique().tolist()}

a1 = AlloUsage(from_date, to_date, site_filter=site_filter, crc_filter=crc_filter)

combo_ts = a1.get_ts(datasets, freq, ['SwazName', 'use_type', 'date'], irr_season=True)

#combo_ts1 = pd.merge(combo_ts.reset_index(), a1.allo['use_type'].reset_index(), on=['crc', 'take_type', 'allo_block'])
#combo_ts2 = pd.merge(combo_ts1, a1.sites['SwazName'].reset_index(), on='wap')
#
#swaz1 = util.grp_ts_agg(combo_ts2, ['SwazName', 'use_type'], 'date', 'A-JUN')[['total_allo', 'total_metered_allo', 'total_restr_allo', 'total_metered_restr_allo', 'total_usage']].sum().round(-4)

#num_cols = combo_ts2.dtypes[combo_ts2.dtypes == 'float64'].index.tolist()
#
#swaz1 = combo_ts2.groupby(['SwazName', 'use_type', 'date'])[['total_allo', 'total_metered_allo', 'total_restr_allo', 'total_metered_restr_allo', 'total_usage']].sum().round()

#swaz2 = swaz1.unstack([0, 1])

swaz1.to_csv(os.path.join(export_dir, export2))


#########################################
### Plotting

cat=['total_metered_allo', 'total_usage']
cat=['total_allo', 'total_metered_allo', 'total_usage']

swaz = 'South Branch'
swaz = 'Mt Harding'
use_type = 'irrigation'
cols = ['total_allo', 'total_restr_allo', 'total_metered_allo', 'total_metered_restr_allo', 'total_usage']

t1 = swaz1.loc[(swaz, use_type, slice(None)), cols].reset_index()

df = t1.drop(['SwazName', 'use_type'], axis=1).set_index('date')

#swaz2 = swaz1.sum(level=[0,2])

grp1 = combo_ts.groupby(level='SwazName')

for i, grp in grp1:
#    print(grp)
    plot_group(grp.sum(level=[0,2]).loc[i], export_path=os.path.join(export_dir, plot_dir), export_name=i + '_allo_usage_' + now1 + '.png')
    plot_stacked_use_type(grp.loc[i], export_path=os.path.join(export_dir, plot_dir), export_name=i + '_allo_use_type_' + now1 + '.png')




#############################################
#### Estimate allocation
#
#allo1 = allocation_ts.allo_ts(server, from_date, to_date, 'A-JUN', 'annual volume')
#allo2 = allo1.reset_index()
#allo2.rename(columns={'date': 'year', 'allo': 'feav'}, inplace=True)
#
#############################################
#### Extract data
#
#sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'CatchmentGroupName', summ_col], where_in={'CatchmentGroupName': catch_group})
#
#crc = mssql.rd_sql(server, database, crc_table).drop('mod_date', axis=1)
#crc = crc.replace({'use_type': use_type_dict})
#
#allo3 = pd.merge(allo2, crc[['crc', 'take_type', 'allo_block', 'use_type']], on=['crc', 'take_type', 'allo_block'], how='left')
#
#crc_wap = mssql.rd_sql(server, database, crc_wap_table, ['crc', 'take_type', 'allo_block', 'wap', 'in_sw_allo', 'max_rate_wap'])
#crc_wap1 = crc_wap[((crc_wap.take_type == 'Take Surface Water') & (crc_wap.in_sw_allo)) | (crc_wap.take_type == 'Take Groundwater')]
#crc_wap1 = crc_wap1[crc_wap1.take_type.isin(list(datasets.values()))].copy()
#
#sites2 = sites1.rename(columns={'ExtSiteID': 'wap'})
#crc_wap1a = pd.merge(crc_wap1, sites2, on=['wap'])
#
#crc_wap2 = pd.merge(allo3, crc_wap1a[['crc', 'take_type', 'allo_block', 'wap', 'max_rate_wap', summ_col]], on=['crc', 'take_type', 'allo_block'])
#
#grp1 = crc_wap2.groupby(['crc', 'take_type', 'allo_block', 'year'])
#crc_wap2['tot_rate'] = grp1['max_rate_wap'].transform('sum')
#crc_wap2['feav'] = (crc_wap2['max_rate_wap']/crc_wap2['tot_rate']) * crc_wap2['feav']
#
#tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_in={'ExtSiteID': crc_wap2.wap.unique().tolist(), 'DatasetTypeID': list(datasets.keys())}, from_date=from_date, to_date=to_date, date_col='DateTime')
#tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)
#
#tsdata2 = grp_ts_agg(tsdata1, ['ExtSiteID', 'DatasetTypeID'], 'DateTime', 'A-JUN').Value.sum().round()
#tsdata3 = tsdata2.reset_index().copy()
#tsdata3.rename(columns={'DateTime': 'year', 'ExtSiteID': 'wap', 'DatasetTypeID': 'take_type'}, inplace=True)
#tsdata3.replace({'take_type': datasets}, inplace=True)
#
#tsdata4 = pd.merge(crc_wap2.drop(['max_rate_wap', 'tot_rate'], axis=1), tsdata3, on=['year', 'wap', 'take_type'], how='left')
#count1 = tsdata4.groupby(['year', 'wap']).crc.transform('count')
#
#tsdata4['Value'] = (tsdata4['Value']/count1).round()
#
#tsdata5 = tsdata4[~(tsdata4.Value > (tsdata4.feav*1.8))]
#
#### Estimate metered allo and usage
#usage1 = tsdata5.dropna().groupby(['year', 'take_type', 'use_type', summ_col])['feav', 'Value'].sum()
#usage1.columns = ['Metered Allocation (m3)', 'Usage (m3)']
#
#### Estimate total allocation
#allo1 = tsdata5.groupby(['year', 'take_type', 'use_type', summ_col]).feav.sum()
#allo1.name = 'Total Allocation (m3)'
#
#### For the count of waps
#tsdata6 = tsdata5[['year', 'take_type', 'use_type', summ_col, 'wap']].copy().drop_duplicates()
#grp2 = tsdata6.groupby(['year', 'take_type', 'use_type', summ_col])['wap'].count()
#grp2.name = 'All WAPs count'
#tsdata7 = tsdata5.dropna()[['year', 'take_type', 'use_type', summ_col, 'wap']].copy().drop_duplicates()
#grp3 = tsdata7.groupby(['year', 'take_type', 'use_type', summ_col])['wap'].count()
#grp3.name = 'Metered WAPs count'
#
#### Combine all
#all1 = pd.concat([allo1, usage1, grp2, grp3], axis=1).sort_index()
#all1.index.name = 'Year End'
#
#all1.to_csv(os.path.join(export_dir, export1))
#
#### Daily usage by SWAZ
#
##swaz1 = pd.merge(tsdata1, sites1, on='ExtSiteID')
##swaz1.replace({'DatasetTypeID': datasets}, inplace=True)
##swaz1.rename(columns={'DatasetTypeID': 'take_type'}, inplace=True)
#
#tsdata7 = tsdata1.copy()
#tsdata7.rename(columns={'ExtSiteID': 'wap', 'DatasetTypeID': 'take_type'}, inplace=True)
#tsdata7.replace({'take_type': datasets}, inplace=True)
#tsdata7['year'] = tsdata7.DateTime.dt.year
#
#crc_wap3 = crc_wap2.drop(['max_rate_wap', 'tot_rate'], axis=1).copy()
#crc_wap3['year'] = crc_wap3['year'].dt.year
#
#tsdata8 = pd.merge(crc_wap3, tsdata7, on=['year', 'wap', 'take_type'])
#count2 = tsdata8.groupby(['DateTime', 'wap']).crc.transform('count')
#
#tsdata8['Value'] = (tsdata8['Value']/count2).round()
#
#swaz2 = tsdata8.groupby(['SwazName', 'take_type', 'use_type',  'DateTime']).Value.sum().round()
#swaz2.name = 'Usage (m3)'
#
#swaz2.to_csv(os.path.join(export_dir, export2), header=True)
#
#swaz3 = swaz2.unstack([0, 1, 2])
#
#swaz3.to_csv(os.path.join(export_dir, export3), header=True)
#
#tsdata8.drop('year', axis=1).to_csv(os.path.join(export_dir, export4), index=False, header=True)

### Extra checks
#
#big1 = swaz2.groupby(level=['SwazName']).quantile(0.97) * 2
#
#for name, val in swaz2.groupby(level=['SwazName']):
#    count1 = val[val > big1[name]]
#    print(count1)
#
#tsdata8 = tsdata1.groupby(['ExtSiteID', 'DateTime']).Value.sum().round()
#tsdata8.name = 'Usage (m3)'
#
#big1 = tsdata8.groupby(level=['ExtSiteID']).quantile(0.97) * 2
#
#for name, val in tsdata8.groupby(level=['ExtSiteID']):
#    count1 = val[val > big1[name]]
#    print(count1)


#################################################
#
#sites = ('K36/0927', 'K36/0854', 'K36/1000', 'K37/0792', 'K37/1442', 'K37/1725', 'K37/2034', 'K37/2238', 'K37/2239', 'K37/2893', 'K37/2894', 'K37/3582', 'L37/0239', 'L37/1265', 'L37/1430')
#
#from_date = '2016-06-15'
#to_date = '2016-07-15'
#
#site = 'K36/1000-M1'
#
#mtypes = wb.measurement_list(base_url, hts, site)
#mtypes
#
#td1 = wb.get_data(base_url, hts, site, 'Compliance Volume', from_date, to_date)
#
#td2 = td1.reset_index().drop(['Site', 'Measurement'], axis=1).set_index('DateTime')
#td2.idxmax()
#td2.plot()
#
#
#swaz = 'Mt Harding'
#
#d1 = tsdata5[tsdata5.SwazName == swaz].copy()
#d2 = d1.groupby(['crc', 'year']).sum()
#
#d2.to_csv(os.path.join(export_dir, 'test5.csv'))
#
#
#d2.loc[(slice(None), slice(None), 'CRC169504'), :]
#
#d2.loc['CRC169504']
#
#tsdata4[tsdata4.crc == 'CRC169504']
#
#tsdata4a = tsdata4.copy()
#
#tsdata4a['wap_count'] = tsdata4.groupby(['year', 'wap']).crc.transform('count')
#
#tsdata4a[tsdata4a.crc == 'CRC169504']
#
#c1 = tsdata4.groupby(['year', 'wap']).crc.count()
#
#tsdata4a[tsdata4a.wap == 'BY20/0089']
#
#
#all1.loc[(slice(None), 'Take Surface Water', 'Mt Harding'), :]
#
#allo2[allo2.crc == 'CRC169502']
#
#crc_wap2[crc_wap2.crc.isin(['CRC012123', 'CRC169502'])]

