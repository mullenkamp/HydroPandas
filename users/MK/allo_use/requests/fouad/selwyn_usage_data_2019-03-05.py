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

#catch_group = ['Ashburton River']
cwms = ['Selwyn - Waihora']
#rdr_site = 'J36/0016-M1'
#summ_col = 'SwazName'

#crc_filter = {'use_type': ['stockwater', 'irrigation']}

#base_url = 'http://wateruse.ecan.govt.nz'
#hts = 'WaterUse.hts'

datasets = ['allo', 'metered_allo', 'restr_allo', 'metered_restr_allo', 'usage']

groupby = ['crc', 'take_type', 'wap', 'use_type', 'date']

freq = 'M'

from_date = '1900-01-01'
to_date = '2019-02-28'

#export_dir = r'E:\ecan\shared\projects\ashburton\2019-02-21'
export_dir = r'E:\ecan\local\Projects\requests\fouad\2019-03-05'
plot_dir = 'plots'
#export1 = 'crc_usage_summary_2019-02-15.csv'
export2 = 'swaz_allo_usage_2019-03-05.csv'
#export3 = 'swaz_allo_usage_pivot_2019-02-25.csv'
#export4 = 'all_the_usage_2019-02-15.csv'

now1 = str(datetime.now().date())

############################################
### Extract data

#sites1 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'CatchmentGroupName', summ_col], where_in={'CatchmentGroupName': catch_group})
#
#site_filter = {'SwazName': sites1.SwazName.unique().tolist()}

a1 = AlloUsage(from_date, to_date, site_filter={'CwmsName': cwms})

combo_ts = a1.get_ts(datasets, freq, groupby)

#combo_ts1 = pd.merge(combo_ts.reset_index(), a1.allo['use_type'].reset_index(), on=['crc', 'take_type', 'allo_block'])
#combo_ts2 = pd.merge(combo_ts1, a1.sites['SwazName'].reset_index(), on='wap')
#
#swaz1 = util.grp_ts_agg(combo_ts2, ['SwazName', 'use_type'], 'date', 'A-JUN')[['total_allo', 'total_metered_allo', 'total_restr_allo', 'total_metered_restr_allo', 'total_usage']].sum().round(-4)

#num_cols = combo_ts2.dtypes[combo_ts2.dtypes == 'float64'].index.tolist()
#
#swaz1 = combo_ts2.groupby(['SwazName', 'use_type', 'date'])[['total_allo', 'total_metered_allo', 'total_restr_allo', 'total_metered_restr_allo', 'total_usage']].sum().round()

#swaz2 = swaz1.unstack([0, 1])

combo_ts.to_csv(os.path.join(export_dir, export2))


#########################################
### Plotting

