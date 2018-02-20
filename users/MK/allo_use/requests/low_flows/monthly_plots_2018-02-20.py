# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 12:46:41 2018

@author: MichaelEK
"""
import pandas as pd
from hydropandas.io.tools.mssql import rd_sql


#####################################
### Parameters

server = 'SQL2012dev01'
database = 'Hydro'
table = 'LowFlowRestrSite'

from_date = '2018-01-01'
to_date = '2018-01-31'

####################################
### extract data

lowflow1 = rd_sql(server, database, table, where_col={'site_type': ['LowFlow']}, from_date=from_date, to_date=to_date, date_col='date')

full_restr1 = lowflow1[lowflow1.restr_category == 'Full'].groupby('date')['restr_category'].count()
full_restr1.name = 'Full'

partial_restr1 = lowflow1[lowflow1.restr_category == 'Partial'].groupby('date')['restr_category'].count()
partial_restr1.name = 'Partial'

restr1 = pd.concat([partial_restr1, full_restr1], axis=1)
restr1.index = pd.to_datetime(restr1.index)
restr1.name = 'Number of consents on restriction'

ax = restr1.plot(ylim=[0, 100])
ax.set_ylabel('Number of lowflow sites on restriction')













































