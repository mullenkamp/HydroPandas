# -*- coding: utf-8 -*-
"""
Example script to estimate a full time series at gauging sites from regresions to recorder sites.
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from core.classes.hydro.base import hydro
from core.ts.plot import reg_plot

############################################
#### Parameters

sites = [65120]

min_years = 10

shp = r'E:\ecan\local\Projects\requests\waikari\zone.shp'

## Export

base_dir = r'E:\ecan\local\Projects\requests\waikari'

##################################
#### Read in data

h1 = hydro().get_data('flow_m', sites)
h2 = h1.get_data('flow', shp)

################################
#### Run regression and stats

malf, reg = h2.flow_reg(sites, buffer_dis=20000, min_obs=10, below_median=True)


###############################
#### Export data

reg2.to_csv(export_reg, index=False)
new_g_malf.to_csv(export_malf)









########################
#### Testing

g1 = h2.data.loc(axis=0)['m_flow', 65120]
g2 = g1[g1 > 0]
g2.name = 65120
f1 = h2.data.loc(axis=0)['flow', 64610]
f2 = f1[f1 > 0]
f2.name = 64610

gf = concat([g2, f2], axis=1, join='inner')

reg_plot(DataFrame(gf[65120]), DataFrame(gf[64610]), freq='day')





































