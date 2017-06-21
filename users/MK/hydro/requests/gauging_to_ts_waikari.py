# -*- coding: utf-8 -*-
"""
Example script to estimate a full time series at gauging sites from regresions to recorder sites.
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from core.classes.hydro.base import hydro
from core.ts.plot import reg_plot

############################################
#### Parameters

sites = [65120, 1468]
flow_csv = 'S:/Surface Water/shared/base_data/flow/all_flow_rec_data.csv'

min_years = 10

## Export
export_reg = r'C:\ecan\local\Projects\Waimakariri\analysis\flow\gauge_sites_est\flow_gauge_reg_253.csv'
export_malf = r'C:\ecan\local\Projects\Waimakariri\analysis\flow\gauge_sites_est\flow_gauge_reg_ts_malf_253.csv'

##################################
#### Read in data

h1 = hydro().rd_csv(flow_csv, time='date', mtypes='flow', dformat='wide')
h2 = h1.rd_henry(sites)
h2.get_geo_loc()

################################
#### Run stats

malf, reg = h2.malf_reg(sites, buff_dis=50000, min_obs=5)

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





































