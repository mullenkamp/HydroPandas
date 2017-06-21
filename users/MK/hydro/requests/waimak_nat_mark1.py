# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 15:17:37 2016

@author: MichaelEK
"""

from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric
from core.misc import printf
from core.ts.sw import stream_nat, malf7d, flow_stats
from geopandas import read_file
from core.classes.hydro import hydro
from os.path import join
from numpy import exp
from core.ts.sw import flow_reg

############################################
#### Parameters
site_csv = r'E:\ecan\local\Projects\requests\waimak\2017-06-12\sites.csv'
qual_codes = [10, 18, 20, 30]

export_shp = r'E:\ecan\local\Projects\requests\waimak\2017-06-12\sites.shp'
bound_shp = r'E:\ecan\local\Projects\requests\waimak\2017-06-12\waimak_area.shp'

end = '2015-06-30'
catch_sites_csv = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/results/catch_sites.csv'
catch_shp = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/catch2.shp'

export_rec_flow_path = r'E:\ecan\local\Projects\requests\waimak\2017-06-12\results\waimak_flow_nat.csv'
export_gauge_flow_path = r'E:\ecan\local\Projects\requests\waimak\2017-06-12\results\waimak_gauge_flow_nat.csv'

catch_shp = r'E:\ecan\local\Projects\requests\waimak\2017-06-12\results\catch_del_poly.shp'
catch_sites_csv = r'E:\ecan\local\Projects\requests\waimak\2017-06-12\results\catch_sites.csv'
flow_csv = 'S:/Surface Water/shared/base_data/flow/all_flow_rec_data.csv'

#### Regressions

reg_base = r'E:\ecan\local\Projects\requests\waimak\2017-06-12\regressions'
lin_reg_csv = 'lin_reg.csv'
log_reg_csv = 'log_reg.csv'
lin_reg_flow_csv = 'lin_reg_flow.csv'
log_reg_flow_csv = 'log_reg_flow.csv'

combo_reg_csv = 'combo_reg.csv'
combo_reg_flow_csv = 'combo_reg_flow.csv'

base_data_csv = 'recorded_flows.csv'
base_reg_data_csv = 'recorded_est_flows.csv'
nat_data_csv = 'nat_flows.csv'

#### stats

stats_base_csv  = 'stats_base.csv'
malf_base_csv = 'malf_base.csv'
stats_nat_csv  = 'stats_nat.csv'
malf_nat_csv = 'malf_nat.csv'


###########################################
#### Load in data

sites = read_csv(site_csv)[['site', 'type']]
sites_m = sites[sites.type == 'Gauging'].site.astype('int32').tolist()
sites_r = sites[sites.type == 'Hydrotel'].site
sites_m.extend([270, 1516, 389, 66215, 253, 1278, 66409])
sites_gwl = ['M35/0366']

h1 = hydro().get_data(mtypes=['flow'], sites=bound_shp, qual_codes=qual_codes, buffer_dis=10000)
h1a = h1.get_data('flow_m', sites_m)
h2 = h1a.get_data('gwl', sites_gwl)



##########################################
#### Flow reg

y_sites = list(h2.mtypes_sites['flow_m'])
x_sites = list(h2.mtypes_sites['flow'])

new1, reg1 = h2.flow_reg(y_sites, x_sites, below_median=True)
new2, reg2 = h2.flow_reg(y_sites, x_sites, below_median=True, logs=True)

reg1.to_csv(join(reg_base, lin_reg_csv))
reg2.to_csv(join(reg_base, log_reg_csv))
new1.to_csv(join(reg_base, lin_reg_flow_csv), pivot=True)
new2.to_csv(join(reg_base, log_reg_flow_csv), pivot=True)
h2.to_csv(join(reg_base, base_data_csv), pivot=True)

gwl1 = h2.sel_ts('gwl', pivot=True)
bad1s = h2.sel_ts('flow_m', 279, pivot=True)

reg9, bad_ts = flow_reg(gwl1, bad1s, below_median=True, make_ts=True)
reg9.loc[:, 'log'] = False

reg_dict = {387: (65901, True), 389: (66213, False), 66215: (66204, False), 361: (66429, False), 1516: (66213, True), 270: (66417, False), 66409: (66213, True), 370: (66415, False), 343: (66417, False), 253: (65901, True), 1115: (65901, True), 339: (66417, True), 371: (66401, False), 66432: (66401, False)}

new_regs = DataFrame()
new_flow = DataFrame()
for i in reg_dict:
    sited = reg_dict[i]
    if sited[1]:
        site_reg = reg2.loc[[i]]
        site_reg.loc[:, 'log'] = True
        site_flow = new2.sel_ts('flow', i).reset_index()
    else:
        site_reg = reg1.loc[[i]]
        site_reg.loc[:, 'log'] = False
        site_flow = new1.sel_ts('flow', i).reset_index()
    new_regs = concat([new_regs, site_reg])
    new_flow = concat([new_flow, site_flow])

new_regs1 = concat([new_regs, reg9])

new_flow1 = new_flow.pivot('time', 'site', 'data')
new_flow2 = concat([new_flow1, bad_ts], axis=1)

new_regs1.to_csv(join(reg_base, combo_reg_csv))
new_flow2.to_csv(join(reg_base, base_reg_data_csv))

##########################################
#### Plot

stats1 = h2.stats('flow')

x_site = 66417
y_site = 343
x_max = 1
y_max = 0.2

h2.plot_reg(x_mtype='flow', x_site=x_site, y_mtype='flow_m', y_site=y_site, x_max=x_max, y_max=y_max)


###########################################
#### Run stream naturalization

## No GW in nat

flow, gaugings, nat_flow, nat_gauge = stream_nat(catch_shp, catch_sites_csv, flow_csv=new_flow2[:end], include_gw=False, export=True, export_rec_flow_path=export_rec_flow_path, export_gauge_flow_path=export_gauge_flow_path)

nat_flow.to_csv(join(reg_base, nat_data_csv))

###########################################
#### MALFs and stats

stats_base = flow_stats(flow)
malf_base = malf7d(flow, intervals=[10])
stats_nat = flow_stats(nat_flow)
malf_nat = malf7d(nat_flow, intervals=[10])

stats_base.to_csv(join(reg_base, stats_base_csv))
malf_base.to_csv(join(reg_base, malf_base_csv))
stats_nat.to_csv(join(reg_base, stats_nat_csv))
malf_nat.to_csv(join(reg_base, malf_nat_csv))




#########################################
#### Testing
site = 343
#site2 = 66201

nat_flow[site].dropna()
flow[site].dropna()


#### Linear reg
t1 = lin_reg(nat_gauge, nat_flow)
t2 = [i[i['n-obs'] > 5] for i in t1]
t3 = [i for i in t2 if not i.empty]

t4 = DataFrame(i[i.NRMSE == min(i.NRMSE)].values.flatten() for i in t3)
t4.columns = t3[0].columns


site = 66417

flow[site].plot()
nat_flow[site].plot()

x_set = h2.sel_ts(mtypes='flow', sites=x)
hydro1 = h2.sel(mtypes='flow_m', sites=y)

median1 = h2.stats('flow')['Median']

grp1 = x_set.groupby(level='site')

new_df = DataFrame()
for name, grp in grp1:
    med1 = median1.loc[name]
    t1 = grp[grp <= (med1 * 1.1)].reset_index()
    new_df = concat([new_df, t1])


x_mtype = 'flow'
y_mtype = 'flow_m'

new_flow2 = new_flow1.copy()

malf7d(new_flow2)

flow = h2.sel_ts('flow', pivot=True)[:end]



x = h2.sel_ts('flow', pivot=True)
y = h2.sel_ts('flow_m', pivot=True)


sites1 = ['M35/5436', 'M35/0366']
sites2 = [66432, 371, 279, 339, 1115, 66409]

gwl1 = h2.sel_ts('gwl', pivot=True)
bad1s = h2.sel_ts('flow_m', sites2, pivot=True)

from core.ts.sw import flow_reg
from core.ts.plot import reg_plot
import seaborn as sns

reg9 = flow_reg(gwl1, bad1s, below_median=True, min_obs=1)
reg10 = flow_reg(gwl1, bad1s)


reg_plot(gwl1[[sites1[1]]], bad1s[[sites2[2]]])


bad1s = h2.sel('flow_m', sites2)
gwl2 = h5.combine(bad1s)


gwl2.plot_reg(x_mtype='gwl', x_site=sites1[1], y_mtype='flow_m', y_site=sites2[2])


gwl_site = 'M35/5436'
f_site = 66432

t1 = gwl2.sel_ts(pivot=True)

t2 = t1.loc[:, [('flow_m', f_site), ('gwl', gwl_site)]]
t2.columns = [str(f_site), gwl_site]
t3 = t2.dropna()

sns.regplot(gwl_site, str(f_site), t3)






















