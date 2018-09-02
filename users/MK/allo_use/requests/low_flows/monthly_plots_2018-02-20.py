# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 12:46:41 2018

@author: MichaelEK
"""
from os import path
import pandas as pd
from pdsql.mssql import rd_sql
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#####################################
### Parameters

server = 'SQL2012dev01'
database = 'Hydro'
table = 'LowFlowRestrSite'

from_date = '2017-10-01'
to_date = '2018-04-30'

include_flow_methods = ['Correlated from Telem', 'Gauged', 'Telemetered', 'Visually Gauged']

export_path = r'E:\ecan\shared\projects\mon_water_report\2018-09-03'
export_name_fancy = '2017-2018_restrictions_fancy.png'
export_name = '2017-2018_restrictions.png'
export_man_calc_sites = 'lowflow_sites.csv'
export_sel2 = 'lowflow_restr_2017-10-01.csv'

####################################
### extract data

lowflow1 = rd_sql(server, database, table, where_col={'site_type': ['LowFlow']}, from_date=from_date, to_date=to_date, date_col='date')
lowflow1['date'] = pd.to_datetime(lowflow1['date'])
lowflow2 = lowflow1[lowflow1.flow_method.isin(include_flow_methods)].copy()

site_restr1 = lowflow2.groupby(['site', 'restr_category'])['crc_count'].count()
site_restr1.name = 'count'
max_days = (pd.to_datetime(to_date) - pd.to_datetime(from_date)).days + 1
max_days1 = site_restr1.groupby(level=['site']).transform('sum')
site_restr2 = (site_restr1/max_days1).round(3).unstack('restr_category')


restr2 = lowflow2.groupby(['restr_category', 'flow_method', 'date'])['site'].count()
restr2.name = 'Number of low flow sites on restriction'

restr2 = restr2.loc[['Full', 'Partial']]

restr3 = restr2.unstack([0, 1])
#restr4 = restr3[['Full', 'Partial', 'No']]
restr4 = restr3[['Full', 'Partial']].fillna(0)

##color palettes
full_color = sns.color_palette('Blues')
partial_color = sns.color_palette('Greens')
no_color = sns.color_palette('Greys')

d1 = restr4.columns.to_frame()
full_n = len(d1.loc['Full'])
partial_n = len(d1.loc['Partial'])

all_colors = []
all_colors.extend(full_color[:full_n])
all_colors.extend(partial_color[:partial_n])
#all_colors.extend(no_color)

### Plots
## Set basic plot settings
sns.set_style("white")
sns.set_context('poster')

fig, ax = plt.subplots(figsize=(15, 10))
restr4.plot.area(stacked=True, ax=ax, color=all_colors, ylim=[0, 150])
#restr1.plot.line(stacked=True, ax=ax)
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles[::-1], labels[::-1], title='Restriction Categories', loc='upper left')
x_axis = ax.axes.get_xaxis()
x_label = x_axis.get_label()
x_label.set_visible(False)
ax.set_ylabel('Number of low flow sites on restriction')
xticks = ax.get_xticks()
if len(xticks) > 15:
    for label in ax.get_xticklabels()[::2]:
        label.set_visible(False)
    ax.xaxis_date()
    fig.autofmt_xdate(ha='center')
plt.tight_layout()

plot2 = ax.get_figure()
plot2.savefig(path.join(export_path, export_name_fancy))

### Non-fancy plot
restr2 = lowflow2.groupby(['restr_category', 'date'])['site'].count()
restr2.name = 'Number of low flow sites on restriction'

restr2 = restr2.loc[['Full', 'Partial']]

restr3 = restr2.unstack(0)
#restr4 = restr3[['Full', 'Partial', 'No']]
restr4 = restr3[['Full', 'Partial']].fillna(0)

## Set basic plot settings
sns.set_style("white")
sns.set_context('poster')

fig, ax = plt.subplots(figsize=(15, 10))
restr4.plot.area(stacked=True, ax=ax, color=[full_color[3], partial_color[3]], ylim=[0, 150], alpha=0.7)
#restr1.plot.line(stacked=True, ax=ax)
handles, labels = ax.get_legend_handles_labels()
ax.legend(handles[::-1], labels[::-1], title='Restriction Categories', loc='upper left')
x_axis = ax.axes.get_xaxis()
x_label = x_axis.get_label()
x_label.set_visible(False)
ax.set_ylabel('Number of low flow sites on restriction')
xticks = ax.get_xticks()
if len(xticks) > 15:
    for label in ax.get_xticklabels()[::2]:
        label.set_visible(False)
    ax.xaxis_date()
    fig.autofmt_xdate(ha='center')
plt.tight_layout()

plot2 = ax.get_figure()
plot2.savefig(path.join(export_path, export_name))



### Manually calc sites
#man_calc_sites = lowflow1.loc[(lowflow1.date == to_date), ['site', 'waterway', 'location', 'flow_method']]
#man_calc_sites.to_csv(path.join(export_path, export_man_calc_sites), index=False)
#

#set2 = lowflow2[(lowflow2.date == '2017-10-01') & (lowflow2.restr_category != 'No')]
#set2.to_csv(path.join(export_path, export_sel2), index=False)





















