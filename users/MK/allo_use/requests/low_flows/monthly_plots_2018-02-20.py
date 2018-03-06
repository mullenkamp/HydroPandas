# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 12:46:41 2018

@author: MichaelEK
"""
from os import path
import pandas as pd
from hydropandas.io.tools.mssql import rd_sql
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#####################################
### Parameters

server = 'SQL2012dev01'
database = 'Hydro'
table = 'LowFlowRestrSite'

from_date = '2017-10-01'
to_date = '2018-01-31'

include_flow_methods = ['Correlated from Telem', 'Gauged', 'Telemetered', 'Visually Gauged']

export_path = r'E:\ecan\local\Projects\requests\helen\2018-02-27'
export_name_fancy = '2017-2018_restrictions_fancy_v02.png'
export_name = '2017-2018_restrictions_v02.png'
export_man_calc_sites = 'lowflow_sites.csv'

####################################
### extract data

lowflow1 = rd_sql(server, database, table, where_col={'site_type': ['LowFlow']}, from_date=from_date, to_date=to_date, date_col='date')
lowflow1['date'] = pd.to_datetime(lowflow1['date'])
lowflow2 = lowflow1[lowflow1.flow_method.isin(include_flow_methods)].copy()

#full_restr1 = lowflow1[lowflow1.restr_category == 'Full'].groupby('date')['restr_category'].count()
#full_restr1.name = 'Full'
#
#partial_restr1 = lowflow1[lowflow1.restr_category == 'Partial'].groupby('date')['restr_category'].count()
#partial_restr1.name = 'Partial'
#
#restr1 = pd.concat([full_restr1, partial_restr1], axis=1)
#restr1.index = pd.to_datetime(restr1.index)
#restr1.columns.name = 'Restriction Type'

#restr2 = restr1.stack()
#restr2.name = 'Number of sites on restriction'
#restr3 = restr2.reset_index().copy()

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

all_colors = []
all_colors.extend(full_color)
all_colors.extend(partial_color)
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























