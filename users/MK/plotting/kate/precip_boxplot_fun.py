# -*- coding: utf-8 -*-
"""
Created on Thu Jul 07 12:29:12 2016

@author: KateSt

csv_path = 'S:/KateSt/OTOP/Climate/OTOPDAILYRAINFALL.csv'
site_list_path = 'S:/KateSt/OTOP/Climate/OTOP rain gauges.csv'
save_folder_path = 'S:/KateSt/OTOP/Climate/Rainfall Boxplots/'
site_name = 'Te Ngawai at Mackenzie pass'
site_number = 401610
"""
def precip_multi(site_list_path, csv_path, save_folder_path):
    from pandas import read_csv
    from precip_boxplot_fun import precip_plot
    import matplotlib.pyplot as plt
    from os import path

#    import
    site_list = read_csv(site_list_path)
    numbers = site_list.columns.values[0]
    names = site_list.columns.values[1]
    site_list = site_list.rename(columns={numbers: 'numbers', names: 'names'})

    rows = len(site_list.index)
    for i in range(rows):
        site_number = site_list.numbers[i]
        site_name = site_list.names[i]
        plot = precip_plot(csv_path, site_name, site_number)
        plt.tight_layout()
        plt.savefig(save_folder_path+site_name+' - '+str(site_number)+'.png')
        #plt.gcf().clear()
        plt.close()
        print(str(site_number)+' '+site_name)

    return()

def precip_plot(csv_path, site_name, site_number):
    import pandas as pd
    import numpy as np
    import seaborn as sns
    import matplotlib.pyplot as plt
    import calendar
    plt.style.use('ggplot')
    from ts_stats_fun import w_resample
    from import_hydstra import rd_hydstra_csv

    pc = rd_hydstra_csv(csv_path)
    pcm = w_resample(pc, period='month', min_ratio=0.9)
    pcm['Month']=pcm.index.month
    pcm['Year']=pcm.index.year

    site = pcm[[site_number,'Month','Year']]
    site = site.rename(columns={site_number: 'Rainfall'})
    table = site.pivot(index = 'Year', columns='Month', values='Rainfall')
    table = table.rename(columns=lambda x: calendar.month_abbr[x])

    """
    #pandas/matplotlib ggplot
    plot = table.plot.box()
    plot = plt.xlabel('Month')
    plot = plt.ylabel('PET (mm)')
    plot = plt.title(site_name+' - '+str(site_number))
    plot = plt.tight_layout()
    """
    #seaborn
    sns.set(context='paper', font_scale=0.95)
    plt.figure(figsize=(3.2, 3.1))
    plot = sns.boxplot(table, linewidth=1)
    plot = plot.set(xlabel='Month',ylabel='Rainfall (mm)',title= site_name+' - '+str(site_number))
    plot = plt.tight_layout()

    return(plot)

    #cols = len(pcm.columns)-2
    #gbl = globals()

    #for i in range(cols):
       # sitenum = str(pcm.columns.values[i])
       # gbl['Site_'+sitenum]= pcm[[pcm.columns[i],pcm.columns[-1]]]

       # gbl['plot'+sitenum]=

"""
Script for creating rainfall and ET from cliflo monthly extraction

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import calendar
matplotlib.style.use('ggplot')
from import_hydstra import rd_hydstra_csv
path = 'S:/KateSt/OTOP/Climate/OTOPMETDATAIMPORT.csv'

pcm = read_csv(path, index_col='Date', parse_dates=True, dayfirst=True)
pcm['Month']=pcm.index.month
pcm['Year']=pcm.index.year

tr = pcm[(pcm['Station'] == 'Timaru Aero AWS') & (pcm['Stat Code'] == 0)]
tr = tr[['Stat Value ','Month','Year']]
tr = tr.rename(columns={'Stat Value ': 'Rainfall'})

table = tr.pivot(index = 'Year', columns='Month', values='Rainfall')
table = table.rename(columns=lambda x: calendar.month_abbr[x])

plt.figure(figsize=(4,3))

sns.set(context='paper', font_scale=0.95)
plt.figure(figsize=(3.2, 3.1))
plot = sns.boxplot(table, linewidth=1)
plot = plot.set(xlabel='Month',ylabel='Rainfall (mm)',title='Timaru - 413205')
plot = plt.tight_layout()

cp = pcm[(pcm['Station'] == 'Timaru Aero Aws') & (pcm['Stat Code'] == 34)]
cp = cp[['Stat Value ','Month','Year']]
cp = cp.rename(columns={'Stat Value ': 'PET'})

table = cp.pivot(index = 'Year', columns='Month', values='PET')
table = table.rename(columns=lambda x: calendar.month_abbr[x])

sns.set(context='notebook', font_scale=1)

sns.set(context='paper', font_scale=0.95)
plt.figure(figsize=(3.2, 4))
plot = sns.boxplot(table, linewidth=0.5)
plot = plot.set(xlabel='Month',ylabel='PET (mm)',title='Timaru - 413205')
plot = plt.tight_layout()

"""






































