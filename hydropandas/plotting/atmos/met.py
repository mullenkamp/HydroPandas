# -*- coding: utf-8 -*-
"""
Created on Wed Feb 22 10:11:56 2017

@author: michaelek
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
    pcm['Month'] = pcm.index.month
    pcm['Year'] = pcm.index.year

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
    plot = plot.set(xlabel='Month', ylabel='Rainfall (mm)', title= site_name+' - '+str(site_number))
    plot = plt.tight_layout()

    return(plot)
