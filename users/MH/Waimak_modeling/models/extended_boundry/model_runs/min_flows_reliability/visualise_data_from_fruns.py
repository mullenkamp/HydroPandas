# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 4/10/2017 3:04 PM
"""

from __future__ import division
from core import env
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from copy import deepcopy

sw_site_groups = {
    'cust': ['cust_oxford',
             'cust_threlkelds'],

    'north_cust': ['cam_youngs',
                   'northbrook_marsh',
                   'saltwater_toppings',
                   'southbrook_marsh',
                   'taranaki_preeces',
                   'waikuku_waikuku-beach-rd',
                   'n7drain_hicklands'],

    'south_cust': ['courtenay_neeves',
                   'greigs_greigs',
                   'ohoka_island',
                   'silverstream_neeves']
}
gw_site_groups = {  # todo get a bunch of wells?
    'inland_shallow': ['M34/0306',
                       'L35/0062'],
    'midplains_shallow': ['M35/0058',
                          'M35/6295',
                          'M35/4873'],
    'coast_shallow': ['M35/0538'],

    'inland_deep': ['M35/9154',
                    'L35/0686'],
    'midplains_deep': ['BW23/0133',
                       'BW23/0134',
                       'M35/11283'],
    'coast_deep': ['M35/5445']
}

groups = {}
groups.update(gw_site_groups)
groups.update(sw_site_groups)


def vis_cc():  # todo
    #todo look at seaborn tsplot
    raise NotImplementedError


def vis_eco_min_flows(relitive_data_path):  # todo
    # nat, full abs, full allo, fulla lo abs
    sens = ['naturalised',
            'full_abs',
            'full_abs_allo',
            'full_allo_cur_use', ]
    data = pd.read_csv(relitive_data_path, skiprows=1)
    data = data.loc[:, sens + ['site']]
    data = pd.melt(data, id_vars=['site'], var_name='scenario')
    idx = data.site.str.contains('/')
    data.loc[idx, 'type'] = 'well'
    data.loc[~idx, 'type'] = 'stream'
    data.loc[~idx, 'value'] *= 100
    for key in groups.keys():
        data.loc[np.in1d(data.site, groups[key]), 'group'] = key
    g = sns.FacetGrid(data, 'type', 'scenario', sharex=False, sharey=False)

    g.map(sns.boxplot, 'group', 'value')
    for row in g.axes:
        y_lower = []
        y_upper = []
        for ax in row:
            low, up = ax.get_ylim()
            y_lower.append(low)
            y_upper.append(up)

        for ax in row:
            ax.set_ylim(min(y_lower), max(y_upper))
            [e.set_rotation(-20) for e in ax.get_xticklabels()]
    g.set_axis_labels(x_var='')
    g.facet_axis(0, 0).set_ylabel('drawdown (m)')
    g.facet_axis(1, 0).set_ylabel('percent')
    plt.show()
    return g


def vis_relibability(relitive_data_path):  # todo
    # mod_period, pc5, will eff, pc5 + will eff
    sens = ['current',
            'pc5_80',
            'pc5_80_wil_eff',
            'wil_eff']
    data = pd.read_csv(relitive_data_path, skiprows=1)
    data = data.loc[:, sens + ['site']]
    data = pd.melt(data, id_vars=['site'], var_name='scenario')
    idx = data.site.str.contains('/')
    data.loc[idx, 'type'] = 'well'
    data.loc[~idx, 'type'] = 'stream'
    data.loc[~idx, 'value'] *= 100
    for key in groups.keys():
        data.loc[np.in1d(data.site, groups[key]), 'group'] = key
    g = sns.FacetGrid(data, 'type', 'scenario', sharex=False, sharey=False)

    g.map(sns.boxplot, 'group', 'value')
    for row in g.axes:
        y_lower = []
        y_upper = []
        for ax in row:
            low, up = ax.get_ylim()
            y_lower.append(low)
            y_upper.append(up)

        for ax in row:
            ax.set_ylim(min(y_lower), max(y_upper))
            [e.set_rotation(-20) for e in ax.get_xticklabels()]
    g.set_axis_labels(x_var='')
    g.facet_axis(0, 0).set_ylabel('drawdown (m)')
    g.facet_axis(1, 0).set_ylabel('percent')
    plt.show( )
    return g

if __name__ == '__main__':
    vis_relibability(
        relitive_data_path=r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\forward_sw_gw\results\cc_only_to_waimak\opt_relative_data.csv",
    )