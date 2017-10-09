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
import itertools
import os

sw_site_groups = {
    'cust': ['cust_oxford',  # todo cust at oxford often goes dry, check in new set of simlulations
             'cust_threlkelds'],
    # todo also some wells going dry, need to handle but wait to see if it's actually a problem
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


def vis_cc(relative_data_path, meta_data_path):
    data = pd.read_csv(relative_data_path, skiprows=1)
    metadata = pd.read_csv(meta_data_path, index_col=0)
    sens = list(metadata.loc[metadata.is_cc].index)
    data = data.loc[:, sens + ['site']]
    metadata = metadata.drop(['path', 'is_cc'], axis=1)
    data = pd.melt(data, id_vars=['site'], var_name='scenario')
    data = pd.merge(data, metadata, 'left', left_on='scenario', right_index=True)
    idx = data.site.str.contains('/')
    data.loc[~idx, 'value'] *= 100
    data = data.reset_index()
    data['condition'] = ['_'.join((i, j)) for i, j in data.loc[:, ['rcp', 'amalg_type']].itertuples(False)]

    out_plots = []
    for site in list(itertools.chain(*groups.values())):
        print(site)
        if site == 'cust_oxford':  # todo just to test, see if it is still going dry and if so handle this
            continue
        if '/' in site:
            ylab = 'draw down'
        else:
            ylab = 'percent RCPpast flow'
        temp_data = data.loc[(data.site == site) & (data.converged) & (data.rcp != 'RCPpast')]
        g = sns.FacetGrid(temp_data, row='sen', sharey=True, sharex=True,
                          row_order=['naturalised', 'current', 'pc5_80_wil_eff'])
        g.map_dataframe(sns.tsplot, time='period', unit='rcm', value='value', ci=95, condition='condition',
                        color={'RCP4.5_tym': 'b', 'RCP4.5_low_3_m': 'c', 'RCP8.5_tym': 'r', 'RCP8.5_low_3_m': 'orange'})
        order = ['RCP4.5_tym', 'RCP8.5_tym', 'RCP4.5_low_3_m', 'RCP8.5_low_3_m']
        temp = np.array(g.axes[0, 0].get_legend_handles_labels())
        tidx = [np.where(temp[1] == e)[0][0] for e in order]
        handles = temp[0, tidx]
        labels = temp[1, tidx]
        g.set_axis_labels('year', ylab)
        g.fig.legend(handles=handles, labels=labels, loc=6)
        g.fig.suptitle('cc_{}'.format(site))
        g.fig.set_size_inches((18.5, 9.5))
        out_plots.append(g)
    return out_plots


def vis_eco_min_flows(relitive_data_path):  # todo handle dry missing wells
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
    g.fig.set_size_inches((18.5, 9.5))
    g.fig.suptitle('min_flows')
    return g


def vis_relibability(relitive_data_path):  # todo handle dry missing wells
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
    g.fig.set_size_inches((18.5, 9.5))
    g.fig.suptitle('reliability')
    return g

def plot_and_save_forward_vis(outdir, relitive_data_path, meta_data_path):
    g = vis_eco_min_flows(relitive_data_path=relitive_data_path)
    g.savefig(os.path.join(outdir,'{}.png'.format(g.fig._suptitle._text)))
    g = vis_relibability(relitive_data_path)
    gs = vis_cc(relitive_data_path,meta_data_path)
    for g in gs:
        g.savefig(os.path.join(outdir,'{}.png'.format(g.fig._suptitle._text)))


if __name__ == '__main__':
    vis_cc(
        r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\forward_sw_gw\results\cc_only_to_waimak\opt_relative_data.csv",
        r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\forward_sw_gw\results\cc_only_to_waimak\opt_meta_data.csv"
    )
