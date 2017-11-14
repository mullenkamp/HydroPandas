# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 14/11/2017 1:18 PM
"""

from __future__ import division
from core import env
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

#todo add the next level of filtering (e.g. post filter to this plot)
def plot_river_mixing(outdir):
    data = pd.read_csv(env.sci(r"Groundwater\Waimakariri\Groundwater\Numerical GW model\nsmc_results\emma_filter\river_end_member_mixing.csv"))

    target_data = pd.read_csv(r"\\gisdata\projects\SCI\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model build and optimisation\4EM_WaimakResults_NSMCFilterWithStreams.csv")
    target_data.loc[:,'Group'] = [e.lower() for e in target_data.Group]
    sites = {}
    for g in set(target_data.SuperGroup):
        sites[g] = list(set(target_data.loc[target_data.SuperGroup==g,'Group']))

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    for key, ids in sites.items():
        model_data = data.loc[:,ids].transpose()
        fig, (ax) = plt.subplots(figsize=(18.5, 9.5))
        pos = np.arange(len(ids))+1
        tar_medians = [target_data.loc[target_data.Group==e,'river_med_4EM'].iloc[0] for e in ids]
        tar_mins = [target_data.loc[target_data.Group==e,'river_5th_4EM'].iloc[0] for e in ids]
        tar_maxes = [target_data.loc[target_data.Group==e,'river_95th_4EM'].iloc[0] for e in ids]
        t = ax.boxplot(x=[[l,u] for l,u in zip(tar_mins, tar_maxes)], positions=pos - 0.33, usermedians=tar_medians, whis = 'range',
                       showbox=False, showfliers=False)
        ys = sorted(list(set([e.remove() for e in t['whiskers']])))
        [ax.plot([p-0.33, p-0.33], [l,u], alpha=0.33, color='k') for p, l, u in zip(pos,tar_mins,tar_maxes)]
        [[e.set_alpha(0.33) for e in j[1]] for j in t.items()]

        # plot the models
        plt_data = [e[np.isfinite(e)] for e in model_data.values]
        t = ax.boxplot(x=plt_data, positions=pos, labels=[e.title() for e in ids])
        [[e.set_linewidth(2) for e in j[1]] for j in t.items()]
        fig.suptitle(key.title())
        ax.set_ylabel('fraction of alpine river')
        fig.savefig(os.path.join(outdir,key))

if __name__ == '__main__':
    plot_river_mixing(r"C:\Users\MattH\Downloads\test_emma_plots")