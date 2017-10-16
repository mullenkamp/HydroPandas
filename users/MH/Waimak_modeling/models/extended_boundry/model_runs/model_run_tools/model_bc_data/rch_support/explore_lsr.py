# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 27/09/2017 2:17 PM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.rch_packages import _get_rch
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_bc_data.LSR_arrays import \
    get_lsrm_base_array, get_lsr_base_period_inputs
import itertools
import os
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt


def plt_cc_rch(naturalised, pc5, title):
    periods = range(2010, 2100, 20)
    rcps = ['RCP4.5', 'RCP8.5']
    rcps_lines = {'RCP4.5': '-', 'RCP8.5': '--'}
    rcms = ['BCC-CSM1.1', 'CESM1-CAM5', 'GFDL-CM3', 'GISS-EL-R', 'HadGEM2-ES', 'NorESM1-M']
    rcm_colors = {'BCC-CSM1.1': 'k', 'CESM1-CAM5': 'r', 'GFDL-CM3': 'g', 'GISS-EL-R': 'b', 'HadGEM2-ES': 'orange',
                  'NorESM1-M': 'purple'}
    sen = 'current'
    if naturalised:
        sen = 'nat'
    if pc5:
        sen = 'pc5'

    fig, ax = plt.subplots(figsize=(18.5, 9.5))
    ax.set_title(title)
    ax.set_xlabel('year')
    ax.set_ylabel('mean LSR / mean opt LSR')
    at = 'period_mean'
    # get data
    for rcp in rcps:
        linestl = rcps_lines[rcp]
        for rcm in rcms:
            color = rcm_colors[rcm]
            data = []
            for period in periods:
                temp = get_lsrm_base_array(sen, rcp, rcm, period, at)
                base_rch = get_lsrm_base_array(**get_lsr_base_period_inputs(sen, rcp, rcm, period, at))
                data.append(np.nanmean(temp / base_rch))
            data = np.array(data)
            ax.plot(periods, data, linestyle=linestl, color=color, label=rcp)
    ax.legend()
    return fig, ax


def plt_vcsn():
    raise NotImplementedError
    # check the vcsn data but have already done a bit of this


def comp_amalg_types(rcm, rcp, naturalised=False, pc5=False):
    periods = range(2010, 2100, 20)
    amalg_types = ['period_mean', '3_lowest_con_mean', 'lowest_year']
    des = 'current_practice'
    sen = 'current'
    if naturalised:
        des = 'naturalised'
        sen = 'nat'
    if pc5:
        des = 'pc5'
        sen = 'pc5'

    fig, ax = plt.subplots(figsize=(18.5, 9.5))
    ax.set_title('{}_{}_{}'.format(rcm, rcp, des))
    ax.set_xlabel('year')
    ax.set_ylabel('mean LSR / mean opt LSR')

    for at, color in zip(amalg_types, ['k', 'r', 'b']):
        data = []
        for period in periods:
            temp = get_lsrm_base_array(sen, rcp, rcm, period, at)
            base_rch = get_lsrm_base_array(**get_lsr_base_period_inputs(sen, rcp, rcm, period, at))
            data.append(np.nanmean(temp / base_rch))
        data = np.array(data)
        ax.plot(periods, data, color=color, label=at)
    ax.legend()
    return fig, ax


def plt_change_maps(base_dir):
    rcps = ['RCP4.5', 'RCP8.5']
    rcms = ['BCC-CSM1.1', 'CESM1-CAM5', 'GFDL-CM3', 'GISS-EL-R', 'HadGEM2-ES', 'NorESM1-M']
    sens = ['current', 'pc5', 'nat']
    amalg_types = ['period_mean', '3_lowest_con_mean', 'lowest_year']
    per = 2090
    no_flow = smt.get_no_flow()
    no_flow[no_flow < 0] = 0
    no_flow = no_flow.astype(bool)
    for rcp, rcm, sen, at in itertools.product(rcps, rcms, sens, amalg_types):
        mod_rch = get_lsrm_base_array(sen, rcp, rcm, per, at)
        base_rch = get_lsrm_base_array(**get_lsr_base_period_inputs(sen, rcp, rcm, per, at))
        plt_data = mod_rch / base_rch
        plt_data[~no_flow] = np.nan
        ttl = 'cc_vs_rcppast_{}-{}-{}-{}-{}'.format(sen, rcp, rcm, per, at)
        mapfig, mapax = smt.plt_matrix(plt_data, title=ttl, cmap='RdBu')  # todo set vmin/vmax if problems develop
        mapfig.savefig(os.path.join(base_dir, '{}.png'.format(ttl)))
        plt.close(mapfig)

def plt_ts_at_comp(base_dir):
    # 100%
    fig, ax = plt_cc_rch(False, True, 'pc5')
    fig.savefig(os.path.join(base_dir, ax.title._text + '.png'))
    # 80%
    fig2, ax2 = plt_cc_rch(False, False, 'no_change')
    fig2.savefig(os.path.join(base_dir, ax2.title._text + '.png'))

    # Nat
    fig3, ax3 = plt_cc_rch(True, False, 'naturalised')
    fig3.savefig(os.path.join(base_dir, ax3.title._text + '.png'))

    # amalg types
    rcps = ['RCP4.5', 'RCP8.5']
    rcms = ['BCC-CSM1.1', 'CESM1-CAM5', 'GFDL-CM3', 'GISS-EL-R', 'HadGEM2-ES', 'NorESM1-M']
    for rcm, rcp in itertools.product(rcms, rcps):
        fig, ax = comp_amalg_types(rcm, rcp)
        fig.savefig(os.path.join(base_dir, ax.title._text + '.png'))
    for rcm, rcp in itertools.product(rcms, rcps):
        fig, ax = comp_amalg_types(rcm, rcp, True)
        fig.savefig(os.path.join(base_dir, ax.title._text + '.png'))


if __name__ == '__main__':
    # todo run this when I get new data
    base_dir_all = env.sci(
        "Groundwater\Waimakariri\Groundwater\Numerical GW model\Model simulations and results\ex_bd_va\lsr_checks")
    # note this means RCP past is not tested, and vcsn only minimally tested
    base_dir_comp = os.path.join(base_dir_all, 'comparisons')
    if not os.path.exists(base_dir_comp):
        os.makedirs(base_dir_comp)
    base_dir_maps = os.path.join(base_dir_all, 'maps')
    if not os.path.exists(base_dir_maps):
        os.makedirs(base_dir_maps)
    test_type = [0, 1]

    if 0 in test_type:
        plt_ts_at_comp(base_dir_comp)
    if 1 in test_type:
        plt_change_maps(base_dir_maps)
