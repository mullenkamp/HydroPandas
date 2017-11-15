# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 15/11/2017 2:20 PM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd


def calc_objective_function(modeled, target, weights, groups, group_weights):
    """

    :param modeled: array of modeled values (nsmc, value)
    :param target: array of target values (value,)
    :param weights: array of un-altered weights (value)
    :param groups: array of group ids or None (no group weighting) (value)
    :param group_weights: dictionary of group ids: multiplicitive weight or None no group weighting
    :return:
    """
    # some checks

    if not(groups is None and group_weights is None) and not (groups is not None and group_weights is not None):
        raise ValueError('groups and groupweights must both be None or Not none')

    if set(groups) != set(group_weights.keys()):
        raise ValueError('all group ids must be in keys')

    residuals = modeled-target[np.newaxis, :]

    if groups is not None:
        weights = np.array([w*group_weights[g] for w,g in zip(weights, groups)])
    weights = weights[np.newaxis, :]
    phi = np.power(weights*residuals, 2).sum()

    return phi


def create_emma_phis(): #todo
    data = pd.read_csv(env.sci(r"Groundwater\Waimakariri\Groundwater\Numerical GW model\nsmc_results\emma_filter\river_end_member_mixing.csv"))

    target_data = pd.read_csv(r"\\gisdata\projects\SCI\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model build and optimisation\4EM_WaimakResults_NSMCFilterWithStreams.csv")
    target_data.loc[:,'Group'] = [e.lower() for e in target_data.Group]
