# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 8/09/2017 8:28 AM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools.model_setup.realisation_id import get_base_rch
#todo how to handle parameterisation! particularly with the RCH layer
#todo make sure model id is pusshed thourgh
def get_forward_rch(model_id, naturalised, pc5=False, org_efficency=None, rcm=None, rcp=None, period=None, amag_type=None): #todo
    """
    get the rch for the forward runs
    :param model_id: which NSMC realisation to use
    :param naturalised: boolean if True then get rch for
    :param org_efficency: the original percent efficiency to use when calculating the reduction of pc5, 80,65,50 are
                          implemented for current states, but only 80% is implemented for climate change senarios
    :param rcm: regional Climate model identifier
    :param rcp: representetive carbon pathway identifier
    :param period: e.g. 2010, 2020, ect
    :param amag_type: the amalgamation type one of: tym: ten year mean,
                                                    min: minimum annual average,
                                                    low_3_m: average of the 3 lowest consecutive years
    :param pc5: boolean if true use assumed PC5 efficency (only applied to the WILS and {something} areas)
    :return: rch array (11,364,365)
    """
    # I think I need to apply everything as a percent change or somehow normalise the rch so that I do not get any big
    # changes associated with changes in models which created the recharge array.
    #todo PC5 only applied to surface water schemes assume no change for GW schemes


    name_convention_current = '{base_dir}/vcsn_climate/{rch|ird}_{current|pc5|nat}.txt'
    name_convention_cc = '{base_dir}/climate_change/{RCP}/{RCM}/{current|pc5|nat}/{rch|ird}_{10yrm|3yrm|low}_{period}.txt'
    from warnings import warn
    warn('get forward rch not complete, returning original rch array for debugging')
    rch = get_base_rch(model_id) # place holder for now
    return rch