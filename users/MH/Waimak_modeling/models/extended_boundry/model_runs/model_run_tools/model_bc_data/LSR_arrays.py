# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 8/09/2017 8:28 AM
"""

from __future__ import division
from core import env

def get_forward_rch(naturalised, pc5=False, rcm=None, rcp=None, period=None, amag_type=None): #todo
    """
    get the rch for the forward runs
    :param naturalised: boolean if True then get rch for
    :param rcm: regional Climate model identifier
    :param rcp: representetive carbon pathway identifier
    :param period: e.g. 2010, 2020, ect
    :param amag_type: the amalgamation type one of: tym: ten year mean,
                                                    min: minimum annual average,
                                                    low_3_m: average of the 3 lowest consecutive years
    :param pc5: boolean if true use assumed PC5 efficency
    :return: rch array (11,364,365)
    """
    raise NotImplementedError