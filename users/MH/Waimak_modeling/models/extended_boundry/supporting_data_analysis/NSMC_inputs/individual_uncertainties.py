# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 22/08/2017 12:55 PM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd


def selwyn_hillfed_uncertainty():
    """
    create selwyn hillfeds uncertainty from the gauging error and from the regression for the selwyn use the uncertainty
    from the gauging which is on the order of 10% (phil Downes personal communication)  the difference between whiteclifs
    and coalgate is on average of all concurante gaugins 0.11 m3/s so no real worries.
    :return:
    """

    uncertainty = 10


def waimaik_and_ashley_uncertainty():
    """
    use the 10% mentioned above
    :return:
    """
    uncertainty = 10


def selwyn_spring_fed_uncertainty():
    temp = {'Halswell River':-1,
            'halswell canal':-1,
            'l2 river':-1,
            'miles drain':-1,
            'mcgraths stream':-1,
            'silverstream':-1,
            'lower selwyn':-1,
            'irwell River':-1,
            'hamner road drain':-1,
            'boggy creek':-1,
            'doylston drain':-1,
            'nairns drain':-1,
            'tramway reserve drain':-1,
            'birdlings brook':-1,
            'harts creek':-1,
            'parkin drain':-1,
            'waikekewai tamutu creek':-1,
            } #todo this is wrong

    uncertainty = len(temp)**0.5 * 10 # assume 10% gauging error and propogate the uncertainty for each stream yeilds ~ # 41%
    #todo given more time I could try to account for the linear regression...