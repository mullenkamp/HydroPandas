# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/09/2017 3:55 PM
"""

from __future__ import division
from core import env

# for stream depletion things
def get_race_data(): #todo
    """
    all influx wells in the well data
    :return:
    """
    raise NotImplementedError

def get_full_consent(): #todo
    """
    EAV/consented annual volume I need to think about this one
    :return:
    """
    raise NotImplementedError

def get_max_rate(): #todo
    """
    max rate of take for all wells in the model
    :return:
    """
    raise NotImplementedError

# for forward runs

def get_forward_wells (full_abstraction, cc_inputs, naturalised):
    #todo options for cc_inputs with None
    raise NotImplementedError()