"""
Author: matth
Date Created: 7/09/2017 4:19 PM
"""

from __future__ import division
from core import env


def get_starting_heads_sd150(model_id):
    hds = _get_no_pumping_ss_hds(model_id)
    return hds

def get_starting_heads_sd30(model_id):
    hds = _get_no_pumping_ss_hds(model_id)
    return hds

def get_starting_heads_sd7(model_id):
    hds = _get_no_pumping_ss_hds(model_id)
    return hds

def _get_no_pumping_ss_hds(model_id): #todo
    #todo add a pickel option?
    raise NotImplementedError

def get_ss_sy(): #todo
    raise NotImplementedError