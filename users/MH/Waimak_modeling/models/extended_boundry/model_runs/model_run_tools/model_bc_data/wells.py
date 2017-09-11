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

def get_forward_wells (full_abstraction, cc_inputs, naturalised, full_allo): #todo should pumping be altered by PC5?
    """
    gets the pumping data for the forward runs
    :param full_abstraction: use the CAV (think about what happens with irrigation abstraction)
    :param cc_inputs: use these to apply scaling factors for the pumping (think about how to work with these spatially)
    :param naturalised: boolean, if True use only the fixed inputs (e.g. rivers, boundary fluxes.  No races)
    :param full_allo: boolean, if True scale the wells by the amount allocated in each zone (could be a dictionary of boolean for each subzone)
    :return:
    """
    #todo options for cc_inputs with None
    raise NotImplementedError()