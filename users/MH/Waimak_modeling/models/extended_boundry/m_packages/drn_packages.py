"""
Author: matth
Date Created: 20/06/2017 11:58 AM
"""

from __future__ import division
from core import env
import flopy

def create_drn_package(m,drn_version):
    flopy.modflow.mfdrn.ModflowDrn(m,
                                   ipakcb=740,
                                   stress_period_data=_get_drn_spd(drn_version),
                                   unitnumber=710)



def _get_drn_spd(version): #todo
    raise NotImplementedError()