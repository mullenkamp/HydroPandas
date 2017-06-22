"""
Author: matth
Date Created: 20/06/2017 11:57 AM
"""

from __future__ import division
from core import env
import flopy

def create_wel_package(m,wel_version):

    wel = flopy.modflow.mfwel.ModflowWel(m,
                                         ipakcb=740,
                                         stress_period_data=_get_wel_spd(wel_version),
                                         options=['AUX IFACE'],
                                         unitnumber=709)

def _get_wel_spd(version): #todo
    raise NotImplementedError()