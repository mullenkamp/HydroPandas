"""
Author: matth
Date Created: 20/06/2017 11:59 AM
"""

from __future__ import division
from core import env
import flopy

def create_rch_package(m, rch_version):
    rch = flopy.modflow.mfrch.ModflowRch(m,
                                         nrchop=3,
                                         ipakcb=740,
                                         rech=_get_rch(rch_version),
                                         irch=0,
                                         unitnumber=716)



def _get_rch(version): #todo
    # get new rch values for nwai
    # create rch values for south wai
    raise NotImplementedError()