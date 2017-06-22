"""
Author: matth
Date Created: 20/06/2017 11:59 AM
"""

from __future__ import division
from core import env
import flopy

def create_bas_package(m):
    bas = flopy.modflow.mfbas.ModflowBas(m,
                                         ibound=_get_ibound(),
                                         strt=_get_starting_heads(),
                                         ifrefm=True,
                                         ixsec=False,
                                         ichflg=False,
                                         stoper=None,
                                         hnoflo=-999.99,
                                         unitnumber=1)


def _get_ibound():#todo
    raise NotImplementedError()

def _get_starting_heads(): #todo
    raise NotImplementedError()