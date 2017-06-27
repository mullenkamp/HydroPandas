"""
Author: matth
Date Created: 20/06/2017 11:59 AM
"""

from __future__ import division
from core import env
import flopy
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import numpy as np

def create_bas_package(m):


    bas = flopy.modflow.mfbas.ModflowBas(m,
                                         ibound=smt.get_no_flow(),
                                         strt=create_starting_heads(),
                                         ifrefm=True,
                                         ixsec=False,
                                         ichflg=False,
                                         stoper=None,
                                         hnoflo=-999.99,
                                         unitnumber=1)


def create_starting_heads():
    #todo set constant head values
    hds = np.repeat(smt.calc_elv_db()[0][np.newaxis, :, :],
              smt.layers, axis=0),  # set to top of layer 1

    raise NotImplementedError('constant head values have not been done')
    return hds