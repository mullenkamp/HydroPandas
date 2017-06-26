"""
Author: matth
Date Created: 20/06/2017 11:59 AM
"""

from __future__ import division
from core import env
import flopy
import numpy as np

from supporting_scripts import _elvdb_calc

def create_dis_package(m):
    elv_db = _elvdb_calc()
    dis = flopy.modflow.mfdis.ModflowDis(m,
                                         nlay=17,
                                         nrow=364,
                                         ncol=365,
                                         nper=1,
                                         delr=200,
                                         delc=200,
                                         laycbd=0,
                                         top=elv_db[0],
                                         botm=elv_db[1:],
                                         perlen=1,
                                         nstp=1,
                                         tsmult=1,
                                         steady=True,
                                         itmuni=4,
                                         lenuni=2,
                                         unitnumber=719,
                                         xul=1512162.53275,
                                         yul=5215083.5772,
                                         rotation=0.0,
                                         proj4_str=2193)






