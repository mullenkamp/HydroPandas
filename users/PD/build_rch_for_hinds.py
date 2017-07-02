"""
coding=utf-8
Author: matth
Date Created: 3/07/2017 11:00 AM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd
import flopy


m = flopy.modflow.Modflow()
dis = flopy.modflow.ModflowDis(m,nlay=1, nrow=2, ncol=2, nper=1, delr=1.0,
                 delc=1.0, laycbd=0, top=1, botm=0, perlen=1, nstp=1,
                 tsmult=1, steady=True, itmuni=4, lenuni=2, extension='dis',
                 unitnumber=None)
rch = flopy.modflow.ModflowRch(m, nrchop=3, ipakcb=None, rech=1e-3, irch=0,
                 extension='rch', unitnumber=None, filenames=None)
