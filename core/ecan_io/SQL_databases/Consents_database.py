"""
Author: matth
Date Created: 28/04/2017 10:52 AM
"""

from __future__ import division
from core import env

class consent_db(object):
    """
    class to hold input kwargs for rd.sql
    """
    # ground and surface water consents
    gw_sw = {'server': 'SQL2012PROD03', 'database': 'DataWarehouse', 'table': 'D_ACC_ActivityAttribute_TakeWaterCombined'}
