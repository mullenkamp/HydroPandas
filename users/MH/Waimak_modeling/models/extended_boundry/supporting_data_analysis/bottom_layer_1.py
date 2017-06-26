"""
coding=utf-8
Author: matth
Date Created: 26/06/2017 2:35 PM
"""

from __future__ import division
from core import env
from core.ecan_io import rd_sql, sql_db
import numpy as np
import pandas as pd
from core.classes.hydro import hydro


def get_mean_water_level():
    well_details_org = rd_sql(**sql_db.wells_db.well_details)
    well_details = well_details_org[(well_details_org['WMCRZone'] == 4) | (well_details_org['WMCRZone'] == 7) |
                                (well_details_org['WMCRZone'] == 8)]  # keep only waimak selwyn and chch zones
    well_details = well_details[well_details['Well_Status'] == 'AE']
    well_details = well_details[pd.notnull(well_details['DEPTH'])]

    well_details = well_details.set_index('WELL_NO')

    data = hydro().get_data(mtypes=['gwl'], sites=list(well_details.index), from_date='2008-01-01',
                            to_date='2016-01-01')
    data1 = hydro().get_data(mtypes=['gwl_m'], sites=list(well_details.index), from_date='2008-01-01',
                            to_date='2016-01-01')
    temp

    data1 = data1.loc[:,sites]

    print 'done'
    raise NotImplementedError


if __name__ == '__main__':
    get_mean_water_level()