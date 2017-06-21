"""
quick script to pull out baro data from a recorder site for nicole
Author: matth
Date Created: 9/03/2017 10:05 AM
"""

from __future__ import division
from core.ecan_io import rd_hydstra_db
import pandas as pd
import numpy as np

well = ['M35_5144']
data, qual = rd_hydstra_db(well, data_type='point',varfrom=111,varto=111,interval='minute', qual_code_export=True, return_qual=True, min_qual=50)

data['qual'] = qual[0]
data.to_csv("P:\Groundwater\Nicole\M35_5144_baro.csv")