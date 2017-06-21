"""
quick script to export all recorder data on a well list for lincon agritech
Author: matth
Date Created: 3/03/2017 12:39 PM
"""

from __future__ import division
from core.ecan_io import rd_hydstra_db
import pandas as pd
import numpy as np

well_list = pd.read_csv(r"P:\Groundwater\Christchurch West Melton\GSHP_mounding_project\possible_record_wells.csv")
well_list = well_list.WELL_NO.str.replace('/','_')

for well in well_list:
    well = np.array([well])
    try:
        data, qual = rd_hydstra_db(well,data_type='point',varfrom=110,varto=110,interval='minute', qual_code_export=True, return_qual=True)
        data['qual'] = qual[0]
        data.to_csv("P:/Groundwater/Christchurch West Melton/GSHP_mounding_project/GW_level_data/recorder data/{}_recorder_data.csv".format(well[0]))
    except Exception as val:
        print('{}: {}'.format(well, val))