"""
quick script to change the ccc well level data to our lyttleton mean sea level
Author: matth
Date Created: 6/03/2017 2:35 PM
"""

from __future__ import division
import pandas as pd
from glob import glob

def ccc_to_well_no(ccc_path, overview_path):
    overview = pd.read_excel(overview_path)
    ccc_to_well_dict = {}
    overview.WELL_NO = overview.WELL_NO.str.replace('/', '_')
    for key, val in zip(overview.WELL_NAME,overview.WELL_NO):
        if pd.isnull(val):
            continue
        ccc_to_well_dict[key] = val

    with open(ccc_path) as f:
        ccc_name = f.readline()
        ccc_name = ccc_name.split('(')[-1]
        ccc_name = ccc_name.strip()
        ccc_name = ccc_name.strip(')')

    ccc_raw = pd.read_csv(ccc_path, skiprows=1)
    ccc_raw['Level(CCC) m'] += -9.043
    ccc_raw = ccc_raw.rename(columns={'Level(CCC) m': 'level (Lyttelton 1937)'})
    ccc_raw.to_csv("P:/Groundwater/Christchurch West Melton/GSHP_mounding_project/GW_level_data/CCC_data/{}.csv".format(ccc_to_well_dict[ccc_name]),
                   index=False)

if __name__ == '__main__':
    path_list = glob("P:/Groundwater/Christchurch West Melton/GSHP_mounding_project/GW_level_data/CCC_data/original_data/GWSites20170306/*.csv")

    for path in path_list:
        try:
            ccc_to_well_no(path,r"P:\Groundwater\Christchurch West Melton\GSHP_mounding_project\GW_level_data\CCC_data\original_data\CCC Monitoring Sites from Wells.xlsx")
        except Exception as val:
            print("{}:{}".format(path,val))
