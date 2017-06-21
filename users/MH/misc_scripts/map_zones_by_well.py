
from core.ecan_io import rd_sql
import core.ecan_io.SQL_databases as sql_db
import pandas as pd
import numpy as np
from copy import deepcopy

all_data = pd.read_excel(r"T:\Temp\Matt_Hanson_TRAN\GWL_Jan_Feb_2017.xlsx")

wmcr = rd_sql(**sql_db.wells_db.WMCR_Zones)
well_details = rd_sql(**sql_db.wells_db.well_details)

wmcr_dict = {}
for key, val in zip(wmcr.Zone_Code, wmcr.Zone_Name):
    wmcr_dict[key] = val

cwms = np.zeros(well_details.WELL_NO.shape,dtype=object)

for i,val in enumerate(well_details.WMCRZone):
    if pd.isnull(val):
        cwms[i] = val
        continue
    cwms[i] = wmcr_dict[val]
well_details['CWMS'] = cwms

well_to_zone_dict = {}
for key, val in zip(well_details.WELL_NO, well_details.CWMS):
    well_to_zone_dict[key] = val

alldata_cwms = np.zeros(all_data.WELL_NO.shape,dtype=object)

for i, val in enumerate(all_data.WELL_NO):
    alldata_cwms[i] = well_to_zone_dict[val.strip()]
all_data['CWMS'] = alldata_cwms

jan = all_data[all_data.Month == 1]
feb = all_data[all_data.Month == 2]

jan.to_csv(r"T:\Temp\Matt_Hanson_TRAN\GWL_Jan_2017.csv")
feb.to_csv(r"T:\Temp\Matt_Hanson_TRAN\GWL_Feb_2017.csv")