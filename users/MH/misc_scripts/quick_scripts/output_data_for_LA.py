"""
quick script to pull things out for lincoln agritech from the wells database
Author: matth
Date Created: 3/03/2017 12:25 PM
"""

from __future__ import division
from core.ecan_io import rd_sql, rd_hydstra_db
import core.ecan_io.SQL_databases as sql_db

wells_data = rd_sql(**sql_db.wells_db.well_details)

well_list = wells_data.WELL_NO[wells_data.WMCRZone == 7]

water_data = rd_sql(**sql_db.wells_db.daily)

water_data = water_data[water_data.WELL_NO.isin(well_list)]
water_data.to_csv(r"P:\Groundwater\Christchurch West Melton\GSHP_mounding_project\GW_level_data\From_ECAN_database\manual_data.csv")

aquifer_tests = rd_sql(**sql_db.wells_db.aquifer_tests)

aquifer_tests = aquifer_tests[aquifer_tests.WELL_NO.isin(well_list)]
aquifer_tests.to_csv(r"P:\Groundwater\Christchurch West Melton\GSHP_mounding_project\aquifer_test_data.csv")
