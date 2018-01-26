# -*- coding: utf-8 -*-
"""
Created on Thu Jan 25 11:22:51 2018

@author: MichaelEK
"""

import numpy as np
import pandas as pd
from hydropandas.io.tools.mssql import rd_sql
from hydropandas.io.tools.sql_arg_class import sql_arg

#######################################
### Parameters

server = 'sql2012dev01'
database = 'Hydro'
all_data_table = 'TSDataNumeric'
daily_data_table = 'TSDataNumericDaily'
hourly_data_table = 'TSDataNumericHourly'

unique_stmt = 'select distinct Site, FeatureMtypeSourceID from '

gw_core_dict = {'WELL_NO': 'EcanSiteID', 'Well_Status': 'Status', 'Date_Drilled': 'DateEstablished', 'Date_Defunct': 'DateDecommissioned', 'OWNER': 'Owner', 'ROAD_OR_STREET': 'StreetAddress', 'LOCALITY': 'Locality', 'LOCATION': 'Description', 'NZTMX': 'NZTMX', 'NZTMY': 'NZTMY'}
gw_attrs = ['WELL_TYPE', 'DRILLER', 'DATE_DRILLED', 'DRILL_METHOD', 'DRILL_METHOD_SECONDARY', 'CASING_MATERIAL', 'DEPTH', 'MEASURED_REPORTED', 'Depth_Proposed', 'DIAMETER', 'REFERENCE_DESCRIPTION', 'GROUND_RL', 'REFERENCE_RL', 'QAR_RL', 'ALTITUDE_MEAS_REP', 'AQUIFER_TYPE', 'AQUIFER_NAME', 'MULTI_AQUIFER', 'PUMP_TYPE', 'PUMP_DEPTH', 'INITIAL_SWL', 'GEOPHYSICAL', 'Strata', 'Isotope', 'WaterUse', 'Diviner', 'DivinerUsed', 'DateOffProposed', 'GWLevelAccess', 'GWuseAlternateWell', 'QComment']

sw_qual_core_dict = {'SITE_ID': 'EcanSiteID', 'LocationDescription': 'Description', 'NZTMX': 'NZTMX', 'NZTMY': 'NZTMY', 'HazardText': 'Hazards'}
sw_quan_attrs = ['MinimumFlowSite', 'ContactDetails', 'HSiteNumber', 'HPN', 'LAWA_SitId']

sw_quan_core_dict = {'SiteNumber': 'EcanSiteID', 'Operational': 'Status', 'Owner': 'Owner', 'LocationDescription': 'Description', 'NZTMX': 'NZTMX', 'NZTMY': 'NZTMY'}
sw_qual_attrs = ['SiteType', 'D_LZ_Port_Key', 'BgaugingSiteIndex2', 'BgaugingSiteIndex3', 'SOF', 'Landuse_code', 'Reference_Site', 'Referenced_to_site', 'StrH_SampleDescription', 'StrH_Oldnumber']

#######################################
### All unique sites/hydro_ids

all_data_sites = rd_sql(server, database, stmt= unique_stmt + all_data_table)
hourly_data_sites = rd_sql(server, database, stmt= unique_stmt + hourly_data_table)
daily_data_sites = rd_sql(server, database, stmt= unique_stmt + daily_data_table)

all_sites_ids = pd.concat([all_data_sites, hourly_data_sites, daily_data_sites]).drop_duplicates().sort_values(['Site', 'FeatureMtypeSourceID'])

all_sites = pd.DataFrame(all_sites_ids.Site.unique(), columns=['Site'])


######################################
### Master Site Tables data

sql1 = sql_arg()

gw_cols = gw_attrs.copy()
gw_cols.extend(list(gw_core_dict.keys()))

gw_args = sql1.get_dict('well_details')
gw_args.update({'col_names': gw_cols})

gw_tab = rd_sql(**sql1.get_dict('well_details'))







































































