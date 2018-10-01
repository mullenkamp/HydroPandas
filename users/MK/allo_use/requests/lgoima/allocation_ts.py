# -*- coding: utf-8 -*-
"""
Created on Mon Oct  1 09:55:07 2018

@author: michaelek
"""
import numpy as np
import pandas as pd
from pdsql import mssql

####################################
### Test parameters

server = 'sql2012test01'
database = 'hydro'
allo_table = 'CrcAllo'
wap_allo_table = 'CrcWapAllo'

from_date = '1999-07-01'
to_date = '2003-06-30'
freq = 'A-JUN'
restr_type = 'annual volume'
remove_months=False

###################################
### Functions

status_codes = ['Terminated - Replaced', 'Issued - Active', 'Terminated - Surrendered', 'Terminated - Cancelled', 'Terminated - Expired', 'Terminated - Lapsed', 'Issued - s124 Continuance']

use_type_dict = {'Aquaculture': 'agriculture', 'Dairy Shed (Washdown/Cooling)': 'agriculture', 'Intensive Farming - Dairy': 'agriculture', 'Intensive Farming - Other (Washdown/Stockwater/Cooling)': 'agriculture', 'Intensive Farming - Poultry': 'agriculture', 'Irrigation - Arable (Cropping)': 'agriculture', 'Irrigation - Industrial': 'agriculture', 'Irrigation - Mixed': 'agriculture', 'Irrigation - Pasture': 'agriculture', 'Irrigation Scheme': 'agriculture' , 'Viticulture': 'agricutlure', 'Community Water Supply': 'water_supply', 'Domestic Use': 'water_supply', 'Construction': 'industrial', 'Construction - Dewatering': 'industrial', 'Cooling Water (non HVAC)': 'industrial', 'Dewatering': 'industrial', 'Gravel Extraction/Processing': 'industrial', 'HVAC': 'industrial', 'Industrial Use - Concrete Plant': 'industrial', 'Industrial Use - Food Products': 'industrial', 'Industrial Use - Other': 'industrial', 'Industrial Use - Water Bottling': 'industrial', 'Mining': 'industrial', 'Firefighting ': 'municipal', 'Firefighting': 'municipal', 'Flood Control': 'municipal', 'Landfills': 'municipal', 'Stormwater': 'municipal', 'Waste Water': 'municipal', 'Stockwater': 'agriculture', 'Snow Making': 'industrial', 'Augment Flow/Wetland': 'other', 'Fisheries/Wildlife Management': 'other', 'Other': 'other', 'Recreation/Sport': 'other', 'Research (incl testing)': 'other', 'Power Generation': 'hydroelectric'}

restr_type_dict = {'max rate': 'max_rate_crc', 'daily volume': 'daily_vol', 'annual volume': 'feav'}


def allo_ts_apply(row, from_date, to_date, freq, restr_col, remove_months=False):
    """
    Pandas apply function that converts the allocation data to a monthly time series.
    """

    crc_from_date = pd.Timestamp(row['from_date'])
    crc_to_date = pd.Timestamp(row['to_date'])
    start = pd.Timestamp(from_date)
    end = pd.Timestamp(to_date)

    if crc_from_date > start:
        start = crc_from_date
    if crc_to_date < end:
        end = crc_to_date

    end_date = end - pd.DateOffset(hours=1) + pd.tseries.frequencies.to_offset(freq)
    dates1 = pd.date_range(start, end_date, freq=freq)
    if remove_months and 'A' not in freq:
        mon1 = np.arange(row['from_month'], 13)
        mon2 = np.arange(1, row['to_month'] + 1)
        in_mons = np.concatenate((mon1, mon2))
        dates1 = dates1[dates1.month.isin(in_mons)]
    dates2 = dates1 - pd.tseries.frequencies.to_offset(freq)
    diff_days1 = (dates1 - dates2).days.values
    diff_days2 = diff_days1.copy()

    if freq in ['A-JUN', 'D', 'W']:
        vol1 = row[restr_col]
    elif 'M' in freq:
        vol1 = dates1.daysinmonth.values / 365.0 * row[restr_col]
    else:
        raise ValueError("freq must be either 'A-JUN', 'M', or 'D'")

    if len(diff_days1) == 1:
        diff_days2[0] = diff_days1[0] - (dates1[-1] - end).days - (diff_days1[0] - (dates1[0] - start).days)
    else:
        diff_days2[0] = (dates1[0] - start).days + 1
        diff_days2[-1] = diff_days1[-1] - (dates1[-1] - end).days
    ratio_days = diff_days2/diff_days1

    vols = pd.Series((ratio_days * vol1).round(), index=dates1)

    return vols


def allo_filter(allo, wap_allo, from_date='1900-07-01', to_date='2020-06-30', in_allo=True):
    """
    Function to take an allo DataFrame and filter out the consents that cannot be converted to a time series due to missing data.
    """
    allo.loc[:, 'to_date'] = pd.to_datetime(allo.loc[:, 'to_date'], errors='coerce')
    allo.loc[:, 'from_date'] = pd.to_datetime(allo.loc[:, 'from_date'], errors='coerce')
    allo1 = allo[allo.take_type.isin(['Take Surface Water', 'Take Groundwater'])]

    wap_allo1 = wap_allo[(wap_allo.take_type == 'Take Surface Water') & (wap_allo.in_sw_allo)].crc.unique()

    ### Remove consents without daily volumes (and consequently yearly volumes)
    allo2 = allo1[allo1.daily_vol.notnull()]

    ### Remove consents without to/from dates or date ranges of less than a month
    allo3 = allo2[allo2['from_date'].notnull() & allo2['to_date'].notnull()]

    ### Restrict dates
    start_time = pd.Timestamp(from_date)
    end_time = pd.Timestamp(to_date)

    allo4 = allo3[(allo3['to_date'] - start_time).dt.days > 31]
    allo5 = allo4[(end_time - allo4['from_date']).dt.days > 31]

    allo5 = allo5[(allo5['to_date'] - allo5['from_date']).dt.days > 31]

    ### Restrict by status_details
    allo6 = allo5[allo5.crc_status.isin(status_codes)]

    ### In allocation columns
    if in_allo:
        allo6 = allo6[(allo6.take_type == 'Take Surface Water') | ((allo6.take_type == 'Take Groundwater') & (allo6.in_gw_allo))]
        allo6 = allo6[(allo6.take_type == 'Take Groundwater') | allo6.crc.isin(wap_allo1)]

    ### Index the DataFrame
    allo7 = allo6.set_index(['crc', 'take_type', 'allo_block']).copy()

    ### Return
    return allo7


def allo_ts_proc(server, database, allo_table, wap_allo_table, from_date, to_date, freq, restr_type, remove_months=False, in_allo=True):
    """
    Combo function to completely create a time series from the allocation DataFrame.
    """

    allo = mssql.rd_sql(server, database, allo_table)
    wap_allo = mssql.rd_sql(server, database, wap_allo_table)

    allo = allo.drop_duplicates(subset=['crc', 'take_type', 'allo_block'])

    allo2 = allo_filter(allo, wap_allo, from_date=from_date, to_date=to_date, in_allo=in_allo)

    restr_col = restr_type_dict[restr_type]

    allo3 = allo2.apply(allo_ts_apply, axis=1, from_date=from_date, to_date=to_date, freq=freq, restr_col=restr_col, remove_months=remove_months)

    allo4 = allo3.stack()
    allo4.index.set_names(['crc', 'take_type', 'allo_block', 'date'], inplace=True)
    allo4.name = 'allo'

    return allo4