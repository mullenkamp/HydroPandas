"""
Author: matth
Date Created: 3/02/2017 3:05 PM
"""

from __future__ import division
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd
from load_data import get_base_well_data_df


class WellBase(object):
    """
    base class to host 1 or more wells data.
    all attributes are considered either to be arrays or dictionaries of arrays

    """
    # default Class Attributes
    # potential databases # pd.dataframes.
    quant_data = None  # for the quanitity data
    qual_data = None  # for the quality data
    added_data_qual = None  # for any parameters calculated by subsequent scripts
    added_data_quant = None  # for any parameters calculated by subsequent scripts

    varlist = None
    all_cwms_zones = ('Zone_Name',
        'Hurunui - Waiau',
        'Lower Waitaki - South Coastal Canterbury',
        'Upper Waitaki',
        'Waimakariri',
        'Banks Peninsula',
        'Kaikoura',
        'Christchurch - West Melton',
        'Selwyn - Waihora',
        'Orari-Opihi-Pareora',
        'Ashburton')

    # location data
    lat = None  # location CRS defined in init
    lon = None  # as above
    cwms_zone = None
    depth = None  # depth from ground level

    # below will be added as needed
    ref_elv = None  # the measurement point reference elevation
    ref_to_ground = None  # distance from reference measurement elevation to ground (negative is ground level below ref)
    location_quality = None  # dictionary of values of location and reference elevation quality codes
    # KEYS: ['loc_qual': location quality code, 'ref_elv_qual': reference quality code]
    zone_data = None  # dictionary of values of zone types
    # KEYS: ['gwa_zone': ground water allocation zone, 'cwms_zone': CWMS zone (or WMCR in wells),
    #        'catchment': catchment code]

    # well characteristics
    diameter = None  # diameter of the well
    active = None  # boolean if true then they well is active
    use = None  # a list of both primary and secondary user codes
    screen_num = None  # the number of screens for a given well
    screen_info = None  # a dictionary of values of screen characteristics (which are dictionaries for the screen number)
    # KEYS: ['screen_top': screen top, 'screen_bot': screen bottom, 'screen_len': screen length]
    # each of these keys call dictionaries to handle multiple screens (e.g. {1: param for screen 1}
    # where screen 1 is the highest (to match wells database system)

    _default_time = None

    def __init__(self, well_num, startdate=None, enddate=None, CRS='NZTM'):
        """

        :param well_num:  well number from wells database
        :param startdate: for data handeling yyyy/mm/dd format string this is the period that data will be loaded for
               (inclusive) if None then load all data
        :param enddate:  as above end date for the period.
        :param CRS: the coordinate reference system to be used for well location one of: 'NZTM', 'NZMG', 'WGS84'
        """
        self.well_num = well_num
        if startdate is not None:
            self._period_s_date = dt.datetime.strptime(startdate, "%Y/%m/%d")
        else:
            self._period_s_date = None
        if enddate is not None:
            self._period_e_date = dt.datetime.strptime(enddate, "%Y/%m/%d")
        else:
            self._period_e_date = None
        self._CRS = CRS

    def get_param(self, varname):
        """
        returns the objects associated with the variable name listed (e.g. depth, ect) regarless if this data is hosted
        as a class attribute or within the quality/quantity data

        :param varname:
        :return:
        """
        out = None
        if varname in self.__dict__.keys():
            out = self.__getattribute__(varname)
        if self.qual_data is not None:
            if varname in self.qual_data.keys():
                out = self.qual_data[varname]
        if self.quant_data is not None:
            if varname in self.quant_data.keys():
                out = self.quant_data[varname]
        if self.added_data_qual is not None:
            if varname in self.added_data_qual.keys():
                out = self.added_data_qual[varname]
        if self.added_data_quant is not None:
            if varname in self.added_data_quant.keys():
                out = self.added_data_quant[varname]
        if self.zone_data is not None:
            if varname in self.zone_data.keys():
                out = self.zone_data[varname]
        if self.location_quality is not None:
            if varname in self.location_quality.keys():
                out = self.location_quality[varname]
        if self.screen_info is not None:
            if varname in self.screen_info.keys():
                out = self.screen_info[varname]
        if out is None:
            raise ValueError("{} not found, data may not have been loaded".format(varname))

        return out

    def _check_duplicate_param_name(self, varname):
        """
        a function to quickly check if a parameter name is already in the class object
        :return: boolean True if parameter name already exists
        """
        self._getvarlist()
        if varname in self.varlist:
            dup = True
        else:
            dup = False
        return dup

    def add_param_qual(self, key, val):
        if self._check_duplicate_param_name(key):
            raise ValueError('{} would be a duplicate, please choose a different parameter name')

        if self.added_data_qual is None:
            self.added_data_qual = {}

        self.added_data_qual[key] = val
        self.varlist.append(key)

    def _getvarlist(self):
        self.varlist = []
        self.varlist.extend(self.__dict__.keys())
        if self.qual_data is not None:
            self.varlist.extend(self.qual_data.keys())
        if self.quant_data is not None:
            self.varlist.extend(self.quant_data.keys())
        if self.added_data_qual is not None:
            self.varlist.extend(self.added_data_qual.keys())
        if self.added_data_quant is not None:
            self.varlist.extend(self.added_data_quant.keys())
        if self.zone_data is not None:
            self.varlist.extend(self.zone_data.keys())
        if self.location_quality is not None:
            self.varlist.extend(self.location_quality.keys())
        if self.screen_info is not None:
            self.varlist.extend(self.screen_info.keys())

    def load_base_data(self, df = None):
        """
        loads base data such as location etc from the wells database as class attributes.  It can either load from the
        SQL server or a data frame.  This functionality is to be more efficient when loading data to a list of wells
        :param df: data frame with the well data loaded from the SQL server
        :return:
        """
        if self._CRS is not "NZTM":
            raise ValueError('CRS not supported currently, only NZTM supported at this time')
        if df is None:
            df = get_base_well_data_df([self.well_num])

        self._load_base_from_df(df)
        self._getvarlist()

    def _load_base_from_df(self, df):
        # select well and check correct number of records
        temp_data = df[df['WELL_NO']==self.well_num]
        if len(temp_data.WELL_NO) == 0:
            raise ValueError('no well data found for well {}'.format(self.well_num))
        elif len(temp_data.WELL_NO) != 1:
            raise ValueError('more than one record exists for well {}'.format(self.well_num))

        # set attributes
        self.lon = temp_data.nztmx.iloc[0]
        self.lat = temp_data.nztmy.iloc[0]
        self.cwms_zone = temp_data.Zone_Name.iloc[0]
        self.depth = temp_data.Depth.iloc[0]

    def plot_tseries(self, param, time=_default_time):
        param_data = self.get_param(param)
        time_data = self.get_param(time)
        param_idx = pd.notnull(param_data)
        param_data = param_data[param_idx]
        time_data = time_data[param_idx]
        time_idx = pd.notnull(time_data)
        param_data = param_data[time_idx]
        time_data = time_data[time_idx]

        if param_data.shape != time_data.shape:
            raise ValueError('inconsistent shapes for plotting data')

        plt.plot(time_data, param_data, linestyle='--', marker='o', color='b')
        plt.ylabel(param)
        plt.xlabel("time")
        plt.show()



if __name__ == '__main__':
    # test get_param for base data held as attribute
    test = WellBase('BS28/5004')
    test.load_base_data()
    print(test.get_param('depth'))
    print(test.get_param('lat'))
    print(test.get_param('lon'))
    print(test.get_param('cwms_zone'))
