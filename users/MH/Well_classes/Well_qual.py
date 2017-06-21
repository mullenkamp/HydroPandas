"""
Author: matth
Date Created: 7/02/2017 10:15 AM
"""

from __future__ import division

from MAV_and_GV import mav, gv
from Well_base import WellBase
from load_data import read_from_pc_squalarc_dataframe, read_from_pc_squalarc_csv_to_dataframe
from users.MH.Well_classes.load_data.qual_parameters import qual_parameters

class WellQual(WellBase):
    _default_time = "qual_time"
    mav = mav
    gv = gv

    def load_qual_data(self, param_list, path = None, data_frame=None):
        """
        external load data function that can either load from the database or from an excel sheet(squalarc output).
        loads only the parameters found in param_list.  Raises warnings for parameters that are not found in
        the database or excel sheet
        :param path: path to output from pc squalarc (sorted by date and halving < values)
        :param param_list
        :return:
        """
        if data_frame is None and path is None:
            self._load_qual_from_database(param_list)
        elif data_frame is not None:
            self._load_qual_from_dataframe(data_frame)
        else:
            self._load_qual_from_csv(path, param_list)
        self._getvarlist()

    def _load_qual_from_csv(self, path, param_list):
        data_frame = read_from_pc_squalarc_csv_to_dataframe(path,param_list)
        self._load_qual_from_dataframe(data_frame)

    def _load_qual_from_dataframe(self, data_frame):
        self.qual_data = read_from_pc_squalarc_dataframe(self.well_num, data_frame, start_date=self._period_s_date,
                                                         end_date=self._period_e_date)

    def _load_qual_from_database(self, param_list):
        raise ValueError('load from database not yet implemented')

if __name__ == '__main__':
    print('test')
    param_list = qual_parameters.keys()
    test = WellQual('K38/1017')
    test.load_qual_data(param_list ,path = r"P:\Groundwater\Annual groundwater quality survey 2016\qual_data2016-2017.csv")
