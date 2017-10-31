# -*- coding: utf-8 -*-
"""
Author: MattH
Date Created: 31/10/2017 4:27 PM
"""

from __future__ import division
from core import env
from extended_boundry_model_tools import smt
import numpy as np

if __name__ == '__main__':
    shp_path = None #todo define
    variable_name = None #todo define this the name should be a string of the column name and the values should be an integer for this purpose
    out_path = None #todo

    outdata = smt.shape_file_to_model_array(shp_path,variable_name,True)
    no_flow = smt.get_no_flow(0)
    no_flow[no_flow<0] = 0
    outdata[~no_flow.astype(bool)] = -1
    outdata = outdata.astype(int)
    np.savetxt(out_path, outdata)
