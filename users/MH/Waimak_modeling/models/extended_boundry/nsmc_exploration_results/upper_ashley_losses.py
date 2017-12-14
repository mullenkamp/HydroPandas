# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 15/12/2017 10:08 AM
"""

from __future__ import division
from core import env
import netCDF4 as nc
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import numpy as np

if __name__ == '__main__':
    mask = smt.shape_file_to_model_array(r"{}\m_ex_bd_inputs\raw_sw_samp_points\sfr\other\ashley_brodricks.shp".format(smt.sdp),'k',True)
    mask = np.isfinite(mask)
    idxs = smt.model_where(mask)
    data = nc.Dataset(r"C:\mh_waimak_model_data\post_filter1_budget.nc")


    'stream leakage'

