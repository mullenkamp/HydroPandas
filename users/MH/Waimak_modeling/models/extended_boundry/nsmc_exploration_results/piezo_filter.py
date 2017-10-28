# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 26/10/2017 12:40 PM
"""

from __future__ import division
from core import env
import pandas as pd
import numpy as np
import itertools
import os


if __name__ == '__main__':
    #include all wells in piezo
    peizo_wells = pd.read_csv(r"\\gisdata\projects\SCI\Groundwater\Waimakariri\Groundwater\2017 piezo survey\Piezo_LTmean_with125m_singlereadings.csv")

    print('done')