# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/11/2017 9:03 AM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import get_wel_spd
import numpy as np
import pandas as pd


def make_end_mixing_load_layers(outpath, version):
    """
    makes a pandas data frame for load layers for MT3d for the endmember mixing stuffs
    :param version: either 'inland' or 'alpine_river'
    :return:
    """

    wells = get_wel_spd(3)

    if version == 'inland':
        outwells = wells.loc[np.in1d(wells.type, ['river', 'boundry_flux'])]
    elif version == 'alpine_river':
        outwells = wells.loc[wells.type == 'race']
    else:
        raise ValueError('unexpected version entry {}'.format(version))
    outwells = outwells.loc[:, ['layer', 'row', 'col']].astype(int)
    outwells += 1
    outwells.loc[:, 'conc'] = 1.0
    outwells.loc[:, 'type'] = 2
    outwells.index.name='well'
    outwells.to_csv(outpath, sep=' ')


if __name__ == '__main__':
    make_end_mixing_load_layers(r"C:\Users\Public\Desktop\alpine_river_wells.txt", 'alpine_river')
    make_end_mixing_load_layers(r"C:\Users\Public\Desktop\inland_wells.txt", 'inland')
