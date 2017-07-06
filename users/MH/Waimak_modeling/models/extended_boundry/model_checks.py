# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 4/07/2017 1:28 PM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.sfr2_packages import _get_reach_data
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import get_wel_spd
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.drn_packages import _get_drn_spd
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import pandas as pd
import matplotlib.pyplot as plt

#todo check there is no overlapping bcs, check there are no bcs in constant head/no_flow

def check_no_overlapping_features():
    no_flow = smt.get_no_flow()
    no_flow[no_flow<0]=0
    no_flow = pd.DataFrame(smt.model_where(~no_flow.astype(bool)), columns=['k','i','j'])


    sfr_data = pd.DataFrame(_get_reach_data(smt.reach_v))

    well_data = get_wel_spd(smt.wel_version)
    well_data = well_data.rename({'row':'i', 'col':'j','layer': 'k'})

    drn_data = _get_drn_spd(smt.reach_v,smt.wel_version)

    all_data = pd.concat((no_flow,sfr_data,well_data,drn_data)) #todo check this is correct!

    if any(all_data.duplcated(['i','j','k'])):
        raise ValueError ('There are duplicates')


def check_layer_overlap():
    for i in range(smt.layers):
        fig, ax = smt.plt_matrix(smt.check_layer_overlap(use_elv_db=True,layer=i,required_overlap=0.50),title='layer {}'.format(i))
        plt.show(fig)


#todo check there is at least 50% cell overlap in all of the layers (check implemented in smt)

if __name__ == '__main__':
    check_no_overlapping_features()