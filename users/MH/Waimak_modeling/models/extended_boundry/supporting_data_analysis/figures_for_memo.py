# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 2/08/2017 1:46 PM
"""

from __future__ import division
from core import env
import pandas as pd
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import matplotlib.pyplot as plt
from glob import glob
from matplotlib.colors import from_levels_and_colors

khpaths = glob(env.sci(r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\Model build and optimisation\InitialParamaters\pilot_pts_2-8-2017\KH_ppk_*.txt"))
cmap, norm = from_levels_and_colors([-1, 0, 1, 2], ['blue', 'black', 'white'])

for path in khpaths:
    data =pd.read_table(path,names=['site','x','y','layer','val'])
    layer = int(path.split('_')[-1].split('.')[0]) -1
    no_flow = smt.get_no_flow(layer)
    fig,ax = smt.plt_matrix(no_flow,title='KV and KH pilot points layer {}'.format(layer),no_flow_layer=None,cmap=cmap,
                            color_bar=False)
    ax.scatter(data.x,data.y,c='red')
    plt.show(fig) #todo save these somewhere

#todo plots of head targets
