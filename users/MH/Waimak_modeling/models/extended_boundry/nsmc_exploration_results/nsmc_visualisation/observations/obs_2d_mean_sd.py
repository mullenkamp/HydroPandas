# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 13/11/2017 5:07 PM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import matplotlib.pyplot as plt
import numpy as np

# todo add and and or to the filter options
def no_change(x, **kwargs):
    return x

def isnan_flow(x,layer):
    no_flow = smt.get_no_flow(layer)
    no_flow[no_flow<0] = 0
    temp = np.isnan(x) & ~no_flow.astype(bool)
    return temp #todo check

def neg_zero_pos(x):
    lower_lim = 0 #todo play with these
    upper_lim = 0
    outdata = np.zeros(x.shape,np.int8)
    outdata[x<lower_lim] = -1
    outdata[x>upper_lim] = 1
    return outdata.astype(np.float16)




def plot_sd_mean_multid(filter_strs, layer, nc_param_data, nc_obs_data, data_id, function_adjust,
                  title, basemap=True, contour={'sd':True,'mean':True}):


    filter_strs = np.atleast_1d(filter_strs)
    param_nc_nums = np.array(nc_param_data.variables['nsmc_num'])
    obs_nc_nums = np.array(nc_obs_data.variables['nsmc_num'])

    # get the data
    plot_data_mean = {}
    plot_data_sd = {}
    textadds = {}
    real_filters = {}
    use_filter_strs = []
    mean_maxes = []
    sd_maxes = []
    mean_mins = []
    sd_mins = []

    for filter_str_raw, in filter_strs:
        # id the filter interpretation
        if '~0_' in filter_str_raw:
            filter_str = filter_str_raw.replace('~0_', '')
            ftype = 1
            textadds[filter_str] = 'failed '
        elif '~-1_' in filter_str_raw:
            filter_str = filter_str_raw.replace('~-1', '')
            ftype = 2
            textadds[filter_str] = 'not run '
        elif '~-10_' in filter_str_raw:
            filter_str = filter_str_raw.replace('~-10', '')
            ftype = 3
            textadds[filter_str] = 'not run or failed '
        else:
            ftype = 0
            filter_str = filter_str_raw
            textadds[filter_str] = ''

        # get the filters and handle the possibly different dimensions between the two netcdfs
        temp_filter_full = dict(zip(param_nc_nums,np.array(nc_param_data.variables[filter_str])))
        temp_filter = [temp_filter_full[e] for e in obs_nc_nums]
        if ftype == 0:
            real_filters[filter_str] = temp_filter == 1
        elif ftype == 1:
            real_filters[filter_str] = temp_filter == 0
        elif ftype == 2:
            real_filters[filter_str] = temp_filter == -1
        elif ftype == 3:
            real_filters[filter_str] = temp_filter < 1
        else:
            raise ValueError('shouldnt get here')
        use_filter_strs.append(filter_str)

        # pull the data out
        if nc_obs_data.variables[data_id].ndim == 3:
            temp = function_adjust(nc_obs_data.variables[data_id][real_filters, layer], layer=layer)
        elif nc_obs_data.variables[data_id].ndim == 2:
            temp = function_adjust(nc_obs_data.variables[data_id][real_filters],layer=0)
        else:
            raise ValueError('{} does not have 2 or 3 dims'.format(data_id))

        mean = np.nanmean(temp, axis=0)
        sd = np.nanstd(temp, axis=0)
        plot_data_mean[filter_str] = mean
        plot_data_sd[filter_str] = sd
        mean_maxes.append(np.nanmax(mean))
        mean_mins.append(np.nanmin(mean))
        sd_maxes.append(np.nanmax(sd))
        sd_mins.append(np.nanmin(sd))

    # initalised teh figure
    fig, axs = plt.subplots(nrows=2, ncols=len(filter_strs), figsize=(18.5, 9.5))
    axs = np.atleast_2d(axs)

    mean_vmax = max(mean_maxes)
    mean_vmin = min(mean_mins)
    sd_vmax = max(sd_maxes)
    sd_vmin = min(sd_mins)

    for i, fstr in enumerate(use_filter_strs):
        plt_cbar=False
        cbar_lab_sd = None
        cbar_lab_mean = None
        if i==len(use_filter_strs)-1:
            plt_cbar = True
            cbar_lab_sd = 'Std'
            cbar_lab_mean = 'mean'
        meanax = axs[0,i]
        sdax = axs[1,i]
        #todo contour the data (what color), include basemap?
        smt.plt_matrix(plot_data_mean[fstr], vmin=mean_vmin, vmax=mean_vmax,
                       title='{} mean'.format(fstr),
                       no_flow_layer=0, ax=meanax, color_bar=plt_cbar, base_map=basemap,
                       cbar_lab=cbar_lab_mean)

        if contour['mean']:
            x,y = smt.get_model_x_y()
            meanax.contour(x,y,plot_data_mean[fstr], colors='g')

        smt.plt_matrix(plot_data_sd[fstr], vmin=sd_vmin, vmax=sd_vmax,
                       title='{} sd'.format(fstr),
                       no_flow_layer=0, ax=sdax, color_bar=plt_cbar, base_map=basemap,
                       cbar_lab=cbar_lab_sd)
        if contour['sd']:
            x,y = smt.get_model_x_y()
            sdax.contour(x,y,plot_data_sd[fstr], colors='g')

    fig.suptitle(title.title())
    return fig, axs




#todo implement and check
#hds by layer
#drn flux
#layer one bottom flux (as 1 0 -1)
# where the model is dry
# could set up where you pass the data id, layer, and a transform function I like this
