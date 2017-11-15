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
import netCDF4 as nc
import os

# todo add and and or to the filter options
def no_change(x, **kwargs):
    return x


def is_dry(x, layer):
    no_flow = smt.get_no_flow(layer)
    no_flow[no_flow < 0] = 0
    elv_db = smt.calc_elv_db()
    temp = (x<elv_db[layer+1]) & no_flow.astype(bool)
    return temp  # todo check


def neg_zero_pos(x, **kwargs):
    lower_lim = 0  # todo play with these
    upper_lim = 0
    outdata = np.zeros(x.shape, np.int8)
    outdata[x < lower_lim] = -1
    outdata[x > upper_lim] = 1
    return outdata.astype(np.float16)


def plot_sd_mean_multid(filter_strs, layer, nc_param_data, nc_obs_data, data_id, function_adjust,
                        title, basemap=True, contour={'sd': True, 'mean': True, 'sum': True}, method='mean_sd',
                        contour_color='g'):
    filter_strs = np.atleast_1d(filter_strs)
    param_nc_nums = np.array(nc_param_data.variables['nsmc_num'])
    obs_nc_nums = np.array(nc_obs_data.variables['nsmc_num'])

    # get the data
    plot_data_mean = {}
    plot_data_sd = {}
    plot_data_sum = {}
    textadds = {}
    use_filter_strs = []
    mean_maxes = []
    mean_mins = []
    sd_maxes = []
    sd_mins = []
    sum_maxes = []
    sum_mins = []

    for filter_str_raw in filter_strs:
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
        temp_filter_full = dict(zip(param_nc_nums, np.array(nc_param_data.variables[filter_str])))
        temp_filter = np.array([temp_filter_full[e] for e in obs_nc_nums])
        if ftype == 0:
            real_filters = temp_filter == 1
        elif ftype == 1:
            real_filters = temp_filter == 0
        elif ftype == 2:
            real_filters = temp_filter == -1
        elif ftype == 3:
            real_filters = temp_filter < 1
        else:
            raise ValueError('shouldnt get here')
        use_filter_strs.append(filter_str)

        real_filters = np.where(real_filters)[0]
        # pull the data out
        if nc_obs_data.variables[data_id].ndim == 4:
            print 'starting to pull data'
            import time
            t= time.time()
            temp = nc_obs_data.variables[data_id][real_filters, layer, :, :]
            print 'pull_masked {}'.format((time.time()-t)/60)
            temp = function_adjust(np.array(nc_obs_data.variables[data_id][real_filters, layer]), layer=layer)
            print 'pull_unmasked {}'.format((time.time()-t)/60)
            print 'data pulled'
            raise
        elif nc_obs_data.variables[data_id].ndim == 3:
            temp = function_adjust(np.array(nc_obs_data.variables[data_id][real_filters]), layer=0)
        else:
            raise ValueError('{} does not have 3 or 4 dims'.format(data_id))

        mean = np.nanmean(temp, axis=0)
        sd = np.nanstd(temp, axis=0)
        dsum = np.nansum(temp, axis=0)
        plot_data_mean[filter_str] = mean
        plot_data_sd[filter_str] = sd
        plot_data_sum[filter_str] = dsum
        sum_maxes.append(np.nanmax(dsum))
        sum_mins.append(np.nanmin(dsum))
        mean_maxes.append(np.nanmax(mean))
        mean_mins.append(np.nanmin(mean))
        sd_maxes.append(np.nanmax(sd))
        sd_mins.append(np.nanmin(sd))

    # initalised teh figure
    if method == 'mean_sd':
        nrows = 2
    elif method == 'sum':
        nrows = 1
    else:
        raise ValueError('unexpected value for method')
    fig, axs = plt.subplots(nrows=nrows, ncols=len(filter_strs), figsize=(18.5, 9.5))
    axs = np.atleast_2d(axs)

    mean_vmax = max(mean_maxes)
    mean_vmin = min(mean_mins)
    sd_vmax = max(sd_maxes)
    sd_vmin = min(sd_mins)
    sum_vmin = min(sum_mins)
    sum_vmax = max(sum_maxes)

    for i, fstr in enumerate(use_filter_strs):
        plt_cbar = False
        cbar_lab_sd = None
        cbar_lab_mean = None
        cbar_lab_sum = None
        if i == len(use_filter_strs) - 1:
            plt_cbar = True
            cbar_lab_sd = 'Std'
            cbar_lab_mean = 'Mean'
            cbar_lab_sum = 'Sum'

        if method == 'mean_sd':
            meanax = axs[0, i]
            sdax = axs[1, i]
            # todo contour the data (what color), include basemap?
            smt.plt_matrix(plot_data_mean[fstr], vmin=mean_vmin, vmax=mean_vmax,
                           title='{} mean'.format(fstr),
                           no_flow_layer=0, ax=meanax, color_bar=plt_cbar, base_map=basemap,
                           cbar_lab=cbar_lab_mean)

            if contour['mean']:
                x, y = smt.get_model_x_y()
                meanax.contour(x, y, plot_data_mean[fstr], colors=contour_color)

            smt.plt_matrix(plot_data_sd[fstr], vmin=sd_vmin, vmax=sd_vmax,
                           title='{} sd'.format(fstr),
                           no_flow_layer=0, ax=sdax, color_bar=plt_cbar, base_map=basemap,
                           cbar_lab=cbar_lab_sd)
            if contour['sd']:
                x, y = smt.get_model_x_y()
                sdax.contour(x, y, plot_data_sd[fstr], colors=contour_color)

        elif method == 'sum':
            sumax = axs[0, i]
            smt.plt_matrix(plot_data_sum[fstr], vmin=sum_vmin, vmax=sum_vmax,
                           title='{} sum'.format(fstr),
                           no_flow_layer=0, ax=sumax, color_bar=plt_cbar, base_map=basemap,
                           cbar_lab=cbar_lab_sum)

            if contour['sum']:
                x, y = smt.get_model_x_y()
                sumax.contour(x, y, plot_data_sum[fstr], colors=contour_color)

        else:
            raise ValueError("shouldn't get here")

    fig.suptitle(title.title())
    return fig, axs


def plot_all_2d_obs(outdir):
    nc_param_data = nc.Dataset(r"K:\mh_modeling\netcdfs_of_key_modeling_data\nsmc_params_obs_metadata.nc")
    nc_hds_data = nc.Dataset(r"K:\mh_modeling\netcdfs_of_key_modeling_data\post_filter1_hds.nc")
    nc_bud_data = nc.Dataset(r"K:\mh_modeling\netcdfs_of_key_modeling_data\post_filter1_cell_budgets.nc")

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # hds by layer
    filter_strs = ['filter2', 'filter3']
    for l in range(smt.layers):
        title = 'heads for layer: {:02d}'.format(l)
        fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=l,
                                       nc_param_data=nc_param_data, nc_obs_data=nc_hds_data,
                                       data_id='heads', function_adjust=no_change,
                                       title=title,
                                       basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                       method='mean_sd', contour_color='g')

        fig.savefig(os.path.join(outdir, title))
        plt.close()

    # drn flux
    title = 'drain flux'
    fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=0,
                                   nc_param_data=nc_param_data, nc_obs_data=nc_bud_data,
                                   data_id='drains', function_adjust=no_change,
                                   title=title,
                                   basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                   method='mean_sd', contour_color='g')

    fig.savefig(os.path.join(outdir, title))
    plt.close()

    # layer one bottom flux (as 1 0 -1)
    title = 'layer 1 up vs downflow'
    fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=0,
                                   nc_param_data=nc_param_data, nc_obs_data=nc_bud_data,
                                   data_id='FLOW LOWER FACE'.lower(), function_adjust=neg_zero_pos,
                                   title=title,
                                   basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                   method='mean_sd', contour_color='g')

    fig.savefig(os.path.join(outdir, title))
    plt.close()

    # where the model is dry
    for l in range(smt.layers):
        title = 'drycells for layer: {:02d}'.format(l)
        fig, axs = plot_sd_mean_multid(filter_strs=filter_strs, layer=l,
                                       nc_param_data=nc_param_data, nc_obs_data=nc_hds_data,
                                       data_id='heads', function_adjust=is_dry,
                                       title=title,
                                       basemap=True, contour={'sd': True, 'mean': True, 'sum': True},
                                       method='mean_sd', contour_color='g')

        fig.savefig(os.path.join(outdir, title))
        plt.close()


# todo implement and check
if __name__ == '__main__':
    plot_all_2d_obs(r"T:\Temp\temp_gw_files\test2dplots")