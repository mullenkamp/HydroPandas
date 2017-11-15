# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 13/11/2017 3:28 PM
"""

from __future__ import division
from core import env
import numpy as np
import matplotlib.pyplot as plt
import netCDF4 as nc


def gen_dist(sd_type, u, sd, n):
    if sd_type == 'log':
        out = 10 ** np.random.normal(u, sd, size=n)
    elif sd_type == 'lin':
        out = np.random.normal(u, sd, size=n)
    else:
        raise ValueError('{} is unexpected sd_type'.format(sd_type))
    return out


# todo
def _plt_parm_boxplot(ax, opt_prior, post_opt_dist, post_jac_dist, labels, filter_dist, limits, ylab, ax_title):
    # watch inputs carefully
    if not all([len(e) == len(opt_prior) for e in [opt_prior, post_opt_dist, post_jac_dist,
                                                   filter_dist, labels, limits]]):
        raise ValueError('all distributions must have the same length')

    # the simple plotting set up
    positions = np.arange(len(opt_prior)) + 1
    # plot the data
    # post_opt_dist
    t = ax.boxplot(x=post_opt_dist, positions=positions - 0.33)
    [[e.set_alpha(0.33) for e in j[1]] for j in t.items()]

    # post_jac_dist
    t = ax.boxplot(x=post_jac_dist, positions=positions)
    [[e.set_alpha(0.33) for e in j[1]] for j in t.items()]

    # filter_dist
    t = ax.boxplot(x=filter_dist, positions=positions + 0.33, )
    [[e.set_linewidth(2) for e in j[1]] for j in t.items()]

    # plot limits
    for lim, pos in zip(limits, positions):
        # plot upper
        ax.plot([pos - 0.33, pos + 0.33], [max(lim), max(lim)], color='k', linewidth=2)
        # plot lower
        ax.plot([pos - 0.33, pos + 0.33], [min(lim), min(lim)], color='k', linewidth=2)

    # opt_prior
    t = ax.boxplot(x=opt_prior, positions=positions, usermedians=opt_prior, labels=labels,
                   showcaps=False, showbox=False, showfliers=False, medianprops={'color': 'm', 'linewidth': 2})

    ax.set_xlim([0, positions[-1] + 1])

    ax.set_ylabel(ylab)
    ax.set_title(ax_title)


def _get_plting_dists(nc_file, sites, data_types, filter_array, layers):
    """

    :param nc_file: param netcdf
    :param sites: dictionary {site: [components]}
    :param data_types: array of one of rch, kv, kh, sfr, drn, simple
    :return:
    """
    opt_prior, post_opt_dist, post_jac_dist, filter_dist, labels, limits = [], [], [], [], [], []

    for data_type, site, layer in zip(data_types, sites.keys(), layers):
        labels.append(site)
        array_len = filter_array.sum()
        opt_prior_t = np.array([0])[np.newaxis, :]
        limits_t = np.array([0, 0])[np.newaxis, :]
        post_opt_dist_t = np.zeros((array_len))[np.newaxis, :]
        post_jac_dist_t = np.zeros((array_len))[np.newaxis, :]
        filter_dist_t = np.zeros((array_len))[np.newaxis, :]
        for feature in sites[site]:
            # for each feature I need
            if data_type == 'simple':
                sd_type = nc_file[feature].sd_type
                opt_p = nc_file[feature].opt_p
                p_sd = nc_file[feature].p_sd
                j_sd = nc_file[feature].j_sd
                u = nc_file[feature].initial
                up = nc_file[feature].upper
                lo = nc_file[feature].lower

                opt_prior_t = np.concatenate((opt_prior_t, [[opt_p]]), axis=0)  # axis just to show for future
                limits_t = np.concatenate((limits_t, [[lo, up]]))
                post_opt_dist_t = np.concatenate(
                    (post_opt_dist_t, gen_dist(sd_type, u, p_sd, array_len)[np.newaxis, :]))
                post_jac_dist_t = np.concatenate(
                    (post_jac_dist_t, gen_dist(sd_type, u, j_sd, array_len)[np.newaxis, :]))
                filter_dist_t = np.concatenate((filter_dist_t, np.array(nc_file[feature][filter_array])))

            else:
                if data_type == 'rch':
                    id_str = 'rch_ppt'
                    prefix = 'rch_ppt'
                    val_str = 'rch_mult'

                elif data_type == 'kv':
                    id_str = 'khv_ppt'
                    prefix = val_str = 'kv'

                elif data_type == 'kh':
                    id_str = 'khv_ppt'
                    prefix = val_str = 'kh'

                elif data_type == 'sfr':
                    # sfr_lower
                    id_str = 'sfr_cond'
                    prefix = 'sfr'
                    val_str = 'sfr_cond_val'

                elif data_type == 'drn':  # I'm not going to implement now
                    id_str = 'drns'
                    prefix = 'drn'
                    val_str = 'drn_cond'
                else:
                    raise ValueError('wrong input for data_type {}'.format(data_type))
                if data_type in ['kv', 'kh']:
                    ids = np.array(nc_file[id_str])
                    idx = ids == feature
                    sd_type = nc_file['{}_p_sd'.format(prefix)].sd_type
                    opt_p = nc_file['{}_opt_p'.format(prefix)][layer][idx][0]
                    p_sd = nc_file['{}_p_sd'.format(prefix)][layer][idx][0]
                    j_sd = nc_file['{}_j_sd'.format(prefix)][layer][idx][0]
                    u = nc_file['{}_initial'.format(prefix)][layer][idx][0]
                    up = nc_file['{}_upper'.format(prefix)][layer][idx][0]
                    lo = nc_file['{}_lower'.format(prefix)][layer][idx][0]
                    filter_dist_t = np.concatenate(
                        (filter_dist_t, np.array(nc_file[feature][filter_array, layer, idx])))
                else:
                    ids = np.array(nc_file[id_str])
                    idx = ids == feature
                    sd_type = nc_file['{}_p_sd'.format(prefix)].sd_type
                    opt_p = nc_file['{}_opt_p'.format(prefix)][idx][0]
                    p_sd = nc_file['{}_p_sd'.format(prefix)][idx][0]
                    j_sd = nc_file['{}_j_sd'.format(prefix)][idx][0]
                    u = nc_file['{}_initial'.format(prefix)][idx][0]
                    up = nc_file['{}_upper'.format(prefix)][idx][0]
                    lo = nc_file['{}_lower'.format(prefix)][idx][0]
                    filter_dist_t = np.concatenate((filter_dist_t, np.array(nc_file[feature][filter_array, idx])))

                opt_prior_t = np.concatenate((opt_prior_t, [[opt_p]]), axis=0)  # axis just to show for future
                limits_t = np.concatenate((limits_t, [[lo, up]]))
                post_opt_dist_t = np.concatenate(
                    (post_opt_dist_t, gen_dist(sd_type, u, p_sd, array_len)[np.newaxis, :]))
                post_jac_dist_t = np.concatenate(
                    (post_jac_dist_t, gen_dist(sd_type, u, j_sd, array_len)[np.newaxis, :]))

        opt_prior.append(np.nanmean(opt_prior_t[1:], axis=0))
        limits.append(np.nanmean(limits[1:], axis=0))
        post_opt_dist.append(np.nanmean(post_opt_dist_t[1:], axis=0))
        post_jac_dist.append(np.nanmean(post_jac_dist_t[1:], axis=0))
        filter_dist.append(np.nanmean(filter_dist_t[1:], axis=0))

    return opt_prior, post_opt_dist, post_jac_dist, filter_dist, labels, limits


def plot_supergroup_ppp_boxplots(filter_strs, param_nc, layers, supergroup, sites, datatypes, ylab):
    fig, axs = plt.subplots(ncols=len(filter_strs), figsize=(18.5, 9.5))
    axs = np.atleast_1d(axs)

    for filter_str_raw, ax in zip(filter_strs, axs.flatten()):
        ftype = 0
        filter_str = filter_str_raw
        textadd = ''
        if '~0_' in filter_str_raw:
            filter_str = filter_str_raw.replace('~0_', '')
            ftype = 1
            textadd = 'failed '
        elif '~-1_' in filter_str_raw:
            filter_str = filter_str_raw.replace('~-1', '')
            ftype = 2
            textadd = 'not run '
        elif '~-10_' in filter_str_raw:
            filter_str = filter_str_raw.replace('~-10', '')
            ftype = 3
            textadd = 'not run or failed '

        temp_filter = np.array(param_nc.variables[filter_str])
        if ftype == 0:
            real_filter = temp_filter == 1
        elif ftype == 1:
            real_filter = temp_filter == 0
        elif ftype == 2:
            real_filter = temp_filter == -1
        elif ftype == 3:
            real_filter = temp_filter < 1
        else:
            raise ValueError('shouldnt get here')

        # get the data
        opt_prior, post_opt_dist, post_jac_dist, filter_dist, labels, limits = _get_plting_dists(nc_file=param_nc,
                                                                                                 sites=sites,
                                                                                                 data_types=datatypes,
                                                                                                 filter_array=real_filter,
                                                                                                 layers=layers)
        _plt_parm_boxplot(ax, opt_prior, post_opt_dist, post_jac_dist, labels, filter_dist, limits, ylab,
                          ax_title='{}{}'.format(textadd,filter_str))


    ymax = max([e.get_ylim()[1] for e in axs.flatten()])
    ymin = min([e.get_ylim()[0] for e in axs.flatten()])
    [e.set_ylim(ymin, ymax) for e in axs.flatten()]
    fig.suptitle(supergroup.title())

    return fig,axs

#todo plot zebs kv and kh groups both by layer(one plot per group) and all groups by layer (one plot per layer)



"""
groups
pumping 
n races, nboundry flux, s river, south races
southern b fluxs
river inflows (may need to split up do to scale)
fault multipliers

recharge (boxes of each irrigation group) # kind of hard
recharge (boxes of each area_TBD) # kind of hard
kv/kh # very hard

"""

if __name__ == '__main__':
    fix, ax = plt.subplots()
    _plt_parm_boxplot(ax=ax, opt_prior=[[3]], post_opt_dist=[[1, 2, 2, 3]], post_jac_dist=[[2, 3, 3, 2]],
                      labels=[['t']],
                      filter_dist=[[2, 3, 3]], limits=[[1, 4]], ylab='test', ax_title='testtest')
    plt.show()
