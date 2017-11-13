# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 13/11/2017 4:59 PM
"""

from __future__ import division
from core import env
import netCDF4 as nc
import matplotlib.pyplot as plt
import numpy as np
from collections import OrderedDict
import os

def obs_boxplots(param_netcdf_file, sites, title, yax, filter_str):
    """

    :param param_netcdf: the parameter netcdf with obs ect (nc object)
    :param sites: an ordered dictionary of lists first list hold everything to plot on the x axis,
                  the second is added together these are string coresponding to the netcdf variable
                  eg. {'eyre': [sfo_e],'eyre2':[sfo_e2],'ohoka':[oho_mill, oho_misc]}
    :param title: title to use
    :param yax: label for the y axis
    :param filter_str: which filter should I use 'str'
    :return: fig, ax
    """
    if not isinstance(sites, OrderedDict):
        raise ValueError('expected ordered dictionary')

    fig, (ax) = plt.subplots(figsize=(18.5, 9.5))

    real_filter = np.array(param_netcdf_file.variables[filter_str])>0
    nsmc_nums = np.array(param_netcdf_file.variables['nsmc_num'][real_filter])
    all_model_data = []
    all_tar_data = []
    # get the data
    # get the target
    # get the weight/sd
    for site in sites.values():
        model_data = np.zeros(nsmc_nums.shape)
        tar_data = np.zeros(nsmc_nums.shape)
        for subsite in site:
            model_data += np.array(param_netcdf_file[subsite][real_filter])
            u = param_netcdf_file[subsite].target
            wt = param_netcdf_file[subsite].nsmc_weight
            if wt != 0:
                tar_data += np.random.normal(loc=u, scale=1 / wt, size=len(tar_data))
        if yax == 'm3/s':  # then convert to m3/s from m3/day
            model_data *= 1 / 86400
            tar_data *= 1 / 86400
        all_model_data.append(model_data)
        all_tar_data.append(tar_data)


    # plot the thing
    positions = np.arange(len(all_model_data)) + 1
    # plot the data
    t = ax.boxplot(x=all_tar_data, positions=positions - 0.33)
    [[e.set_alpha(0.33) for e in j[1]] for j in t.items()]
    t = ax.boxplot(x=all_model_data, positions=positions, labels=sites.keys())
    [[e.set_linewidth(2) for e in j[1]] for j in t.items()]
    fig.suptitle(title.title())
    ax.set_ylabel(yax)

    return fig, ax

def plot_all_obs_boxplots(nc_path, outdir):
    nc_data = nc.Dataset(nc_path)

    plots = {
        # tile                                  site 1 name         st comps
        'total waimakariri losses': OrderedDict([('Waimakariri', ['sfx_w_all'])]),

        'middle ashley losses': OrderedDict([('Ashley', ['mid_ash_g'])]),

        'silver stream flow at island rd': OrderedDict([('Silver Stream', [u'd_sil_harp', u'd_sil_heyw', u'd_sil_ilnd', ])]),

        'ohoka flow at island rd': OrderedDict([('Ohoka', [u'd_oho_btch', u'd_oho_jefs', u'd_oho_kpoi', u'd_oho_misc',
                                                           u'd_oho_miscz', u'd_oho_mlbk', u'd_oho_whit', ])]),

        'cust flow at threlkelds rd': OrderedDict([('Cust', [u'sfo_c_tlks'])]),

        'cam at youngs rd': OrderedDict([('Cam', [u'd_cam_mrsh', u'd_cam_yng', u'd_nbk_mrsh', u'd_sbk_mrsh'])]),
        'taranaki at stokes': OrderedDict([('Taranaki', [u'd_tar_gre', u'd_tar_stok', ])]),

        'waikuku at leggitts': OrderedDict([('Waikuku', [u'd_kuku_leg', ])]),

        'saltwater at factory rd': OrderedDict([('Saltwater', [u'd_salt_fct', u'd_salt_s', u'd_salt_top'])]),

        'total selwyn gains': OrderedDict([('Selwyn Steams', [u'sel_str'])]),

        'eyre river targets': OrderedDict([('Wollffs Rd', [u'sfo_e_wolf']),
                                           ('Poyntzs Rd', ['sfo_e_poyn']),
                                           ('Downs Rd', ['sfo_e_down']),
                                           ('S. Eyre Rd', ['sfo_e_seyr'])]),

        'Carpet Drains': OrderedDict([('Ashley Carpet', ['d_ash_c']),
                                      ('Cust Carpet', ['d_cust_c'])]),

        'Total Chch Flow': OrderedDict([('Chch Streams', ['chch_str'])]),

        'Coastal Discharge': OrderedDict([('Ashley', ['chb_ash']),
                                          ('Cust', ['chb_cust']),
                                          ('Chch', ['chb_chch']),
                                          ('Selwyn', ['sel_off'])]),

        'Inland Vertical Targets': OrderedDict([(e, [e]) for e in [u'brnthl_4_1',
                                                                   u'eyrftn_2_1',
                                                                   u'eyrftn_6_2',
                                                                   u'eyrftn_6_1',
                                                                   u'eyrftm_2_1',
                                                                   u'eyrftl_7_2',
                                                                   u'oxfds_6_2',
                                                                   u'oxfdnr_3_1',
                                                                   u'oxfdnr_4_3',
                                                                   u'oxfdnr_4_1',
                                                                   ]]),

        'Coastal Vertical Targets': OrderedDict([(e, [e]) for e in [u'wdnd_4_2',
                                                                    u'wdnd_8_4',
                                                                    u'wdnd_8_2',
                                                                    u'peg_8_7',
                                                                    u'peg_9_8',
                                                                    u'peg_10_9',
                                                                    u'peg_10_7',
                                                                    u'chch_4_2'
                                                                    ]]),

        'Waimakariri Springfed Streams': OrderedDict([('Saltwater', [u'd_salt_fct', u'd_salt_s', u'd_salt_top']),
                                                      ('Waikuku', [u'd_kuku_leg', ]),
                                                      ('Taranaki', [u'd_tar_gre', u'd_tar_stok', ]),
                                                      ('Cam',
                                                       [u'd_cam_mrsh', u'd_cam_yng', u'd_nbk_mrsh', 'd_sbk_mrsh']),
                                                      ('Ohoka',
                                                       [u'd_oho_btch', u'd_oho_jefs', u'd_oho_kpoi', u'd_oho_misc',
                                                        u'd_oho_miscz', u'd_oho_mlbk', u'd_oho_whit', ]),
                                                      ('Silver Stream', [u'd_sil_harp', u'd_sil_heyw', u'd_sil_ilnd', ])
                                                      ]),
    }

    for key, val in plots.items():
        if key in ['Coastal Vertical Targets', 'Inland Vertical Targets']:
            ylab = 'm'
        else:
            ylab = 'm3/s'

        fig, ax = obs_boxplots(param_netcdf_file=nc_data,
                               sites=val, title=key, yax=ylab,
                               filter_str='filter1')  # todo update when run filter 5

        # save figures
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        fig.savefig(os.path.join(outdir, fig._suptitle._text))




# todo
# avon heathcote vs styks otikino
# hds (zeb will provide groups)
# nice to have how obs change vs filters

if __name__ == '__main__':
    plot_all_obs_boxplots(r"K:\mh_modeling\netcdfs_of_key_modeling_data\nsmc_params_obs_metadata.nc",
                          r"C:\Users\MattH\Downloads\test_plot_params")