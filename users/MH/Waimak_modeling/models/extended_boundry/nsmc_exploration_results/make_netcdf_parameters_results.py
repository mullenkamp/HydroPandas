# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 27/10/2017 10:25 AM
"""

from __future__ import division
from core import env
import netCDF4 as nc
from rrfextract import extractrrf
import numpy as np

# rap up the NSMC parameters adn observations into a netcdf file
nsmc_dim = 7890
rch_dim = 46
layer_dim = 11
sfr_dim = 47
khv_dim = 178  # todo


def _add_simple_params(param, nc_file):
    simple_parameters = {'pump_c': {'units': 'none',
                                    'long_name': 'christchurch west melton pumping multiplier'},
                         'pump_s': {'units': 'none',
                                    'long_name': 'selwyn pumping multiplier'},
                         'pump_w': {'units': 'none',
                                    'long_name': 'waimakariri pumping multiplier'},
                         'sriv': {'units': 'none',
                                  'long_name': 'selwyn river influx multiplier'},
                         'n_race': {'units': 'none',
                                    'long_name': 'waimakariri race multiplier'},
                         's_race': {'units': 'none',
                                    'long_name': 'selwyn race multiplier'},
                         'nbndf': {'units': 'none',
                                   'long_name': 'northern boundary flux multiplier'},
                         'top_e_flo': {'units': 'm3/s',
                                       'long_name': 'top of the eyre flow'},
                         'mid_c_flo': {'units': 'm3/s',
                                       'long_name': 'mid cust (biwash) flow'},
                         'top_c_flo': {'units': 'm3/s',
                                       'long_name': 'top of the cust flow'},
                         'ulrzf': {'units': 'm3/s',
                                   'long_name': 'inland southwestern boundary flux'},
                         'llrzf': {'units': 'm3/s',
                                   'long_name': 'coastal southwestern boundary flux'},
                         'fkh_mult': {'units': 'none',
                                      'long_name': 'fault kh multiplier'},
                         'fkv_mult': {'units': 'none',
                                      'long_name': 'christchurch west melton pumping multiplier'}}

    for key in simple_parameters.keys():
        temp = nc_file.createVariable(key, 'f8', ('nsmc_num',), fill_value=np.nan)
        temp.setncatts({'units': simple_parameters[key]['units'],
                        'long_name': simple_parameters[key]['long_name'],
                        'missing_value': np.nan})
        temp[:] = param.loc[key].values


def _add_rch_params(param, nc_file):
    # rch ppts
    rch_ppt_ids = ['rch_ppt_{:02d}'.format(e) for e in range(46)]
    rch_pts = nc_file.createVariable('rch_ppt', str, ('rch_ppt',), fill_value='none')
    rch_pts.setncatts({'units': 'none',
                       'long_name': 'recharge pilot point identifier',
                       'comments': 'this is a unique identifier',
                       'missing_value': 'none'})
    rch_pts[:] = rch_ppt_ids

    rch_x = nc_file.createVariable('rch_ppt_x', 'f8', ('rch_ppt',), fill_value=np.nan)
    rch_x.setncatts({'units': 'nztmx',
                     'long_name': 'recharge pilot point longitude',
                     'missing_value': np.nan})
    rch_x[:] = None  # todo

    rch_y = nc_file.createVariable('rch_ppt_y', 'f8', ('rch_ppt',), fill_value=np.nan)
    rch_y.setncatts({'units': 'nztmy',
                     'long_name': 'recharge pilot point latitude',
                     'missing_value': np.nan})
    rch_y[:] = None  # todo

    rch_group = nc_file.createVariable('rch_ppt_group', 'i4', ('rch_ppt',), fill_value=-9)
    rch_group.setncatts({'units': '',  # todo set groups
                         'long_name': 'recharge pilot point groups',
                         'missing_value': -9})
    rch_group[:] = None  # todo set this

    rch_mult = nc_file.createVariable('rch_mult', 'f8', ('nsmc_num', 'rch_ppt'), fill_value=np.nan)
    rch_mult.setncatts({'units': 'none',
                        'long_name': 'recharge multipliers',
                        'missing_value': np.nan})
    temp_data = np.zeros((nsmc_dim, rch_dim)) * np.nan
    for i, key in enumerate(rch_ppt_ids):
        temp_data[:, i] = param.loc[key].values

    rch_mult[:] = temp_data


def _add_sfr_cond(param, nc_file):
    # stream hconds
    hcond_sites = ['hcond1', 'hcond2', 'hcond3', 'hcond4', 'hcond5', 'hcond6', 'hcond7', 'hcond8', 'hcond9', 'hcond10',
                   'hcond11', 'hcond12', 'hcond13', 'hcond14', 'hcond15', 'hcond16', 'hcond17', 'hcond18', 'hcond19',
                   'hcond20', 'hcond21', 'hcond22', 'hcond23', 'hcond24', 'hcond25', 'hcond26', 'hcond27', 'hcond28',
                   'hcond29', 'hcond30', 'hcond31', 'hcond32', 'hcond33', 'hcond34', 'hcond34x', 'hcond35', 'hcond35x',
                   'hcond36', 'hcond37', 'hcond38', 'hcond39', 'hcond40', 'hcond41', 'hcond42', 'hcond43', 'hcond44',
                   'hcond44x']

    sfr_cond = nc_file.createVariable('sfr_cond', str, ('sfr_cond',), fill_value='none')
    sfr_cond.setncatts({'units': 'none',
                        'long_name': 'sfr conductance identifier',
                        'missing_value': 'none'})
    sfr_cond[:] = hcond_sites

    sfr_riv = nc_file.createVariable('sfr_riv', str, ('sfr_cond',), fill_value='none')
    sfr_riv.setncatts({'units': 'none',
                       'long_name': 'river',
                       'missing_value': 'none'})
    sfr_riv[:] = None  # todo define

    sfr_cond_val = nc_file.createVariable('sfr_cond_val', 'f8', ('nsmc_num', 'sfr_cond'), fill_value=np.nan)
    sfr_cond_val.setncatts({'units': 'none',  # todo define this
                            'long_name': 'sfr conductance at points',
                            'missing_value': np.nan})

    temp_data = np.zeros((nsmc_dim, sfr_dim)) * np.nan
    for i, key in enumerate(hcond_sites):
        temp_data[:, i] = param.loc[key].values


def _add_drain_cond(param, nc_file):
    raise NotImplementedError


def _add_kv_kh(param, nc_file):
    # kh kv pilot points dimensions (nsmc_num, ppt, layer)
    ppt_ids = ['pp005602', 'pp005630', 'pp005798', 'pp010726', 'pp010754', 'pp010810', 'pp010894', 'pp010922',
               'pp015738', 'pp015794', 'pp015822', 'pp015850', 'pp015878', 'pp015906', 'pp015934', 'pp015962',
               'pp015990', 'pp016018', 'pp020834', 'pp020862', 'pp020890', 'pp020918', 'pp020946', 'pp020974',
               'pp021002', 'pp021030', 'pp021058', 'pp021086', 'pp021114', 'pp021142', 'pp025930', 'pp025958',
               'pp025986', 'pp026014', 'pp026042', 'pp026070', 'pp026098', 'pp026126', 'pp026154', 'pp026182',
               'pp026210', 'pp026238', 'pp031054', 'pp031082', 'pp031110', 'pp031138', 'pp031166', 'pp031194',
               'pp031222', 'pp031250', 'pp031278', 'pp031306', 'pp031334', 'pp031362', 'pp036150', 'pp036178',
               'pp036206', 'pp036234', 'pp036262', 'pp036290', 'pp036318', 'pp036346', 'pp036374', 'pp036402',
               'pp036430', 'pp036458', 'pp041302', 'pp041330', 'pp041358', 'pp041386', 'pp041414', 'pp041442',
               'pp041470', 'pp041498', 'pp041526', 'pp041554', 'pp041582', 'pp046398', 'pp046426', 'pp046454',
               'pp046482', 'pp046510', 'pp046538', 'pp046566', 'pp046594', 'pp046622', 'pp046650', 'pp046678',
               'pp051522', 'pp051550', 'pp051578', 'pp051606', 'pp051634', 'pp051662', 'pp051690', 'pp051718',
               'pp051746', 'pp051774', 'pp051802', 'pp056618', 'pp056646', 'pp056674', 'pp056702', 'pp056730',
               'pp056758', 'pp056786', 'pp056814', 'pp056842', 'pp056870', 'pp056898', 'pp061714', 'pp061742',
               'pp061770', 'pp061798', 'pp061826', 'pp061854', 'pp061882', 'pp061910', 'pp061938', 'pp061966',
               'pp061994', 'pp062022', 'pp066838', 'pp066866', 'pp066894', 'pp066922', 'pp066950', 'pp066978',
               'pp067006', 'pp067034', 'pp067062', 'pp067090', 'pp067118', 'pp071962', 'pp071990', 'pp072018',
               'pp072046', 'pp072074', 'pp072102', 'pp072130', 'pp072158', 'pp072186', 'pp079628', 'pp079668',
               'pp079708', 'pp079748', 'pp079788', 'pp079828', 'pp086948', 'pp086988', 'pp087028', 'pp087068',
               'pp087108', 'pp094268', 'pp094308', 'pp094348', 'pp094388', 'pp094428', 'pp101588', 'pp101628',
               'pp101668', 'pp101708', 'pp108908', 'pp108948', 'pp108988', 'pp109028', 'pp116228', 'pp116268',
               'pp116308', 'pp116348', 'pp123548', 'pp123588', 'pp123628', 'pp123668', 'pp130868', 'pp130908',
               'pp130948', 'pp130988']  # from layer 1 I assume this is all of them

    ppts = nc_file.createVariable('khv_ppt', str, ('khv_ppt',), fill_value='none')
    ppts.setncatts({'units': 'none',
                    'long_name': 'kv and kh pilot point id',
                    'missing_value': 'none'})
    ppts[:] = ppt_ids

    # X
    pptsx = nc_file.createVariable('khv_ppt_x', 'f8', ('khv_ppt',), fill_value=np.nan)
    pptsx.setncatts({'units': 'none',
                     'long_name': 'kv and kh pilot point longitude',
                     'missing_value': np.nan})
    pptsx[:] = None  # todo

    # Y
    pptsy = nc_file.createVariable('khv_ppt_y', 'f8', ('khv_ppt',), fill_value=np.nan)
    pptsy.setncatts({'units': 'none',
                     'long_name': 'kv and kh pilot point latitude',
                     'missing_value': np.nan})
    pptsy[:] = None  # todo

    # kv
    kv = nc_file.createVariable('kv', 'f8', ('nsmc_num', 'layer', 'khv_ppt'), fill_value=np.nan)
    kv.setncatts({'units': 'none',  # todo get units
                  'long_name': 'vertical conductivity',
                  'missing_value': np.nan})
    temp_data = np.zeros((nsmc_dim, layer_dim, khv_dim)) * np.nan
    for i in range(0, layer_dim):
        for k, key in enumerate(ppt_ids):
            try:
                temp_data[:, i, k] = param.loc['{}_v{}'.format(key, i + 1)]
            except KeyError:
                pass
    kv[:] = temp_data

    # kh
    kh = nc_file.createVariable('kh', 'f8', ('nsmc_num', 'layer', 'khv_ppt'), fill_value=np.nan)
    kh.setncatts({'units': 'none',  # todo get units
                  'long_name': 'horizontal conductivity',
                  'missing_value': np.nan})
    temp_data = np.zeros((nsmc_dim, layer_dim, khv_dim)) * np.nan
    for i in range(0, layer_dim):
        for k, key in enumerate(ppt_ids):
            try:
                temp_data[:, i, k] = param.loc['{}_h{}'.format(key, i + 1)]
            except KeyError:
                pass
    kh[:] = temp_data


def make_netcdf_nsmc(nc_outfile, rrffile):
    # get the data

    obs, param = extractrrf(rrffile=rrffile)

    # set up netcdf file
    nc_file = nc.Dataset(nc_outfile, 'w')
    nc_file.notes = 'phi lower and phi upper are -1 and -2, respectivly and where present will appear at the end of the nsmc_variable'

    # make dimensions
    nc_file.createDimension('nsmc_num', nsmc_dim)
    nc_file.createDimension('layer', layer_dim)
    nc_file.createDimension('rch_ppt', rch_dim)
    nc_file.createDimension('sfr_cond', sfr_dim)
    nc_file.createDimension('khv_ppt', khv_dim)


    # variables
    nsmc_num = nc_file.createVariable('nsmc_num', 'i4', ('nsmc_num',), fill_value=-9)
    nsmc_num.setncatts({'units': 'none',
                        'long_name': 'Null Space Monte Carlo Realisation Number',
                        'comments': 'unique identifier phi lower and phi upper are -1 and -2, respectively',
                        'missing_value': -9})
    nsmc_num[:] = range(1, nsmc_dim + 3)

    layer = nc_file.createVariable('layer', 'i4', ('layer',), fill_value=-9)
    layer.setncatts({'units': 'none',
                     'long_name': 'model layer',
                     'comments': '1 indexed',
                     'missing_value': -9})
    nsmc_num[:] = range(1, layer_dim + 1)

    # parameters #todo add phi low and high
    _add_simple_params(param, nc_file)

    _add_rch_params(param, nc_file)

    _add_sfr_cond(param, nc_file)

    _add_drain_cond(param, nc_file)

    _add_kv_kh(param, nc_file)

    # todo add observations
    # todo add convergance
    # todo add phis
    # todo add pass filter
