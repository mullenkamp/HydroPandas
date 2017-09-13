# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 12/09/2017 6:26 PM
"""

from __future__ import division
from core import env
# todo the below is copied from the previous work, It may take a minute, but I can work through this.

import numpy as np
import pandas as pd
import flopy
import pickle
import os
import glob
from users.MH.Waimak_modeling.supporting_data_path import sdp
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from data_at_wells import _get_kstkpers

def get_flux_at_points(sites, kstpkpers=None, rel_kstpkpers=None, name_file_path=None, hds_path=None, m=None):
    # same as flow at points for drain only points
    sites = np.atleast_1d(sites)
    sw_samp_pts_df = get_samp_points_df()
    sw_samp_pts_dict = _get_sw_samp_pts_dict()

    if not set(sites).issubset(sw_samp_pts_df.index):
        raise NotImplementedError('sites: {} not implemented'.format(set(sites)-set(sw_samp_pts_df.index)))



    bud_file = None #todo this is a bit of a bigie

    kstpkpers = _get_kstkpers(bud_file=bud_file,kstpkpers=kstpkpers,rel_kstpkpers=rel_kstpkpers)
    kstpkper_names = ['{}_{}'.format(e[0],e[1]) for e in kstpkpers]
    outdata = pd.DataFrame(index=sites, columns=kstpkper_names)

    for kstpkper in kstpkpers:
        for site in sites:
            raise NotImplementedError #todo start here.






    raise NotImplementedError


def get_flow_at_points(sites):
    # same as flux at points for drain only points
    raise NotImplementedError


def get_con_at_points():
    raise NotImplementedError


def _get_flux_flow_arrays(site, sw_samp_pts_dict, sw_samp_pts_df):
    """
    same as get_flux_arrays for drn only points
    :return: (drn_array, sfr_array) either could be None but not both
    """
    drn_array, sfr_array = None, None
    if site not in sw_samp_pts_df.index:
        raise NotImplementedError('{} not implemented'.format(site))

    if sw_samp_pts_df.loc[site, 'm_type'] == 'comp':
        raise NotImplementedError
    else:
        if sw_samp_pts_df.loc[site, 'bc_type'] == 'drn':
            drn_array = sw_samp_pts_dict[site]
        if sw_samp_pts_df.loc[site, 'bc_type'] == 'sfr':
            sfr_array = sw_samp_pts_dict[site]
        if sfr_array is not None and drn_array is not None:
            raise ValueError('returned both sfr and drn array, when non component site passed')

    if sfr_array is None & drn_array is None:
        raise ValueError('shouldnt get here')

    return drn_array, sfr_array


def get_samp_points_df(recalc=False):
    """
    generate a dataframe with useful info about sampling points
    bc_type: drn or sfr
    m_type: min_flow, swaz, comp (component), other
    n: number of points
    comps: if None not a combination if valuse the group of other combination of multiple to use for the flux arrays

    :param recalc: normal pickle thing
    :return:
    """
    # create a dataframe linking identifiers with key information (e.g. sfr vs drain, flow point, flux point, swaz, etc, number of sites.)
    pickle_path = "{}/sw_samp_pts_info.p".format(smt.pickle_dir)
    if os.path.exists(pickle_path) and not recalc:
        outdata = pickle.load(open(pickle_path))
        return outdata

    outdata = pd.DataFrame(columns=['bc_type', 'm_type', 'n', 'comps'])

    identifiers = {
        'drn_min_flow': {'path': "{}/m_ex_bd_inputs/raw_sw_samp_points/drn/min_flow/*.shp".format(smt.sdp),
                         'bc_type': 'drn',
                         'm_type': 'min_flow',
                         'comps': None},

        'sfr_min_flow': {'path': "{}/m_ex_bd_inputs/raw_sw_samp_points/sfr/min_flow/*.shp".format(smt.sdp),
                         'bc_type': 'sfr',
                         'm_type': 'min_flow',
                         'comps': None},
        'drn_swaz': {'path': "{}/m_ex_bd_inputs/raw_sw_samp_points/drn/swaz/*.shp".format(smt.sdp),
                     'bc_type': 'drn',
                     'm_type': 'swaz',
                     'comps': None},

        'sfr_swaz': {'path': "{}/m_ex_bd_inputs/raw_sw_samp_points/sfr/swaz/*.shp".format(smt.sdp),
                     'bc_type': 'sfr',
                     'm_type': 'swaz',
                     'comps': None}
    }

    for key, vals in identifiers.items():
        paths = glob.glob(vals['path'])
        names = [os.path.basename(e).strip('.shp') for e in paths]
        for itm in ['bc_type', 'm_type', 'n', 'comps']:
            outdata.loc[names, itm] = vals[itm]

    samp_dict = _get_sw_samp_pts_dict(recalc)
    for itm in outdata.index:
        outdata.loc[itm, 'n'] = samp_dict[itm].sum()

    pickle.dump(outdata, open(pickle_path, mode='w'))
    return outdata


def _get_sw_samp_pts_dict(recalc=False):
    """
    gets a dictionary of boolean arrays for each sampling point.  These are ultimately derived from shape files, but
    if possible this function will load a pickled dictionary
    :param recalc: bool if True then the pickled dictionary (if any) will not be re-loaded and instead the dictionary
                   will be calculated from all avalible shapefiles (in base_shp_path)
    :return: dictionary {location: masking array}
    """
    pickle_path = "{}/sw_samp_pts_dict.p".format(smt.pickle_dir)
    if os.path.exists(pickle_path) and not recalc:
        sw_samp_pts = pickle.load(open(pickle_path))
        return sw_samp_pts

    # load all shapefiles in base_shp_path
    base_shp_path = "{}/m_ex_bd_inputs/raw_sw_samp_points/*/*/*.shp".format(smt.sdp)
    temp_lst = glob.glob(base_shp_path)
    temp_kys = [os.path.basename(e).strip('.shp') for e in temp_lst]

    shp_dict = dict(zip(temp_kys, temp_lst))

    sw_samp_pts = {}
    for loc, path in shp_dict.items():
        temp = np.isfinite(smt.shape_file_to_model_array(path, 'k', alltouched=True))
        sw_samp_pts[loc] = temp

    pickle.dump(sw_samp_pts, open(pickle_path, mode='w'))
    return sw_samp_pts









# some changes in sfr require some changes here.

def streamflow_for_kskps(model_base_path, kstpkpers=None, drn_points=None, str_points=None):
    """
    get stream flows at points (specified by drn and str points for streams issued as drains and as stream respectivly)
    returns a pandas dataframe
    :param model_base_path: the base path to the model files (e.g. the name file without the extension)
    :param kstpkpers: either tuple (kstp, kper) or list of said tuples if none returns for all kstp,kper in the file
    :param drn_points: list of identifies for drain flow measurement points if None returns all avalible points) pass
                       an empty list to return none
    :param str_points: as above but for stream boundry conditions.
    :return: stream flows in model units pandas dataframe with multiindex of (kstp,kper) and columns of str and drn points
    """

    drn_samp_pts_dict = get_drn_samp_pts_dict()

    if drn_points is None:
        drn_points = get_drn_samp_pts()
    else:
        if not set(drn_points).issubset(get_drn_samp_pts()):
            raise ValueError('unexpected drn sample points: {}'.format(set(drn_points) - set(get_drn_samp_pts())))

    if str_points is None:
        str_points = get_str_sample_points()
    else:
        if not set(str_points).issubset(get_str_sample_points()):
            raise ValueError('unexpected str sample points: {}'.format(set(str_points) - set(get_str_sample_points())))

    drain_cbc = flopy.utils.CellBudgetFile('{}.cbd'.format(model_base_path))

    str_str_cbc = flopy.utils.CellBudgetFile('{}.csts'.format(model_base_path))

    str_model_cbc = flopy.utils.CellBudgetFile('{}.cstm'.format(model_base_path))

    if kstpkpers is None:
        kstpkpers = drain_cbc.get_kstpkper()

    kstpkpers = np.atleast_2d(kstpkpers)  # sort out the possiblity of only one kstpkper
    kstpkpers = [tuple(e) for e in kstpkpers]
    # set up multi index pandas dataframe (kstp, kper, columns=all the drain and stream points calculated)

    idx = pd.MultiIndex.from_tuples(kstpkpers, names=['kstp', 'kper'])
    outdata = pd.DataFrame(index=idx, columns=str_points + drn_points)
    for kstpkper in kstpkpers:
        kstp = kstpkper[0]
        kper = kstpkper[1]
        # create hydrograph data for drains
        drn_flow = drain_cbc.get_data(kstpkper=kstpkper, full3D=True)[0][0]
        for drn in drn_points:
            outdata.loc[kstp, kper].loc[drn] = _get_drn_flow(drn, drn_flow, drn_samp_pts_dict)

        # create hydrograph data for streams
        str_flow = str_str_cbc.get_data(kstpkper=kstpkper, full3D=True)[0][0]
        str_model_flow = str_model_cbc.get_data(kstpkper=kstpkper, full3D=True)[0][0]
        for stream in str_points:
            outdata.loc[kstp, kper].loc[stream] = _get_str_flow(stream, str_flow, str_model_flow, drn_flow,
                                                                drn_samp_pts_dict)
    return outdata


def _get_drn_flow(drn, drn_flows, drain_flow_dict):
    """
    function to get stream flow from drains or sd from stream to model flow
    :param drn:
    :param drn_flows:
    :param drain_flow_dict:
    :return:
    """
    mask_array = drain_flow_dict[drn]

    if drn_flows[mask_array].mask.sum() != 0:
        raise ValueError('masked values returned for {}'.format(drn))

    water_volume = drn_flows[mask_array].data.sum() * -1
    return water_volume


def _get_str_flow(stream, str_to_str_flow, str_to_model_flow, drn_flow, drn_samp_dict):
    """
    large wrapper to calculate any flows as stream points
    :param stream: the key for the location to calcuate the stream flow or sd
    :param str_to_str_flow: the 2d array of the stream to stream flow
    :param str_to_model_flow:  the 2d array of the stream to model flow
    :param drn_flow:  the 2d array of the drain to model flow
    :param drn_samp_dict: the drain sampling dictionary that links mask arrays to drain sample points
    :return:
    """

    str_function = _str_to_function_dict()[stream]
    water_volume = str_function(str_flow=str_to_str_flow, drn_flow=drn_flow, drn_samp_dict=drn_samp_dict,
                                str_model_flow=str_to_model_flow)

    return water_volume


def get_str_sample_points():
    """
    get the keys for the set up stream functions
    :return:
    """
    temp = _str_to_function_dict()
    return temp.keys()


def _str_to_function_dict():
    """
    map stream functions (below) to the stream name
    :return:
    """
    str_to_function = {
        'eyre_warren_hyg': _calc_eyre_warren_hydrograph,
        'ashley_sh1_hyg': _calc_ashley_sh1_hydrograph,
        'cust_therelkelds_hyg': _calc_cust_therelkelds_hydrograph,
        'cust_oxfordrd_hyg': _calc_cust_oxfordrd_hydrograph,
        'num7drn_cust_hyg': _calc_num7drain_hydrograph,
        'kaiapoi_waimak_hyg': _calc_kaiapoi_waimak_hydrograph,
        'eyre_swaz_sd': _calc_eyre_swaz_sd,
        'ashley_swaz_sd': _calc_ashley_swaz_sd,
        'custmain_swaz_sd': _calc_custmain_swaz_sd,
        'custr_swaz_sd': _calc_custr_swaz_sd,
        'num7drn_swaz_sd': _calc_num7drn_swaz_sd
    }
    return str_to_function


def _get_str_flow_at_pt(str_flow, row, col):
    """
    internal function to deal with the challenges of duplication of stream points and return the correct cell by cell
    flow works on either the stream flow or the model to stream flow
    :param str_flow:
    :param row:
    :param col:
    :return:
    """
    dup = get_stream_duplication_array()[row, col]
    if dup == 0:
        raise ValueError('duplication array returned zero at row: {} col:{}'.format(row, col))
    water_vol, mask = str_flow.data[row, col], str_flow.mask[row, col]
    if mask:
        raise ValueError('retuned masked values')
    return water_vol / dup


# functions to calcualte the stream flow or stream depletion at each of a number of sites

def _calc_eyre_warren_hydrograph(str_flow, **kwargs):
    """
    This is the first of many stream functions to calculate the flow or stream depletion at a given point.
    :param str_flow:
    :param kwargs: not used, to prevent errors when passing arguments across all stream functions
    :return:
    """
    water_vol = _get_str_flow_at_pt(str_flow, 56, 119)
    return water_vol


def _calc_ashley_sh1_hydrograph(str_flow, **kwargs):
    water_vol = _get_str_flow_at_pt(str_flow, 32, 309)
    return water_vol


def _calc_cust_therelkelds_hydrograph(str_flow, drn_flow, drn_samp_dict, **kwargs):
    water_vol = _get_str_flow_at_pt(str_flow, 80, 281)
    n7drn_flow = _get_drn_flow('num7drain_swaz', drn_flow, drn_samp_dict)
    return water_vol + n7drn_flow


def _calc_cust_oxfordrd_hydrograph(str_flow, **kwargs):
    water_vol = _get_str_flow_at_pt(str_flow, 53, 220)

    return water_vol


def _calc_num7drain_hydrograph(str_flow, drn_flow, drn_samp_dict, **kwargs):
    water_vol = _get_str_flow_at_pt(str_flow, 77, 280)

    n7drn_flow = _get_drn_flow('num7drain_swaz', drn_flow, drn_samp_dict)
    return water_vol + n7drn_flow


def _calc_kaiapoi_waimak_hydrograph(str_flow, drn_flow, drn_samp_dict, **kwargs):
    water_vol = _get_str_flow_at_pt(str_flow, 86, 290)  # at bottom of cust stream bcs (exclueds part of num7drn)
    drns_to_add = ['num7drain_swaz', 'cam_kaiapoi', 'courtenay_kaiapoi', 'kaiapoi_swaz', 'ohoka_swaz']
    for drn in drns_to_add:
        water_vol += _get_drn_flow(drn, drn_flow, drn_samp_dict)

    return water_vol


def _calc_eyre_swaz_sd(str_model_flow, drn_samp_dict, **kwargs):
    """
    the first of the stream depltion calculations
    :param str_model_flow:
    :param drn_samp_dict:
    :param kwargs:
    :return:
    """
    water_vol = _get_drn_flow('str_eyre_swaz', str_model_flow, drn_samp_dict)

    return water_vol


def _calc_ashley_swaz_sd(str_model_flow, drn_samp_dict, drn_flow, **kwargs):
    water_vol = _get_drn_flow('str_ashley_swaz', str_model_flow, drn_samp_dict)
    water_vol += _get_drn_flow('ashley_swaz', drn_flow, drn_samp_dict)

    return water_vol


def _calc_custmain_swaz_sd(str_model_flow, drn_samp_dict, **kwargs):
    water_vol = _get_drn_flow('str_custmaindrn_swaz', str_model_flow, drn_samp_dict)

    return water_vol


def _calc_custr_swaz_sd(str_model_flow, drn_samp_dict, **kwargs):
    water_vol = _get_drn_flow('str_cust_swaz', str_model_flow, drn_samp_dict)

    return water_vol


def _calc_num7drn_swaz_sd(str_model_flow, drn_samp_dict, drn_flow, **kwargs):
    water_vol = _get_drn_flow('str_num7drain_swaz', str_model_flow, drn_samp_dict)
    water_vol += _get_drn_flow('num7drain_swaz', drn_flow, drn_samp_dict)

    return water_vol


# todo below is from the drain files, but may be useful


def get_drn_concentration(location, m, con, mt3d_kskper=None,
                          mf_kskper=None, recalc=False):
    """

    :param location: list drain locations
    :param m: mf model
    :param con: either a UCN file object or the path to a UNC file
    :param mt3d_kskper: mt3d time step and time period to include if None uses the last
    :param mf_kskper: mf time step and time period to include if None use the last
    :param recalc: bool recalculate the drain points
    :return: dataframe of concentration and volume for each point
    """
    location = list(np.atleast_1d(location))
    outdata = pd.DataFrame(index=location, columns=['con', 'vol'])
    drn_points = get_drn_samp_pts_dict(recalc=recalc)
    if not set(location).issubset(drn_points.keys()):
        raise ValueError(
            '{} has not been added to drain site dictionary'.format(set(location) - set(drn_points.keys())))

    # load cell by cell and concentration data
    cbc = flopy.utils.CellBudgetFile('{}/{}'.format(m.model_ws, m.get_output(unit=741)
                                                    ))
    if mf_kskper is None:
        mf_kskper = cbc.get_kstpkper()[-1]  # use the last timestep/time period
    drn_recharge_data = cbc.get_data(kstpkper=mf_kskper, full3D=True)[0][0]  # keep only top layer
    if isinstance(con, str):
        con = flopy.utils.UcnFile(con)
    if mt3d_kskper is None:
        mt3d_kskper = con.get_kstpkper()[-1]  # use the last time step is none
    gw_conc_data = con.get_data(mt3d_kskper)[0]
    # set null values to nan
    gw_conc_data[np.isclose(gw_conc_data, 1e+30)] = np.nan

    for loc in location:
        mask_array = drn_points[loc]

        if drn_recharge_data[mask_array].mask.sum() != 0:
            raise ValueError('masked values returned for {}'.format(loc))

        water_volume = drn_recharge_data[mask_array].data.sum()
        load = (gw_conc_data[mask_array] * drn_recharge_data[mask_array].data).sum()
        outdata.loc[loc, 'con'] = load / water_volume
        outdata.loc[loc, 'vol'] = water_volume

    return outdata


def get_drn_samp_pts_dict(recalc=False):
    """
    gets a dictionary of boolean arrays for each sampling point.  These are ultimately derived from shape files, but
    if possible this function will load a pickled dictionary
    :param recalc: bool if True then the pickled dictionary (if any) will not be re-loaded and instead the dictionary
                   will be calculated from all avalible shapefiles (in base_shp_path)
    :return: dictionary {location: masking array}
    """
    pickle_path = "{}/inputs/pickeled_files/drn_samp_pts.p".format(sdp)
    if os.path.exists(pickle_path) and not recalc:
        drn_con_samp_pts = pickle.load(open(pickle_path))
        return drn_con_samp_pts

    # load all shapefiles in base_shp_path
    base_shp_path = "{}/inputs/shp_files/drain_catchments2/*.shp".format(sdp)
    temp_lst = glob.glob(base_shp_path)
    temp_kys = [os.path.basename(e).split('.')[0] for e in temp_lst]

    shp_dict = dict(zip(temp_kys, temp_lst))

    drn_con_samp_pts = {}
    for loc in shp_dict.keys():
        temp = shape_file_to_model_array(shp_dict[loc], 'elv', alltouched=True)
        temp2 = np.zeros(temp.shape, dtype=bool)
        temp2[np.isfinite(temp)] = True
        temp2[np.isnan(temp)] = False
        drn_con_samp_pts[loc] = temp2

    pickle.dump(drn_con_samp_pts, open(pickle_path, mode='w'))
    return drn_con_samp_pts


def get_drn_samp_pts():
    keys = get_drn_samp_pts_dict().keys()
    for key in keys:
        if 'str_' in key:
            keys.remove(key)

    keys.remove('all_drains')
    keys.remove('waimak_drn')
    return keys
