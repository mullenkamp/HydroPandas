# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 12/09/2017 6:26 PM
"""

from __future__ import division
from core import env
import numpy as np
import pandas as pd
import flopy
import pickle
import os
import glob
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from data_at_wells import _get_kstkpers
from warnings import warn

def get_flux_at_points(sites, base_path, kstpkpers=None, rel_kstpkpers=None):
    """
    get fluxes a pre-defined sites for drain and sfr packages. modflow directions apply e.g. negative values are flux
    out of the model cells into the sw feature
    :param sites: pre-defined site identifiers
    :param base_path: name file path with or without extension
    :param kstpkpers: actual kstpkpers to use (e.g. [(0,0),(0,1)])
    :param rel_kstpkpers: relative kstpkpers to use as general indexer (e.g. [0,1,2,3]) or all
    :return: flux for the sites passed as a dataframe index sites, cols = kstpkpers
    """
    base_path = base_path.strip('.nam')
    cbb_path = base_path + '.cbc'
    sites = np.atleast_1d(sites)
    sw_samp_pts_df = get_samp_points_df()
    sw_samp_pts_dict = _get_sw_samp_pts_dict()

    if not set(sites).issubset(sw_samp_pts_df.index):
        raise NotImplementedError('sites: {} not implemented'.format(set(sites)-set(sw_samp_pts_df.index)))


    flux_bud_file = flopy.utils.CellBudgetFile(cbb_path)

    kstpkpers = _get_kstkpers(bud_file=flux_bud_file,kstpkpers=kstpkpers,rel_kstpkpers=rel_kstpkpers)
    kstpkper_names = ['{}_{}'.format(e[0],e[1]) for e in kstpkpers]
    outdata = pd.DataFrame(index=sites, columns=kstpkper_names)

    for kstpkper, name in zip(kstpkpers, kstpkper_names):
        for site in sites:
            drn_array, sfr_array = _get_flux_flow_arrays(site,sw_samp_pts_dict,sw_samp_pts_df)
            if sfr_array.sum()==1:
                warn('{} only examining flux at 1 point, likley a flow point'.format(site))

            if drn_array is not None and sfr_array is not None:
                drn_bud = flux_bud_file.get_data(kstpkper=kstpkper, text='drain', full3D=True)[0]
                if drn_bud[drn_array].mask.sum() != 0:
                    raise ValueError('masked values returned for {}'.format(site))

                # flux is returned using modflow standard convention - is flow out of model + is flow in to model
                flux = drn_bud[drn_array].sum()
                sfr_bud = flux_bud_file.get_data(kstpkper=kstpkper,text='stream leakage', full3D=True)[0]
                if sfr_bud[sfr_array].mask.sum() != 0:
                    raise ValueError('masked values returned for {}'.format(site))

                # flux is returned using modflow standard convention - is flow out of model + is flow in to model
                flux += sfr_bud[sfr_array].sum()
                #todo check

            elif drn_array is not None:
                bud = flux_bud_file.get_data(kstpkper=kstpkper,text='drain', full3D=True)[0]
                if bud[drn_array].mask.sum() != 0:
                    raise ValueError('masked values returned for {}'.format(site))
                # flux is returned using modflow standard convention - is flow out of model + is flow in to model
                flux = bud[drn_array].sum()  #todo check

            elif sfr_array is not None:
                bud = flux_bud_file.get_data(kstpkper=kstpkper,text='stream leakage', full3D=True)[0]
                if bud[sfr_array].mask.sum() != 0:
                    raise ValueError('masked values returned for {}'.format(site))
                # flux is returned using modflow standard convention - is flow out of model + is flow in to model
                flux = bud[sfr_array].sum() #todo check

            else:
                raise ValueError('should not get here')
            outdata.loc[site,name] = flux

    return outdata


def get_flow_at_points(sites,base_path, kstpkpers=None, rel_kstpkpers=None):
    """
    get flows a pre-defined sites for drain and sfr packages flows are always postive
    :param sites: pre-defined site identifiers
    :param base_path: name file path with or without extension
    :param kstpkpers: actual kstpkpers to use (e.g. [(0,0),(0,1)])
    :param rel_kstpkpers: relative kstpkpers to use as general indexer (e.g. [0,1,2,3]) or all
    :return: flow for the sites passed as a dataframe index sites, cols = kstpkpers
    """
    base_path = base_path.strip('nam')
    cbb_path = base_path + '.cbc'
    flow_path = base_path + '.sfo'
    sites = np.atleast_1d(sites)
    sw_samp_pts_df = get_samp_points_df()
    sw_samp_pts_dict = _get_sw_samp_pts_dict()

    if not set(sites).issubset(sw_samp_pts_df.index):
        raise NotImplementedError('sites: {} not implemented'.format(set(sites)-set(sw_samp_pts_df.index)))

    flux_bud_file = flopy.utils.CellBudgetFile(cbb_path)
    flow_bud_file = flopy.utils.CellBudgetFile(flow_path)

    kstpkpers = _get_kstkpers(bud_file=flux_bud_file,kstpkpers=kstpkpers,rel_kstpkpers=rel_kstpkpers)
    kstpkper_names = ['{}_{}'.format(e[0],e[1]) for e in kstpkpers]
    outdata = pd.DataFrame(index=sites, columns=kstpkper_names)

    for kstpkper, name in zip(kstpkpers, kstpkper_names):
        for site in sites:
            drn_array, sfr_array = _get_flux_flow_arrays(site,sw_samp_pts_dict,sw_samp_pts_df)

            if drn_array is not None and sfr_array is not None:
                drn_bud = flux_bud_file.get_data(kstpkper=kstpkper, text='drain', full3D=True)[0]
                if drn_bud[drn_array].mask.sum() != 0:
                    raise ValueError('masked values returned for {}'.format(site))
                # flux is returned using modflow standard convention - is flow out of model + is flow in to model
                flow = drn_bud[drn_array].sum()*-1 #flow should be positive  #todo check
                sfr_bud = flow_bud_file.get_data(kstpkper=kstpkper,text='STREAMFLOW OUT', full3D=True)[0]
                if sfr_bud[sfr_array].mask.sum() != 0:
                    raise ValueError('masked values returned for {}'.format(site))
                # flux is returned using modflow standard convention - is flow out of model + is flow in to model
                flow += sfr_bud[sfr_array].sum()
                raise NotImplementedError('combined not defined')

            elif drn_array is not None:
                bud = flux_bud_file.get_data(kstpkper=kstpkper,text='drain', full3D=True)[0]
                if bud[drn_array].mask.sum() != 0:
                    raise ValueError('masked values returned for {}'.format(site))
                # flux is returned using modflow standard convention - is flow out of model + is flow in to model
                flow = bud[drn_array].sum()*-1 #flow should be positive  #todo check

            elif sfr_array is not None:
                bud = flow_bud_file.get_data(kstpkper=kstpkper,text='STREAMFLOW OUT', full3D=True)[0]
                if bud[sfr_array].mask.sum() != 0:
                    raise ValueError('masked values returned for {}'.format(site))
                # flux is returned using modflow standard convention - is flow out of model + is flow in to model
                flow = bud[sfr_array].sum()

            else:
                raise ValueError('should not get here')

            outdata.loc[site,name] = flow
    if (outdata<0).any():
        raise ValueError('negative values returned for flow')
    return outdata

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


#todo make SWAZ sites most drain groups plus 2x cust, eyre, num7?, ashley
