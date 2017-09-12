# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 7/09/2017 3:55 PM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.m_packages.wel_packages import _get_wel_spd_v1
import pandas as pd
import numpy as np
import pickle
import os
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
from users.MH.Waimak_modeling.supporting_data_path import sdp

# for stream depletion things
def get_race_data():
    """
    all influx wells in the well data (e.g. streams, races, boundary fluxes)
    :return:
    """
    outdata = _get_wel_spd_v1()
    outdata = outdata.loc[outdata.loc[:,'type'] != 'well']
    return outdata

def get_full_consent(recalc=False):
    """
    EAV/consented annual volume I need to think about this one
    :return: # the full dataframe with just the flux converted
    """
    pickle_path = "{}/model_well_full_abstraction.p".format(smt.pickle_dir)
    if (os.path.exists(pickle_path)) and (not recalc):
        outdata = pickle.load(open(pickle_path))
        return outdata

    outdata = _get_wel_spd_v1()
    allo = pd.read_csv("{}/inputs/wells/allo_gis.csv".format(sdp))
    allo = allo.dropna(subset=['status_details'])
    allo = allo.loc[np.in1d(allo.status_details,['Issued - Active','Issued - s124 Continuance']) & (allo.take_type=='Take Groundwater')]

    # adjust irrigation wells cav where they are calcualted on the July to June year to an October to April?
    idx = (allo.use_type == 'irrigation') & (allo.from_month != 'OCT') # nans assumed to be july to june
    allo.loc[idx,'cav'] *= 6/12
    fluxes = allo.groupby('wap').aggregate({'cav': np.sum})
    allo2 = allo.drop_duplicates('wap')
    allo2 = allo2.set_index('wap')
    fluxes = pd.merge(fluxes,pd.DataFrame(allo2.loc[:, 'cwms']), right_index=True, left_index=True)

    print_digong = False # something I did to debug, it looks like there are some differences in teh well list,
    # but they look ok
    if print_digong:
        print ('only waimak')
        print(len(set(fluxes.loc[np.in1d(fluxes.cwms,['Waimakariri'])].index)-set(outdata.index)))
        print((set(fluxes.loc[np.in1d(fluxes.cwms,['Waimakariri'])].index)-set(outdata.index)))
        print ('only chch')
        print(len(set(fluxes.loc[np.in1d(fluxes.cwms,['Christchurch - West Melton'])].index)-set(outdata.index)))
        print((set(fluxes.loc[np.in1d(fluxes.cwms,['Christchurch - West Melton'])].index)-set(outdata.index)))
        print('number of missing in model')
        print(len(set(outdata.loc[outdata.type == 'well'].index)-set(fluxes.index)))

    outdata.loc[outdata.type=='well','flux'] = 0
    idx = set(fluxes.index).intersection(outdata.index)
    outdata.loc[idx,'flux'] = fluxes.loc[idx,'cav']/-365

    pickle.dump(outdata, open(pickle_path, 'w'))
    return outdata

def get_max_rate(recalc=False):
    """
    replaces the flux value with the full consented volumes for the wells north of the waimakariri
    :param recalc: if true recalc the pickle
    :return:
    """
    pickle_path = "{}/model_well_max_rate.p".format(smt.pickle_dir)
    if (os.path.exists(pickle_path)) and (not recalc):
        outdata = pickle.load(open(pickle_path))
        return outdata

    outdata = _get_wel_spd_v1()
    allo = pd.read_csv("{}/inputs/wells/allo_gis.csv".format(sdp))
    allo = allo.dropna(subset=['status_details'])
    allo = allo.loc[np.in1d(allo.status_details, ['Issued - Active', 'Issued - s124 Continuance']) & (allo.take_type == 'Take Groundwater')]

    # adjust irrigation wells cav where they are calcualted on the July to June year to an October to April?
    idx = (allo.use_type == 'irrigation') & (allo.from_month != 'OCT') # nans assumed to be july to june
    allo.loc[idx,'cav'] *= 6/12
    fluxes = allo.groupby('wap').aggregate({'cav': np.sum, 'max_rate_wap':np.sum, 'max_rate':np.sum})
    allo2 = allo.drop_duplicates('wap')
    allo2 = allo2.set_index('wap')
    fluxes = pd.merge(fluxes,pd.DataFrame(allo2.loc[:, 'cwms']), right_index=True, left_index=True)

    # first max_rate_wap then max_rate then cav/365
    fluxes.loc[:,'flux'] = fluxes.loc[:,'max_rate_wap'] * -86.4 # to convert from l/s to m3/day
    fluxes.loc[fluxes.flux.isnull(),'flux'] = fluxes.loc[:, 'max_rate'] * -86.4
    fluxes.loc[fluxes.flux.isnull(),'flux'] = fluxes.loc[:, 'cav'] / -365
    outdata.loc[outdata.type == 'well', 'flux'] = 0
    idx = set(fluxes.index).intersection(outdata.index)
    outdata.loc[idx,'flux'] = fluxes.loc[idx, 'flux']

    pickle.dump(outdata, open(pickle_path, 'w'))
    return outdata


def get_forward_wells (full_abstraction, cc_inputs, naturalised, full_allo):
    """
    gets the pumping data for the forward runs
    :param full_abstraction: boolean use the CAV (think about what happens with irrigation abstraction)
    :param cc_inputs: use these to apply scaling factors for the pumping (think about how to work with these spatially)
    :param naturalised: boolean, if True use only the fixed inputs (e.g. rivers, boundary fluxes.  No races)
    :param full_allo: boolean, if True scale the wells by the amount allocated in each zone (could be a dictionary of boolean for each subzone)
    :return:
    """
    # check input make sense
    if full_abstraction and naturalised:
        raise ValueError('cannot both fully abstracted and naturalised')
    if full_allo and naturalised:
        raise ValueError('cannot both be fully allocated and naturalised')

    if full_abstraction:
        outdata = get_full_consent()
    else:
        outdata = _get_wel_spd_v1()

    if full_allo:
        allo_mult = get_full_allo_multipler()
        idx = allo_mult.index
        outdata.loc[idx,'flux'] *= allo_mult.loc[idx]

    if naturalised:
        outdata = outdata.loc[np.in1d(outdata.loc['type'],['boundry_flux', 'llr_boundry_flux', 'river', 'ulr_boundry_flux'])]

    if cc_inputs is not None and any(pd.notnull(cc_inputs)):
        cc_mult = get_cc_pumping_muliplier(cc_inputs)
        outdata.loc[outdata.loc[:,'use_type']=='irrigation-sw','flux'] *= cc_mult

        # pumping is truncated at full allocation and abstraction value
        # we assume that any additional irrigation demand would be met with surface water schemes from the alpine rivers

        max_pumping = get_full_consent()
        allo_mult = get_full_allo_multipler()
        idx = allo_mult.index
        max_pumping.loc[idx,'flux'] *= allo_mult.loc[idx]

        idx = outdata.index
        outdata.loc[outdata.loc[idx,'flux'] > max_pumping.loc[idx,'flux'],'flux'] = max_pumping.loc[idx, 'flux']

    #todo check this thourally
    return outdata

def get_cc_pumping_muliplier(cc_inputs): #todo waiting on mike
    # return a single value for now
    raise NotImplementedError

def get_full_allo_multipler(): #todo
    #return a series with index well numbers and values multiplier #todo (should any be less than one)
    #these wells only in Waimakariri Zone
    raise NotImplementedError

if __name__ == '__main__':
    #todo check all components throughly
    test = get_max_rate()