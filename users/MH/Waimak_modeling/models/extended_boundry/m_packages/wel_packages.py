"""
Author: matth
Date Created: 20/06/2017 11:57 AM
"""

from __future__ import division
from core import env
import flopy
from users.MH.Waimak_modeling.model_tools.well_values import get_race_data, get_nwai_wells
from users.MH.Waimak_modeling.models.extended_boundry.extended_boundry_model_tools import smt
import pandas as pd
import numpy as np
from users.MH.Waimak_modeling.supporting_data_path import sdp
from core.ecan_io import rd_sql, sql_db


def create_wel_package(m, wel_version):
    wel = flopy.modflow.mfwel.ModflowWel(m,
                                         ipakcb=740,
                                         stress_period_data={0: smt.convert_well_data_to_stresspd(get_wel_spd(wel_version))},
                                         options=['AUX IFACE'],
                                         unitnumber=709)


def get_wel_spd(version):
    if version == 1:
        outdata = _get_wel_spd_v1()
    else:
        raise ValueError('unexpected version: {}'.format(version))
    return outdata


def _get_wel_spd_v1(recalc=False):  # todo add pickle
    races = get_race_data()
    elv_db = smt.calc_elv_db()
    for site in races.index:
        races.loc[site, 'row'], races.loc[site, 'col'] = smt.convert_coords_to_matix(races.loc[site, 'x'],
                                                                                     races.loc[site, 'y'])
    races['zone'] = 'n_wai'

    n_wai_wells = get_nwai_wells()
    for site in n_wai_wells.index:
        temp = smt.convert_coords_to_matix(n_wai_wells.loc[site, ['x', 'y', 'z']],elv_db=elv_db)
        n_wai_wells.loc[site, 'layer'], n_wai_wells.loc[site, 'row'], n_wai_wells.loc[site, 'col'] = temp
    n_wai_wells['zone'] = 'n_wai'

    s_wai_wells = _get_s_wai_wells()
    temp = smt.get_well_postions(np.array(s_wai_wells['well']), one_val_per_well=True, raise_exct=False)
    s_wai_wells['layer'], s_wai_wells['row'], s_wai_wells['col'] = temp
    no_flow = smt.get_no_flow()
    for i in s_wai_wells:
        layer, row, col = s_wai_wells.loc[i,['layer','row','col']]
        if no_flow[layer,row,col] == 0:  # get rid of non-active wells
            s_wai_wells.loc[i,'layer'] = np.nan

    s_wai_wells = pd.dropna(subset=['layer','row','col'])



    s_wai_rivers = _get_s_wai_rivers()

    all_wells = pd.concat((races, n_wai_wells, s_wai_wells, s_wai_rivers)) #todo check output

    raise NotImplementedError()
    return all_wells


def _get_s_wai_wells():
    allo = pd.read_csv("{}/inputs/wells/allo_gis.csv".format(sdp),index_col='crc')

    # option 2
    end_time = pd.datetime(2016,12,31)
    start_time = pd.datetime(2008,1,1)

    allo2 = allo.loc[np.in1d(allo['cwms'], ['Selwyn - Waihora', 'Christchurch - West Melton']) &
                     (allo['take_type'] == 'Take Groundwater') &
                     ((allo.status_details.str.contains('Terminated')) | allo.status_details.str.contains('Issued'))]

    allo2.loc[:,'to_date'] = pd.to_datetime(allo2.loc[:,'to_date'], format='%d/%m/%Y', errors='coerce')
    allo2.loc[:,'from_date'] = pd.to_datetime(allo2.loc[:,'from_date'], format='%d/%m/%Y', errors='coerce')
    allo2.loc[allo2.loc[:, 'to_date'] > end_time,'to_date'] = end_time
    allo2.loc[allo2.loc[:, 'to_date'] < start_time, 'to_date'] = None
    allo2.loc[allo2.loc[:, 'from_date'] < start_time, 'from_date'] = start_time
    allo2.loc[allo2.loc[:, 'from_date'] > end_time,'from_date'] = None
    idx = (pd.notnull(allo2.loc[:,'to_date']) & pd.notnull(allo2.loc[:,'from_date']))
    allo2.loc[idx,'temp_days'] = (allo2.loc[idx,'to_date'] - allo2.loc[idx,'from_date'])
    allo2['days'] = [e.days for e in allo2.loc[:,'temp_days']]

    # the below appear to be an interal consents marker... and should not be included here as a replacement consent
    # is active at the same time at the consent with negitive number of days
    allo2.loc[allo2.loc[:,'days']<0,'days'] = 0

    allo2['flux'] = allo2.loc[:,'cav']/365 * allo2.loc[:,'days']/(end_time - start_time).days *-1
    #todo check carefully

    out_data = allo2.reset_index().groupby('wap').aggregate({'flux': np.sum, 'crc':','.join})
    out_data['consent'] = [tuple(e.split(',')) for e in out_data.loc[:,'crc']]
    out_data = out_data.drop('crc', axis=1)
    out_data = out_data.dropna()

    out_data['type'] = 'well'
    out_data['zone'] = 's_wai'
    out_data = out_data.rename(columns={'wap':'well'})
    out_data.loc[:'flux'] *= 1/2  # start with a 50% scaling factor from CAV


    return out_data

def _get_s_wai_rivers(): #todo
    # todo look at julians report to get a boundry
    raise NotImplementedError()

if __name__ == '__main__':
    test = _get_s_wai_wells()
    test.to_csv(r"C:\Users\MattH\Downloads\test_s_wai_wells.csv")