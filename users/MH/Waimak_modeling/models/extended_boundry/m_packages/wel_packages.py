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
import os
import pickle
import geopandas as gpd


def create_wel_package(m, wel_version):
    wel = flopy.modflow.mfwel.ModflowWel(m,
                                         ipakcb=740,
                                         stress_period_data={
                                             0: smt.convert_well_data_to_stresspd(get_wel_spd(wel_version))},
                                         options=['AUX IFACE'],
                                         unitnumber=709)


def get_wel_spd(version):
    if version == 1:
        outdata = _get_wel_spd_v1()
    else:
        raise ValueError('unexpected version: {}'.format(version))
    return outdata


def _get_wel_spd_v1(recalc=False):
    pickle_path = '{}/well_spd.p'.format(smt.pickle_dir)
    if os.path.exists(pickle_path) and not recalc:
        well_data = pickle.load(open(pickle_path))
        return well_data

    races = get_race_data()
    # scale races so that they are right (see memo)
    races.loc[races.type == 'boundry_flux', 'flux'] *= 0.6 * 86400 / races.loc[
        races.type == 'boundry_flux', 'flux'].sum()

    elv_db = smt.calc_elv_db()
    for site in races.index:
        races.loc[site, 'row'], races.loc[site, 'col'] = smt.convert_coords_to_matix(races.loc[site, 'x'],
                                                                                     races.loc[site, 'y'])
    races['zone'] = 'n_wai'
    races = races.set_index('well')

    n_wai_wells = get_nwai_wells()
    for site in n_wai_wells.index:
        x, y, z = n_wai_wells.loc[site, ['x', 'y', 'z']]
        temp = smt.convert_coords_to_matix(x, y, z, elv_db=elv_db)
        n_wai_wells.loc[site, 'layer'], n_wai_wells.loc[site, 'row'], n_wai_wells.loc[site, 'col'] = temp
    n_wai_wells['zone'] = 'n_wai'
    n_wai_wells = n_wai_wells.set_index('well')

    s_wai_wells = _get_s_wai_wells()  # there are some s_wai wells which do not have data in wells, but do in consents file fix if bored
    temp = smt.get_well_postions(np.array(s_wai_wells.index), one_val_per_well=True, raise_exct=False)
    s_wai_wells['layer'], s_wai_wells['row'], s_wai_wells['col'] = temp
    no_flow = smt.get_no_flow()
    for i in s_wai_wells.index:
        layer, row, col = s_wai_wells.loc[i, ['layer', 'row', 'col']]
        if any(pd.isnull([layer, row, col])):
            continue
        if no_flow[layer, row, col] == 0:  # get rid of non-active wells
            s_wai_wells.loc[i, 'layer'] = np.nan

    s_wai_wells = s_wai_wells.dropna(subset=['layer', 'row', 'col'])

    s_wai_rivers = _get_s_wai_rivers().set_index('well')

    all_wells = pd.concat((races, n_wai_wells, s_wai_wells, s_wai_rivers))

    for i in all_wells.index:
        row, col = all_wells.loc[i, ['row', 'col']]
        x, y = smt.convert_matrix_to_coords(row, col)
        all_wells.loc[i, 'mx'] = x
        all_wells.loc[i, 'my'] = y

    # check wells in correct aquifer
    aq_to_layer = {'Avonside Formation': 0,
                   'Springston Formation': 0,
                   'Christchurch Formation': 0,
                   'Riccarton Gravel': 1,
                   'Bromley Formation': 2,
                   'Linwood Gravel': 3,
                   'Heathcote Formation': 4,
                   'Burwood Gravel': 5,
                   'Shirley Formation': 6,
                   'Wainoni Gravel': 7}
    leapfrog_aq = gpd.read_file("{}/m_ex_bd_inputs/shp/layering/gis_aq_name_clipped.shp".format(smt.sdp))
    leapfrog_aq = leapfrog_aq.set_index('well')
    leapfrog_aq.loc[:,'use_aq_name'] = leapfrog_aq.loc[:,'aq_name']
    leapfrog_aq.loc[leapfrog_aq.use_aq_name.isnull(),'use_aq_name'] = leapfrog_aq.loc[leapfrog_aq.use_aq_name.isnull(),'aq_name_gi']

    for well in all_wells.index:
        try:
            all_wells.loc[well,'aquifer_in_confined'] = aq = leapfrog_aq.loc[well,'use_aq_name']
            all_wells.loc[well, 'layer_by_aq'] = aq_to_layer[aq]
        except KeyError:
            pass

    all_wells.loc[:,'layer_by_depth'] = all_wells.loc[:,'layer']
    all_wells.loc[all_wells.layer_by_aq.notnull(),'layer'] = all_wells.loc[all_wells.layer_by_aq.notnull(),'layer_by_aq']

    # move wells that fall on other boundry conditions north of waimak (or in constant head)
    overlap = gpd.read_file("{}/m_ex_bd_inputs/shp/overlap_adjustment2.shp".format(smt.sdp))
    overlap = overlap.set_index('index')



    for well in all_wells.index:
        if not well in overlap.index:
            continue
        all_wells.loc[well,'layer'] += overlap.loc[well,'add_k']
        all_wells.loc[well,'row'] += overlap.loc[well,'add_row']
        all_wells.loc[well,'col'] += overlap.loc[well,'add_col']

    overlap = gpd.read_file("{}/m_ex_bd_inputs/shp/overlap_adjustment2part2.shp".format(smt.sdp))
    overlap = overlap.set_index('Field1')
    for well in all_wells.index:
        if not well in overlap.index:
            continue
        all_wells.loc[well,'layer'] += overlap.loc[well,'add_layer']
        all_wells.loc[well,'row'] += overlap.loc[well,'add_row']
        all_wells.loc[well,'col'] += overlap.loc[well,'add_col']

    pickle.dump(all_wells, open(pickle_path, 'w'))
    return all_wells


def _get_s_wai_wells():
    allo = pd.read_csv("{}/inputs/wells/allo_gis.csv".format(sdp), index_col='crc')

    # option 2
    end_time = pd.datetime(2016, 12, 31)
    start_time = pd.datetime(2008, 1, 1)

    allo2 = allo.loc[np.in1d(allo['cwms'], ['Selwyn - Waihora', 'Christchurch - West Melton']) &
                     (allo['take_type'] == 'Take Groundwater') &
                     ((allo.status_details.str.contains('Terminated')) | allo.status_details.str.contains('Issued'))]

    allo2.loc[:, 'to_date'] = pd.to_datetime(allo2.loc[:, 'to_date'], format='%d/%m/%Y', errors='coerce')
    allo2.loc[:, 'from_date'] = pd.to_datetime(allo2.loc[:, 'from_date'], format='%d/%m/%Y', errors='coerce')
    allo2.loc[allo2.loc[:, 'to_date'] > end_time, 'to_date'] = end_time
    allo2.loc[allo2.loc[:, 'to_date'] < start_time, 'to_date'] = None
    allo2.loc[allo2.loc[:, 'from_date'] < start_time, 'from_date'] = start_time
    allo2.loc[allo2.loc[:, 'from_date'] > end_time, 'from_date'] = None
    idx = (pd.notnull(allo2.loc[:, 'to_date']) & pd.notnull(allo2.loc[:, 'from_date']))
    allo2.loc[idx, 'temp_days'] = (allo2.loc[idx, 'to_date'] - allo2.loc[idx, 'from_date'])
    allo2.loc[:, 'days'] = [e.days for e in allo2.loc[:, 'temp_days']]

    # the below appear to be an interal consents marker... and should not be included here as a replacement consent
    # is active at the same time at the consent with negitive number of days
    allo2.loc[allo2.loc[:, 'days'] < 0, 'days'] = 0

    allo2.loc[:, 'flux'] = allo2.loc[:, 'cav'] / 365 * allo2.loc[:, 'days'] / (end_time - start_time).days * -1
    allo2.loc[allo2.use_type=='irrigation','flux']*=0.5 #todo I'm trying this for not

    out_data = allo2.reset_index().groupby('wap').aggregate({'flux': np.sum, 'crc': ','.join})
    out_data['consent'] = [tuple(e.split(',')) for e in out_data.loc[:, 'crc']]
    out_data = out_data.drop('crc', axis=1)
    out_data = out_data.dropna()

    out_data['type'] = 'well'
    out_data['zone'] = 's_wai'
    out_data.index.names = ['well']
    out_data.loc[:, 'flux'] *= 0.50  # start with a 50% scaling factor from CAV come back if time #todo should chch wells be scaled???

    return out_data

def _check_chch_wells():
    allo = pd.read_csv("{}/inputs/wells/allo_gis.csv".format(sdp), index_col='crc')

    # option 2
    end_time = pd.datetime(2016, 12, 31)
    start_time = pd.datetime(2008, 1, 1)

    allo2 = allo.loc[np.in1d(allo['cwms'], ['Christchurch - West Melton']) &
                     (allo['take_type'] == 'Take Groundwater') &
                     ((allo.status_details.str.contains('Terminated')) | allo.status_details.str.contains('Issued'))]

    allo2.loc[:, 'to_date'] = pd.to_datetime(allo2.loc[:, 'to_date'], format='%d/%m/%Y', errors='coerce')
    allo2.loc[:, 'from_date'] = pd.to_datetime(allo2.loc[:, 'from_date'], format='%d/%m/%Y', errors='coerce')
    allo2.loc[allo2.loc[:, 'to_date'] > end_time, 'to_date'] = end_time
    allo2.loc[allo2.loc[:, 'to_date'] < start_time, 'to_date'] = None
    allo2.loc[allo2.loc[:, 'from_date'] < start_time, 'from_date'] = start_time
    allo2.loc[allo2.loc[:, 'from_date'] > end_time, 'from_date'] = None
    idx = (pd.notnull(allo2.loc[:, 'to_date']) & pd.notnull(allo2.loc[:, 'from_date']))
    allo2.loc[idx, 'temp_days'] = (allo2.loc[idx, 'to_date'] - allo2.loc[idx, 'from_date'])
    allo2.loc[:, 'days'] = [e.days for e in allo2.loc[:, 'temp_days']]

    # the below appear to be an interal consents marker... and should not be included here as a replacement consent
    # is active at the same time at the consent with negitive number of days
    allo2.loc[allo2.loc[:, 'days'] < 0, 'days'] = 0

    allo2.loc[:, 'flux'] = allo2.loc[:, 'cav'] / 365 * allo2.loc[:, 'days'] / (end_time - start_time).days * -1
    allo2.loc[allo2.use_type=='irrigation','flux']*=0.5 #todo I'm trying this for not

    out_data = allo2.reset_index().groupby('wap').aggregate({'flux': np.sum, 'crc': ','.join})
    out_data['consent'] = [tuple(e.split(',')) for e in out_data.loc[:, 'crc']]
    out_data = out_data.drop('crc', axis=1)
    out_data = out_data.dropna()

    out_data['type'] = 'well'
    out_data['zone'] = 's_wai'
    out_data.index.names = ['well']
    out_data.loc[:, 'flux'] *= 0.50  # start with a 50% scaling factor from CAV come back if time

    return out_data

def _check_waimak_wells():
    allo = pd.read_csv("{}/inputs/wells/allo_gis.csv".format(sdp), index_col='crc')

    # option 2
    end_time = pd.datetime(2016, 12, 31)
    start_time = pd.datetime(2008, 1, 1)

    allo2 = allo.loc[np.in1d(allo['cwms'], ['Waimakariri']) &
                     (allo['take_type'] == 'Take Groundwater') &
                     ((allo.status_details.str.contains('Terminated')) | allo.status_details.str.contains('Issued'))]

    allo2.loc[:, 'to_date'] = pd.to_datetime(allo2.loc[:, 'to_date'], format='%d/%m/%Y', errors='coerce')
    allo2.loc[:, 'from_date'] = pd.to_datetime(allo2.loc[:, 'from_date'], format='%d/%m/%Y', errors='coerce')
    allo2.loc[allo2.loc[:, 'to_date'] > end_time, 'to_date'] = end_time
    allo2.loc[allo2.loc[:, 'to_date'] < start_time, 'to_date'] = None
    allo2.loc[allo2.loc[:, 'from_date'] < start_time, 'from_date'] = start_time
    allo2.loc[allo2.loc[:, 'from_date'] > end_time, 'from_date'] = None
    idx = (pd.notnull(allo2.loc[:, 'to_date']) & pd.notnull(allo2.loc[:, 'from_date']))
    allo2.loc[idx, 'temp_days'] = (allo2.loc[idx, 'to_date'] - allo2.loc[idx, 'from_date'])
    allo2.loc[:, 'days'] = [e.days for e in allo2.loc[:, 'temp_days']]

    # the below appear to be an interal consents marker... and should not be included here as a replacement consent
    # is active at the same time at the consent with negitive number of days
    allo2.loc[allo2.loc[:, 'days'] < 0, 'days'] = 0

    allo2.loc[:, 'flux'] = allo2.loc[:, 'cav'] / 365 * allo2.loc[:, 'days'] / (end_time - start_time).days * -1
    allo2.loc[allo2.use_type == 'irrigation', 'flux'] *= 0.5  # todo I'm trying this for not

    out_data = allo2.reset_index().groupby('wap').aggregate({'flux': np.sum, 'crc': ','.join})
    out_data['consent'] = [tuple(e.split(',')) for e in out_data.loc[:, 'crc']]
    out_data = out_data.drop('crc', axis=1)
    out_data = out_data.dropna()

    out_data['type'] = 'well'
    out_data['zone'] = 'n_wai'
    out_data.index.names = ['well']
    out_data.loc[:, 'flux'] *= 0.50  # start with a 50% scaling factor from CAV come back if time #todo should chch wells be scaled???

    return out_data



def _get_s_wai_rivers():
    # julian's report suggeststs that the hawkins is mostly a balanced flow and it was not included in
    # scott and thorley 2009 so it is not being simulated here

    # values for the upper selwyn, horoatat and waianiwanwa river are taken from scott and thorley 2009 and are evenly
    # distributed across the area as injection wells in layer 1

    # value in the shapefile reference the reach numbers from scott and thorley 2009

    no_flow = smt.get_no_flow(0)
    no_flow[no_flow < 0] = 0
    rivers = smt.shape_file_to_model_array("{}/m_ex_bd_inputs/shp/selwyn_hill_feds.shp".format(smt.sdp), 'reach', True)
    rivers[~no_flow.astype(bool)] = np.nan
    waian = pd.DataFrame(smt.model_where(rivers[np.newaxis, :, :] == 106), columns=['layer', 'row', 'col'])
    waian['well'] = ['waian{:04d}'.format(e) for e in waian.index]
    waian['flux'] = 0.142 * 86400 / len(waian)  # evenly distribute flux from scott and thorley 2009

    selwyn = pd.DataFrame(smt.model_where(rivers[np.newaxis, :, :] == 104), columns=['layer', 'row', 'col'])
    selwyn['well'] = ['selwyn{:04d}'.format(e) for e in selwyn.index]
    selwyn['flux'] = 4.152 * 86400 / len(selwyn)  # evenly distribute flux from scott and thorley 2009

    horo = pd.DataFrame(smt.model_where(rivers[np.newaxis, :, :] == 105), columns=['layer', 'row', 'col'])
    horo['well'] = ['horo{:04d}'.format(e) for e in horo.index]
    # evenly distribute flux from scott thorley 2009 but only including 7/32 of the river in my model
    horo['flux'] = 0.554 * 86400 / len(horo)

    # the hawkins 0.25 m3/s equally distributed between watsons bridge road and homebush road
    hawkins = pd.DataFrame(smt.model_where(rivers[np.newaxis, :, :] == 103), columns=['layer', 'row', 'col'])
    hawkins['well'] = ['hawkins{:04d}'.format(e) for e in hawkins.index]
    hawkins['flux'] = 0.25 * 86400 / len(hawkins)  # evenly distribute flux from scott and thorley 2009

    outdata = pd.concat((waian, selwyn, horo, hawkins))
    outdata['zone'] = 's_wai'
    outdata['type'] = 'river'
    outdata['consent'] = None

    return outdata


if __name__ == '__main__':

    n_wells_new = _check_waimak_wells()
    allo = pd.read_csv("{}/inputs/wells/allo_gis.csv".format(sdp), index_col='crc')
    chch_wells = _check_chch_wells()
    n_wells = get_nwai_wells()
    s_wells_all = _get_s_wai_wells()
    well_spd = _get_wel_spd_v1(recalc=True)
    print 'done'
