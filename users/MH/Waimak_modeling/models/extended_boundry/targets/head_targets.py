# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 4/07/2017 5:24 PM
"""

from __future__ import division
from core import env
from core.ecan_io import rd_sql, sql_db
import numpy as np
import pandas as pd
from core.classes.hydro import hydro
from osgeo import gdal
from users.MH.Waimak_modeling.supporting_data_path import sdp
import rasterio
import matplotlib.pyplot as plt


def get_water_level_data(min_reading = 1):
    well_details_org = rd_sql(**sql_db.wells_db.well_details)
    well_details = well_details_org[(well_details_org['WMCRZone'] == 4) | (well_details_org['WMCRZone'] == 7) |
                                (well_details_org['WMCRZone'] == 8)]  # keep only waimak selwyn and chch zones
    well_details = well_details[pd.notnull(well_details['DEPTH'])]

    screen_details = rd_sql(**sql_db.wells_db.screen_details)
    screen_details.loc[:,'WELL_NO'] = [e.strip() for e in screen_details.loc[:,'WELL_NO']]
    screen_details = screen_details.set_index('WELL_NO')

    well_details.loc[:,'WELL_NO'] = [e.strip() for e in well_details.loc[:,'WELL_NO']]
    well_details = well_details.set_index('WELL_NO')

    data = hydro().get_data(mtypes=['gwl'], sites=list(well_details.index)).data
    data = data.loc['gwl'].reset_index()
    data1 = hydro().get_data(mtypes=['gwl_m'], sites=list(well_details.index)).data
    temp = data1.groupby(level=['mtype', 'site']).describe()[['min', '25%', '50%', '75%', 'mean', 'max', 'count']].round(2)
    sites = list(temp.loc['gwl_m'].index[temp.loc['gwl_m']['count'] >= min_reading])

    data1 = data1.loc['gwl_m',sites].reset_index()
    data = pd.concat((data,data1),axis=0)
    data = data.set_index('site')
    data['month'] = [e.month for e in data.time]
    data['year'] = [e.year for e in data.time]

    #todo think about which raster to use
    rb = rasterio.open(r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\supporting_data_for_scripts\ex_bd_va_sdp\m_ex_bd_inputs\shp\dem.tif")


    out_data = pd.DataFrame(index=set(data.index))

    for well in out_data.index:
        out_data.loc[well,'nztmx'] = well_details.loc[well,'NZTMX']
        out_data.loc[well,'nztmy'] = well_details.loc[well,'NZTMY']
        out_data.loc[well,'depth'] = well_details.loc[well,'DEPTH']
        qarrl = out_data.loc[well,'qar_rl'] = well_details.loc[well,'QAR_RL']
        out_data.loc[well,'rl_from_dem'] = False


        ref_level = well_details.loc[well, 'REFERENCE_RL']
        ground_ref_level = well_details.loc[well, 'GROUND_RL']
        if pd.isnull(ground_ref_level):
            ground_ref_level = 0
        if pd.isnull(ref_level):  # if there is no reference level assume it is at the ground from DEM #todo can I use lidar?
            out_data.loc[well, 'rl_from_dem'] = True
            x, y = well_details.loc[well, ['NZTMX', 'NZTMY']]
            ref_level = list(rb.sample([(x, y)]))[0][0] + ground_ref_level * -1
            if np.isclose(ref_level, -3.40282306e+38):
                ref_level = np.nan
        out_data.loc[well,'ref_level'] = ref_level
        out_data.loc[well,'ground_level'] = ref_level + ground_ref_level

        if well in screen_details.index:
            top = np.min(screen_details.loc[well,'TOP_SCREEN'])
            bot = np.min(screen_details.loc[well,'BOTTOM_SCREEN'])
            out_data.loc[well,'mid_screen_depth'] = (bot+top)/2
        else:
            out_data.loc[well,'mid_screen_depth'] = well_details.loc[well,'DEPTH'] - 2


    out_data.loc[:,'mid_screen_elv'] = out_data.loc[:,'ground_level'] - out_data.loc[:,'mid_screen_depth']


    data2008 = data.loc[data['year'] >= 2008]
    for val, dat in zip(['_2008','_all'],[data2008, data]):
        g = dat.loc[(dat.data > -999) & (dat.data < 999)].groupby('site')
        out_data['h2o_dpth_mean'] = g.aggregate({'data':np.mean})
        out_data['h2o_dpth_min'] = g.aggregate({'data':np.min})
        out_data['h2o_dpth_max'] = g.aggregate({'data':np.max})
        out_data['h2o_dpth_sd'] = g.aggregate({'data': np.std})
        out_data['readings_nondry'] = g.count().loc[:,'data']



        for name, month in zip(['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'],range(1,13)):

            temp = dat.loc[(np.in1d(dat['month'],[month])) & (dat.data > -999) & (dat.data < 999)]
            tempg = temp.groupby('site')
            out_data['h2o_dpth_{}'.format(name)] = tempg.aggregate({'data':np.mean})
            out_data['reading_{}'.format(name)] = tempg.count().loc[:,'data']



        g = dat.groupby('site')
        out_data['readings'] = g.count().loc[:,'data']

        g = dat.loc[(dat.data <= -999)].groupby('site')
        out_data['count_dry'] = g.count().loc[:,'data']

        g = dat.loc[(dat.data >= 999)].groupby('site')
        out_data['count_flowing'] = g.count().loc[:,'data']

        keys_to_convert = pd.Series(out_data.keys())
        keys_to_convert = list(keys_to_convert[keys_to_convert.str.contains('h2o_dpth')])
        for key in keys_to_convert:
            out_data.loc[:,key.replace('dpth','elv')] = out_data.loc[:,key] + out_data.loc[:,'ref_level']

        # add data quality flags
        all_readings = rd_sql(col_names=['WELL_NO', 'DATE_READ', 'TIDEDA_FLAG'],
                              where_col='WELL_NO',
                              where_val=[e.encode('ascii','ignore') for e in list(out_data.index)],**sql_db.wells_db.daily)

        all_readings.loc[:,'date'] = pd.to_datetime(all_readings.loc[:,'DATE_READ'])
        if '2008' in val:
            all_readings = all_readings.loc[all_readings.date > pd.datetime(2008,1,1)]
        all_readings = all_readings.set_index('WELL_NO')

        # flag if more than 10% of the reading are by owners or more than 5% of the readings are suspect
        out_data.loc[:,'owner_measured'] = False
        out_data.loc[:,'bad_data'] = False

        for well in out_data.index:
            if well not in all_readings.index:
                continue
            flags = pd.Series(np.atleast_1d(all_readings.loc[well,'TIDEDA_FLAG']))
            owner = flags.str.lower().str.contains('f').sum()
            bad_data = flags.str.lower().str.contains('x').sum()
            n_readings = len(flags)

            if owner/n_readings >0.1:
                out_data.loc[well,'owner_measured'] = True
            if bad_data/n_readings > 0.05:
                out_data.loc[:, 'bad_data'] = True



        # monitoring well need at least min of 6/year and not owner_measured or bad_data
        g = dat.groupby(['site', 'year'])
        temp = g.count()['data'].reset_index()
        temp = temp.loc[temp['data']>6]
        years = temp.groupby('site').count()['year']
        years = years.loc[years<5] # need at least 5 years of data to be considered a monitoring well

        out_data.loc[:, 'monitoring_well'] = False
        out_data.loc[list(years.index), 'monitoring_well'] = True
        out_data.loc[(out_data['bad_data']) | (out_data['owner_measured']), 'monitoring_well'] = False

        out_data.loc[:,'water_range'] = out_data.loc[:,'h2o_dpth_sep'] - out_data.loc[:,'h2o_dpth_mar']

        out_data.loc[:,'dry_percentage'] = (out_data.loc[:,'readings'] - out_data.loc[:,'readings_nondry'])/out_data.loc[:,'readings'] *100

        for name in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']:
            out_data['s_factor_{}'.format(name)] = out_data['h2o_dpth_{}'.format(name)] / out_data.loc[:, 'h2o_dpth_mean']


        out_data.to_csv(env.sci("Groundwater/Waimakariri/Groundwater/Numerical GW model/Model build and optimisation/targets/head_targets/first_pass_head_targets{}.csv".format(val)))


def correct_wl_define_error(outdata):
    # error as a percentage
    outdata.loc[:,'total_error_m'] = np.nan
    outdata.loc[:, 'include_non-gap'] = False


    # assign geospatial group 'g_group'

    # create weighting system for each group
    temp = outdata.loc[outdata['monitoring_well']]
    g = temp.groupby('g_group') #todo think about this

    for well in outdata.index:
        readings = outdata.loc[well,'readings_nondry']

        # correct all data that have more than __ percent seasonal bias


        # add the different error terms:
        # measurement error
        me = outdata.loc[well, 'm_error'] = 0.01 * outdata.loc['well','h2o_dpth_mean']
        # farmer vs ecan
        farm = outdata.loc[well, 'users_error'] = outdata.loc[well,'owner_measured'] * 0.10* outdata.loc['well','h2o_dpth_mean']
        # seasonal bias correction
        sea = outdata.loc[well, 'scorrection_error'] = None # todo pull from the varience of the seaonsal corrections and how uneven the sample is
        # DEM error
        if outdata.loc[well,'rl_from_dem']:
            dem = outdata.loc[well, 'dem_error'] = 5 # based on half the 90% uncertainty of the DEM as we exect less uncetatiny in the plains vs the hill country
        else:
            dem = outdata.loc[well, 'dem_error'] = 0.1 # for other casing errors

        low_rd_err = 0
        if readings < 5:
            low_rd_err = 0.15 * outdata.loc[well, 'h2o_dpth_mean']
        elif readings < 20:
            low_rd_err = 0.1 * outdata.loc[well, 'h2o_dpth_mean']

        outdata.loc[well, 'low_rd_error'] = low_rd_err
        outdata.loc[well, 'total_error_m'] = dem + sea + (me + farm + low_rd_err)/(readings)**0.5




     # add all errors for the toal error





    # todo boolean col include_non-gap
    raise NotImplementedError()


if __name__ == '__main__':
    get_water_level_data()