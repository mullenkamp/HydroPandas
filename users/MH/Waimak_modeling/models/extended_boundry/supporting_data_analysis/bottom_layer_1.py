"""
coding=utf-8
Author: matth
Date Created: 26/06/2017 2:35 PM
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

def get_mean_water_level():
    well_details_org = rd_sql(**sql_db.wells_db.well_details)
    well_details = well_details_org[(well_details_org['WMCRZone'] == 4) | (well_details_org['WMCRZone'] == 7) |
                                (well_details_org['WMCRZone'] == 8)]  # keep only waimak selwyn and chch zones
    well_details = well_details[well_details['Well_Status'] == 'AE']
    well_details = well_details[pd.notnull(well_details['DEPTH'])]

    well_details = well_details.set_index('WELL_NO')

    data = hydro().get_data(mtypes=['gwl'], sites=list(well_details.index), from_date='2008-01-01',
                            to_date='2016-01-01').data
    data = data.loc['gwl'].reset_index()
    data1 = hydro().get_data(mtypes=['gwl_m'], sites=list(well_details.index), from_date='2008-01-01',
                            to_date='2016-01-01').data
    temp = data1.groupby(level=['mtype', 'site']).describe()[['min', '25%', '50%', '75%', 'mean', 'max', 'count']].round(2)
    sites = list(temp.loc['gwl_m'].index[temp.loc['gwl_m']['count'] >=5])



    data1 = data1.loc['gwl_m',sites].reset_index()
    data = pd.concat((data,data1),axis=0)
    data = data.set_index('site')
    data.loc[np.isclose(data['data'], -999.99),'data'] = np.nan #dry wells
    data.loc[np.isclose(data['data'], 999.99),'data'] = np.nan #flowing wells

    overview_data = data.groupby('site').describe()

    #todo for some reason this is trouble
    rb = rasterio.open(r"P:\Groundwater\Waimakariri\Groundwater\Numerical GW model\supporting_data_for_scripts\ex_bd_va_sdp\m_ex_bd_inputs\shp\LINZ_DEM_8M_Canterbury_2012_31.tif")
    # make referenced to waterlevel
    overview_data = overview_data['data']
    for site in overview_data.index:
        overview_data.loc[site,'nztmx'] = well_details.loc[site,'NZTMX']
        overview_data.loc[site,'nztmy'] = well_details.loc[site,'NZTMY']
        overview_data.loc[site,'depth'] = well_details.loc[site,'DEPTH']

        ref_level = well_details.loc[site, 'REFERENCE_RL']
        if pd.isnull(ref_level):  # if there is no reference level assume it is at the ground from DEM
            ground_ref_level = well_details.loc[site, 'GROUND_RL']
            if pd.isnull(ground_ref_level):
                ground_ref_level = 0

            x,y = well_details.loc[site,['NZTMX', 'NZTMY']]
            ref_level = list(rb.sample([(x,y)]))[0][0] + ground_ref_level*-1
            if np.isclose(ref_level,-3.40282306e+38):
                ref_level = np.nan
        for key in [u'mean', u'std', u'min', u'25%', u'50%', u'75%', u'max']:
            overview_data.loc[site, key] += ref_level
        overview_data.loc[site,'ref_level'] = ref_level

    return overview_data


if __name__ == '__main__':
    test = get_mean_water_level()
    test.to_csv(r"C:\Users\MattH\Downloads\test_mean_water_level.csv")

    #test = pd.read_csv(r"C:\Users\MattH\Downloads\test_mean_water_level.csv")
    fig, ax = plt.subplots(1,1)
    sc = ax.scatter(test['ref_level']*-1, test['50%'], c=test['depth'], vmin=0,vmax=100)
    fig.colorbar(sc)

    fig2, ax2 = plt.subplots(1,1)
    sc1 = ax2.scatter(test['nztmx'],test['nztmy'],s=test['depth'],c=test['50%']-test['ref_level'])
    fig2.colorbar(sc1)


    print 'dpone'