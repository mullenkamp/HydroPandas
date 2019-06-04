# -*- coding: utf-8 -*-
"""
Created on Tue Jan 02 10:40:45 2018

@author: MichaelEK
Functions for reading in Squalarc data.
"""
import numpy as np
import pandas as pd
import geopandas as gpd
from hydropandas.io.tools.mssql import rd_sql
from hydropandas.util.misc import select_sites
from core.spatial.vector import xy_to_gpd, sel_sites_poly


def rd_squalarc(sites, mtypes=None, from_date=None, to_date=None, convert_dtl=False, dtl_method=None, export=None):
    """
    Function to read in "squalarc" data. Which is atually stored in the mssql db.

    Parameters
    ----------
    sites: ndarry, list, or str
        The site names as a list, array, csv with the first column as the site names, or a polygon shapefile of the area of interest.
    mtypes: list or None
        A list of measurement type names to be in the output. Leaving it empty returns all mtypes.
    from_date: str
        A start date string in of '2010-01-01'.
    to_date: str
        A end date string in of '2011-01-01'.
    convert_dtl: bool
        Should values under the detection limit be converted to numeric?
    dtl_method: str
        The method to use to convert values under a detection limit to numeric. None or 'standard' takes half of the detection limit. 'trend' is meant as an output for trend analysis with includes an additional column dtl_ratio referring to the ratio of values under the detection limit.
    export: str or None
        Either None or a string path to a csv file.
    """

    #### Read in sites
    sites1 = select_sites(sites)

    #### Extract by polygon
    if isinstance(sites1, gpd.GeoDataFrame):
        ## Surface water sites
        sw_sites_tab = rd_sql('SQL2012PROD05', 'Squalarc', 'SITES', col_names=['SITE_ID', 'NZTMX', 'NZTMY'])
        sw_sites_tab.columns = ['site', 'NZTMX', 'NZTMY']
        gdf_sw_sites = xy_to_gpd('site', 'NZTMX', 'NZTMY', sw_sites_tab)
        sites1a = sites1.to_crs(gdf_sw_sites.crs)
        sw_sites2 = sel_sites_poly(gdf_sw_sites, sites1a).drop('geometry', axis=1)

        ## Groundwater sites
        gw_sites_tab = rd_sql('SQL2012PROD05', 'Wells', 'WELL_DETAILS', col_names=['WELL_NO', 'NZTMX', 'NZTMY'])
        gw_sites_tab.columns = ['site', 'NZTMX', 'NZTMY']
        gdf_gw_sites = xy_to_gpd('site', 'NZTMX', 'NZTMY', gw_sites_tab)
        gw_sites2 = sel_sites_poly(gdf_gw_sites, sites1a).drop('geometry', axis=1)

        sites2 = sw_sites2.site.append(gw_sites2.site).astype(str).tolist()
    else:
        sites2 = pd.Series(sites1, name='site').astype(str).tolist()

    #### Extract the rest of the data
    if len(sites2) > 10000:
        n_chunks = int(np.ceil(len(sites2) * 0.0001))
        sites3 = [sites2[i::n_chunks] for i in xrange(n_chunks)]
        samples_tab = pd.DataFrame()
        for i in sites3:
            samples_tab1 = rd_sql('SQL2012PROD05', 'Squalarc', '"SQL_SAMPLE_METHODS+"',
                                  col_names=['Site_ID', 'SAMPLE_NO', 'ME_TYP', 'Collect_Date', 'Collect_Time',
                                             'PA_NAME', 'PARAM_UNITS', 'SRESULT'], where_col='Site_ID', where_val=i)
            samples_tab1.columns = ['site', 'sample_id', 'source', 'date', 'time', 'parameter', 'units', 'val']
            samples_tab1.loc[:, 'source'] = samples_tab1.loc[:, 'source'].str.lower()
            samples_tab = pd.concat([samples_tab, samples_tab1])
    else:
        samples_tab = rd_sql('SQL2012PROD05', 'Squalarc', '"SQL_SAMPLE_METHODS+"',
                             col_names=['Site_ID', 'SAMPLE_NO', 'ME_TYP', 'Collect_Date', 'Collect_Time', 'PA_NAME',
                                        'PARAM_UNITS', 'SRESULT'], where_col='Site_ID', where_val=sites2)
        samples_tab.columns = ['site', 'sample_id', 'source', 'date', 'time', 'parameter', 'units', 'val']
        samples_tab.loc[:, 'source'] = samples_tab.loc[:, 'source'].str.lower()

    samples_tab2 = samples_tab.copy()
    num_test = pd.to_numeric(samples_tab2.loc[:, 'time'], 'coerce')
    samples_tab2.loc[num_test.isnull(), 'time'] = '0000'
    samples_tab2.loc[:, 'time'] = samples_tab2.loc[:, 'time'].str.replace('.', '')
    samples_tab2 = samples_tab2[samples_tab2.date.notnull()]
    #    samples_tab2.loc[:, 'time'] = samples_tab2.loc[:, 'time'].str.replace('9999', '0000')
    time1 = pd.to_datetime(samples_tab2.time, format='%H%M', errors='coerce')
    time1[time1.isnull()] = pd.Timestamp('2000-01-01 00:00:00')
    datetime1 = pd.to_datetime(samples_tab2.date.dt.date.astype(str) + ' ' + time1.dt.time.astype(str))
    samples_tab2.loc[:, 'date'] = datetime1
    samples_tab2 = samples_tab2.drop('time', axis=1)
    samples_tab2.loc[samples_tab2.val.isnull(), 'val'] = np.nan
    samples_tab2.loc[samples_tab2.val == 'N/A', 'val'] = np.nan

    #### Select within time range
    if isinstance(from_date, str):
        samples_tab2 = samples_tab2[samples_tab2['date'] >= from_date]
    if isinstance(to_date, str):
        samples_tab2 = samples_tab2[samples_tab2['date'] <= to_date]

    if mtypes is not None:
        mtypes1 = select_sites(mtypes)
        data = samples_tab2[samples_tab2.parameter.isin(mtypes1)].reset_index(drop=True)
    else:
        data = samples_tab2.reset_index(drop=True)

    #### Correct poorly typed in site names
    data.loc[:, 'site'] = data.loc[:, 'site'].str.upper().str.replace(' ', '')

    #### Convert detection limit values
    if convert_dtl:
        less1 = data['val'].str.match('<')
        if less1.sum() > 0:
            less1.loc[less1.isnull()] = False
            data2 = data.copy()
            data2.loc[less1, 'val'] = pd.to_numeric(data.loc[less1, 'val'].str.replace('<', ''), errors='coerce') * 0.5
            if dtl_method in (None, 'standard'):
                data3 = data2
            if dtl_method == 'trend':
                df1 = data2.loc[less1]
                count1 = data.groupby('parameter')['val'].count()
                count1.name = 'tot_count'
                count_dtl = df1.groupby('parameter')['val'].count()
                count_dtl.name = 'dtl_count'
                count_dtl_val = df1.groupby('parameter')['val'].nunique()
                count_dtl_val.name = 'dtl_val_count'
                combo1 = pd.concat([count1, count_dtl, count_dtl_val], axis=1, join='inner')
                combo1['dtl_ratio'] = (combo1['dtl_count'] / combo1['tot_count']).round(2)

                ## conditionals
                #            param1 = combo1[(combo1['dtl_ratio'] <= 0.4) | (combo1['dtl_ratio'] == 1)]
                #            under_40 = data['parameter'].isin(param1.index)
                param2 = combo1[(combo1['dtl_ratio'] > 0.4) & (combo1['dtl_val_count'] != 1)]
                over_40 = data['parameter'].isin(param2.index)

                ## Calc detection limit values
                data3 = pd.merge(data, combo1['dtl_ratio'].reset_index(), on='parameter', how='left')
                data3.loc[:, 'val_dtl'] = data2['val']

                max_dtl_val = data2[over_40 & less1].groupby('parameter')['val'].transform('max')
                max_dtl_val.name = 'dtl_val_max'
                data3.loc[over_40 & less1, 'val_dtl'] = max_dtl_val
        else:
            data3 = data
    else:
        data3 = data

    #### Return and export
    if isinstance(export, str):
        data3.to_csv(export, encoding='utf-8', index=False)
    return data3


