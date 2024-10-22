# -*- coding: utf-8 -*-
"""
Stream naturalisation functions.
"""
import pandas as pd
import geopandas as gpd
from hydropandas.io.tools.general_ts import rd_ts
from hydropandas.tools.general.spatial.vector import pts_poly_join
from hydropandas.util.misc import select_sites, save_df


def stream_nat(sites, catch_shp=r'S:\Surface Water\shared\GIS_base\vector\catchments\catch_delin_recorders.shp', include_gw=True, max_date='2015-06-30', sd_hdf='S:/Surface Water/shared/base_data/usage/sd_est_all_mon_vol.h5', flow_csv=None, crc_shp=r'S:\Surface Water\shared\GIS_base\vector\allocations\allo_gis.shp', catch_col='site', pivot=False, return_data=False, export_path=None):
    """
    Function to naturalize stream flows from monthly sums of usage.

    Parameters
    ----------
    sites: list, ndarray, Series
        A list of recorder sites to be naturalised.
    catch_shp: str
        A shapefile of the delineated catchments for all recorders.
    include_gw: bool
        Should stream depleting GW takes be included?
    max_date: str
        The last date to be naturalised. In the form of '2015-06-30'.
    sd_hdf: str
        The hdf file of all the crc/waps with estimated usage and allocation.
    flow_csv: str or None
        If None, then use the hydro class to import the data. Otherwise, flow data can be imported as a csv file with the first column as datetime and each other column as a recorder site in m3/s. It can also be a dataframe.
    crc_shp: str
        A shapefile of all of th locations of the crc/waps.
    pivot: bool
        Should the output be pivotted?
    return_data: bool
        Should the allocation/usage time series be returned?
    export_path: str or None
        Path to save results as either hdf or csv (or None).

    Returns
    -------
    DataFrame
    """

    qual_codes = [10, 18, 20, 50]

    ### Read in data
    ## Site numbers
    sites1 = select_sites(sites)

    ## Stream depletion
    sd = pd.read_hdf(sd_hdf)
    sd.time = pd.to_datetime(sd.time)
    if include_gw:
        sd1 = sd[sd.time <= max_date]
    else:
        sd1 = sd[(sd.take_type == 'Take Surface Water') & (sd.time <= max_date)]

    ## Recorder flow
    if type(flow_csv) is str:
        flow = rd_ts(flow_csv)
        flow.columns = flow.columns.astype(int)
        flow.index.name = 'time'
        flow.columns.name = 'site'
        flow = flow.stack()
        flow.name = 'flow'
        flow.index = flow.index.reorder_levels(['site', 'time'])
        flow = flow.sort_index()
    elif isinstance(flow_csv, pd.DataFrame):
        flow = flow_csv.copy()
        flow.columns = flow.columns.astype(int)
        flow.index.name = 'time'
        flow.columns.name = 'site'
        flow = flow.stack()
        flow.name = 'flow'
        flow.index = flow.index.reorder_levels(['site', 'time'])
        flow = flow.sort_index()
    elif isinstance(flow_csv, pd.Series):
        flow = flow_csv.copy()
    else:
        raise ValueError('Pass something useful to flow_csv.')

    ## crc shp
    crc_loc = gpd.read_file(crc_shp)
    crc_loc1 = pd.merge(crc_loc[['crc', 'take_type', 'allo_block', 'wap', 'use_type', 'geometry']], sd[['crc', 'take_type', 'allo_block', 'wap', 'use_type']].drop_duplicates(), on=['crc', 'take_type', 'allo_block', 'wap', 'use_type'])

    ## Catchment areas shp
    catch = gpd.read_file(catch_shp).drop('NZREACH', axis=1)
    catch = catch[catch[catch_col].isin(sites1)]

    ### Spatial processing of WAPs, catchments, and sites
    ## WAPs to catchments sjoin
    crc_catch, catch2 = pts_poly_join(crc_loc1, catch, catch_col)

#    id_areas = catch2.area.copy()
#    tot_areas = catch2.area.copy()
#
#    ## Unique catchments/gauges
##    sites = wap_catch[catch_col].unique()
#    sites2 = catch[catch_col].unique()

    ### Next data import
    ## Gaugings
#    gaugings = rd_henry(sites=sites.astype('int32'), agg_day=True, sites_by_col=True)
#    gaugings.columns = gaugings.columns.astype(int)

    ## site specific flow
#    rec_sites = flow.columns[in1d(flow.columns, sites)]
#    gauge_sites = sites[~in1d(sites, rec_sites)]
#    gauge_sites2 = gaugings.columns[in1d(gaugings.columns, gauge_sites)]
#    site_flow = flow[rec_sites]
#    gaugings = gaugings[gauge_sites2]

    ### filter down the sites
    sd1a = pd.merge(crc_catch, sd1, on=['crc', 'take_type', 'allo_block', 'wap', 'use_type']).drop('geometry', axis=1)

    ### Remove excessive usages
    sd1a = sd1a[~((sd1a.sd_usage / sd1a.ann_restr_allo_m3/12) >= 1.5)]

    ### Calc SD for site and month
    sd2 = sd1a.groupby(['site', 'time'])['sd_usage'].sum().reset_index()
    days1 = sd2.time.dt.daysinmonth
    sd2['sd_rate'] = sd2.sd_usage/days1/24/60/60

    ### Resample SD to daily time series
    days2 = pd.to_timedelta((days1/2).round().astype('int32'), unit='D')
    sd3 = sd2.drop('sd_usage', axis=1)
    sd3.loc[:, 'time'] = sd3.loc[:, 'time'] - days2
    grp1 = sd3.groupby(['site'])
    first1 = grp1.first()
    last1 = sd2.groupby('site')[['time', 'sd_rate']].last()
    first1.loc[:, 'time'] = pd.to_datetime(first1.loc[:, 'time'].dt.strftime('%Y-%m') + '-01')
    sd4 = pd.concat([first1.reset_index(), sd3, last1.reset_index()]).reset_index(drop=True).sort_values(['site', 'time'])
    sd5 = sd4.set_index('time')
    sd6 = sd5.groupby('site').apply(lambda x: x.resample('D').interpolate(method='pchip'))['sd_rate']

    ### Naturalise flows
    nat1 = pd.concat([flow, sd6], axis=1, join='inner')
    nat1['nat_flow'] = nat1['flow'] + nat1['sd_rate']


    ## Normalize to area if desired
#    if norm_area:
#        # recorder flow in mm/day
#        site_order = tot_areas[flow1.columns].values / 60 / 60 / 24 / 1000
#        flow_norm = flow1.div(site_order)
#        nat_flow_norm = nat_flow.div(site_order)
#
#        # Gauges flow in mm/day
#        site_order = tot_areas[gaugings1.columns].values / 60 / 60 / 24 / 1000
#        gaugings_norm = gaugings1.div(site_order)
#        nat_gauge_norm = nat_gauge.div(site_order)
#
#        ### Export and return results
#        if export:
#            nat_flow_norm.to_csv(export_rec_flow_path)
#            nat_gauge_norm.to_csv(export_gauge_flow_path)
#        return([flow_norm, gaugings_norm, nat_flow_norm, nat_gauge_norm])
#    else:
#        if export:
#            nat_flow.to_csv(export_rec_flow_path)
#            nat_gauge.to_csv(export_gauge_flow_path)
#        return([flow1, gaugings1, nat_flow, nat_gauge])
    if pivot:
        nat2 = nat1.round(3).unstack('site')
    else:
        nat2 = nat1.round(3)
    if isinstance(export_path, str):
        save_df(nat2, export_path)
    if return_data:
        return nat2, sd1a
    else:
        return nat2
