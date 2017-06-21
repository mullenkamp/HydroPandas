# -*- coding: utf-8 -*-
"""
Stream naturalisation functions.
"""


def stream_nat(catch_shp, catch_sites_csv, include_gw=True, max_date='2015-06-30', sd_csv='S:/Surface Water/shared/base_data/usage/2017-03-24/sd_est_all_mon_vol.csv', flow_csv='S:/Surface Water/shared/base_data/flow/all_flow_rec_data.csv', crc_shp=r'S:\Surface Water\shared\GIS_base\vector\allocations\allo_loc.shp', ros_csv='S:/Surface Water/shared/base_data/usage/2017-03-24/allo_use_ros_ann.csv', catch_col='site', wap_gauge_col='wap', catch_sites_col=['GRIDCODE', 'SITE'], norm_area=False, export=False, export_rec_flow_path='rec_flow_nat.csv', export_gauge_flow_path='gauge_flow_nat.csv'):
    """
    Function to naturalize stream flows from monthly sums of usage.
    """
    from pandas import read_csv, to_datetime, concat, datetime, DataFrame, date_range, merge
    from core.ecan_io import rd_henry, rd_ts
    from numpy import nan, in1d, append
    from geopandas import read_file
    from core.spatial import pts_poly_join, catch_net

    ### Read in data
    ## Stream depletion
    sd = read_csv(sd_csv)
    if include_gw:
        sd1 = sd[sd.dates <= max_date]
    else:
        sd1 = sd[(sd.take_type == 'Take Surface Water') & (sd.dates <= max_date)]

    ## Recorder flow
    if type(flow_csv) is str:
        flow = rd_ts(flow_csv)
        flow.columns = flow.columns.astype(int)
    elif type(flow_csv) is DataFrame:
        flow = flow_csv
        flow.columns = flow.columns.astype(int)

    ## crc shp
    crc_loc = read_file(crc_shp)

    ## Catchment areas shp
    catch = read_file(catch_shp)[[catch_col, 'geometry']]

    ### Spatial processing of WAPs, catchments, and sites
    ## WAPs to catchments sjoin
    crc_catch, catch2 = pts_poly_join(crc_loc, catch, catch_col)

    id_areas = tot_areas = catch2.area

    ## Unique catchments/gauges
#    sites = wap_catch[catch_col].unique()
    sites = catch[catch_col].unique()

    ## Determine upstream catchments
    catch_df, singles_df = catch_net(catch_sites_csv, catch_sites_col)

    ### Next data import
    ## Gaugings
    gaugings = rd_henry(sites=sites.astype('int32'), agg_day=True, sites_by_col=True)
    gaugings.columns = gaugings.columns.astype(int)

    ## site specific flow
    rec_sites = flow.columns[in1d(flow.columns, sites)]
    gauge_sites = sites[~in1d(sites, rec_sites)]
    gauge_sites2 = gaugings.columns[in1d(gaugings.columns, gauge_sites)]
    site_flow = flow[rec_sites]
    gaugings = gaugings[gauge_sites2]

    ### filter down the sites
    sd1a = merge(crc_catch, sd1, on=['crc', 'take_type', 'use_type'])

    ### Remove excessive usages
    sd1a = sd1a[~((sd1a.sd_usage / sd1a.ann_up_allo/12) >= 1.5)]

    ### Create a daily stream depletion for each gauge/recorder
    ## Make empty data frame by the date bounds of the SD
    dates1 = sd1.dates.sort_values().unique()
    first1 = to_datetime(dates1[0][:8] + '01')
    last1 = to_datetime(dates1[-1])

    df1 = DataFrame(nan, index=date_range(first1, last1), columns=sites)
    flow1 = site_flow[first1:last1].sort_index('columns')
    gaugings1 = gaugings[first1:last1].sort_index('columns')

    ## Process SD data
    for i in sites:
        if sum(i == singles_df) == 1:
            crc_site = crc_catch[in1d(crc_catch[catch_col], i)][['crc', 'take_type', 'use_type']].drop_duplicates()
            sd2 = merge(sd1a, crc_site, on=['crc', 'take_type', 'use_type'])
            tot_area = int(id_areas[in1d(id_areas.index, i)].sum())
            tot_areas[i] = tot_area
        else:
            t1 = append(catch_df.loc[i, :].dropna().values, i)
            crc_site = crc_catch[in1d(crc_catch[catch_col], t1)][['crc', 'take_type', 'use_type']].drop_duplicates()
            sd2 = merge(sd1a, crc_site, on=['crc', 'take_type', 'use_type'])
            tot_area = int(id_areas[in1d(id_areas.index, t1)].sum())
            tot_areas[i] = tot_area

        if sd2.size > 0:
            ## Calc monthly volumes
            sd_mon1 = sd2.groupby('dates').sd_usage.sum()
            sd_mon1.index = to_datetime(sd_mon1.index)

            ## convert to rate (m3/s)
            sd_mon2 = (sd_mon1/sd_mon1.index.days_in_month/24/60/60).round(3)

            ## Convert to daily time series
            t2 = sd_mon2.shift(-15, freq='D')

            start1 = datetime(t2.index.year[0], t2.index.month[0], 1)
            end1 = sd_mon2.index[-1]

            t2[start1] = sd_mon2[0]
            t2[end1] = sd_mon2[-1]

            t3 = t2.sort_index()
            sd_day1 = t3.resample('D').interpolate(method='pchip')

            ## Put into empty dataframe
            df1.ix[sd_day1.index, i] = sd_day1

    ## Change nan to 0
    df2 = df1.dropna(axis=1, how='all').round(3)
    df2[df2.isnull()] = 0

    ### Naturalize flows
    ## Recorder flows
    nat_flow1 = flow1.add(df2, axis=1).dropna(axis=1, how='all')
    no_nat_flow = flow1.loc[:, ~in1d(flow1.columns, nat_flow1.columns)]
    nat_flow = concat([nat_flow1, no_nat_flow], axis=1).sort_index('columns')

    ## Gaugings
    nat_gauge1 = gaugings1.add(df2, axis=1).dropna(axis=1, how='all')
    no_nat_gauge = gaugings1.loc[:, ~in1d(gaugings1.columns, nat_gauge1.columns)]
    nat_gauge = concat([nat_gauge1, no_nat_gauge], axis=1).dropna(axis=0, how='all').sort_index('columns')

    ## Normalize to area if desired
    if norm_area:
        # recorder flow in mm/day
        site_order = tot_areas[flow1.columns].values / 60 / 60 / 24 / 1000
        flow_norm = flow1.div(site_order)
        nat_flow_norm = nat_flow.div(site_order)

        # Gauges flow in mm/day
        site_order = tot_areas[gaugings1.columns].values / 60 / 60 / 24 / 1000
        gaugings_norm = gaugings1.div(site_order)
        nat_gauge_norm = nat_gauge.div(site_order)

        ### Export and return results
        if export:
            nat_flow_norm.to_csv(export_rec_flow_path)
            nat_gauge_norm.to_csv(export_gauge_flow_path)
        return([flow_norm, gaugings_norm, nat_flow_norm, nat_gauge_norm])
    else:
        if export:
            nat_flow.to_csv(export_rec_flow_path)
            nat_gauge.to_csv(export_gauge_flow_path)
        return([flow1, gaugings1, nat_flow, nat_gauge])
