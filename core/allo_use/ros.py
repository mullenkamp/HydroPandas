# -*- coding: utf-8 -*-
"""
Reliability of Supply and min flow restrictions functions.
"""


### ROS and min flow restrictions


def ros_proc(allo_use, date_col='date', allo_col='allo', min_days=150, export_use_ros_path=None, export_ann_use_ros_path=None):
    """
    Function to process the reliability of supply data from the low flows db and merge it with the allo_use.
    """
    from pandas import merge, to_datetime
    from core.ecan_io import rd_sql
    from core.misc import save_df

    def resample2(df, min_days=20):
        from numpy import nan

        df.index = df.date
        df_grp = df.resample('M')
        df_count = df_grp['band_restr'].count()
        df2 = (df_grp['band_restr'].sum() / df_count).round(2)
        df2[df_count < min_days] = nan
        df2[df2 > 100] = 100
        return(df2)

    def resample_ann(df):
        from numpy import nan

        df.index = df.date
        df_grp = df.resample('A-JUN')
        df2 = df_grp.sum().round(2)
        df_mean = df_grp['band_restr'].mean().round(2)
        df_first = df_grp[['band', 'site']].first()
        df2['site'] = df_first['site']
        df2['band'] = df_first['band']
        df2['band_restr'] = df_mean
        return(df2)

    def resample1(df):
        df.index = df.date
        df_grp = df.resample('A-JUN')
        df2 = df_grp['up_allo_m3'].transform(sum).round(2)
        return(df2)

    #########################################
    ### Read in data

    restr = rd_sql(code='lowflow_restr_day')
    crc = rd_sql(code='lowflow_band_crc').drop('active', axis=1)
    sites = rd_sql(code='lowflow_gauge').drop(['active', 'DB'], axis=1)


    ## Rename columns

#    restr.columns = restr_names
#    sites.columns = sites_names
#    crc.columns = crc_names
#    usage_names = usage.columns.values.tolist()
#
#    list1 = [[usage_names[0]], ['dates'], usage_names[2:]]
#    usage.columns = [item for sublist in list1 for item in sublist]

    #######################################
    ### Prepare data

    allo_use1 = allo_use.copy()
    allo_use1.loc[date_col] = to_datetime(allo_use1[date_col])
    allo_use1 = allo_use1[allo_use1[allo_col].notnull()]
    restr.loc[restr.band_restr > 100, 'band_restr'] = 100

    #########################################################
    ### Aggregate the daily low flow data to monthly

    restr2_grp1 = restr.groupby(['lowflow_id', 'band'])

    restr2_mon = restr2_grp1.apply(resample2).reset_index()
    restr2_mon = restr2_mon[restr2_mon.band_restr.notnull()]

    #####################################
    ### Combine all together

    ## crc to siteID and band

    crc_band_mon = merge(crc, restr2_mon, on=['lowflow_id', 'band'])
    use_band = merge(allo_use1, crc_band_mon, on=['crc', date_col])

    grp1_names = ['crc', date_col, 'take_type', 'allo_block', 'wap']
    use_band_grp1 = use_band.groupby(grp1_names)

    use_band_min = use_band_grp1.apply(lambda x: x[x.band_restr == x.band_restr.min()]).drop(grp1_names, axis=1).reset_index().drop(['level_5'], axis=1)

    use_band_min2 = use_band_min.drop_duplicates(subset=grp1_names)

    ## Calc new allocation based on flow restrictions

    use_band2 = use_band_min2.copy()
    use_band2['up_allo_m3'] = use_band_min2[allo_col]*use_band_min2['band_restr']*0.01
#    use_band2['up_allo_wqn_m3'] = use_band_min2['mon_vol_wqn']*use_band_min2['band_restr']*0.01

    use_band2.loc[:, 'up_allo_m3'] = use_band2.loc[:, 'up_allo_m3'].round(2)
#    use_band2.loc[:, 'up_allo_wqn_m3'] = use_band2.loc[:, 'up_allo_wqn_m3'].round(2)

    ## Merge other data together for saving

    use_ros1 = merge(use_band2, sites, on=['lowflow_id'], how='left')

#    use_ros1 = merge(use_restr, allo, on=['crc', 'take_type', 'use_type'], how='left')

    ## Combine with all allocation data and update ann allo for all crc

#    use_ros1 = merge(usage, use_restr2[['crc', 'dates', 'band', 'band_restr', 'up_allo_m3', 'up_allo_wqn_m3', 'site']], on=['crc', 'dates'], how='left')

    use_ros1.loc[use_ros1.up_allo_m3.isnull(), 'up_allo_m3'] =  use_ros1.loc[use_ros1.up_allo_m3.isnull(), allo_col]
#    use_ros1.loc[use_ros1.up_allo_wqn_m3.isnull(), 'up_allo_wqn_m3'] = use_ros1.loc[use_ros1.up_allo_wqn_m3.isnull(), 'mon_vol_wqn']

    use_ros1.loc[:, 'up_allo_m3'] = use_ros1.loc[:, 'up_allo_m3'].round(2)
#    use_ros1.loc[:, 'up_allo_wqn_m3'] = use_ros1.loc[:, 'up_allo_wqn_m3'].round(2)

    ## Aggregate to annual allocation for monthly dataframe
    res1 = use_ros1[['crc', date_col, 'take_type', 'allo_block', 'wap', 'up_allo_m3']].groupby(['crc', 'take_type', 'allo_block', 'wap']).apply(resample1).reset_index()
    res1.columns = ['crc', 'take_type', 'allo_block', 'wap', date_col, 'ann_up_allo']
    use_ros1b = merge(use_ros1, res1, on=grp1_names, how='left')
    use_ros1b = use_ros1b[use_ros1b['ann_up_allo'] != 0].drop(['lowflow_id'], axis=1)
    use_ros1b.columns = ['crc', date_col, 'take_type', 'allo_block', 'wap', 'mon_allo_m3', 'mon_usage_m3', 'band', 'band_restr', 'mon_restr_allo_m3', 'gauge_num', 'ann_restr_allo_m3']

    ## Make annual aggregations
    ann_ros = use_ros1.groupby(['crc', 'take_type', 'allo_block', 'wap']).apply(resample_ann)
    ann_ros1 = ann_ros[(ann_ros[allo_col] != 0) & ann_ros[allo_col].notnull()].reset_index().drop(['lowflow_id'], axis=1)
    ann_ros1.columns = ['crc', 'take_type', 'allo_block', 'wap', date_col, 'ann_allo_m3', 'ann_usage_m3', 'band', 'band_restr', 'ann_restr_allo_m3', 'gauge_num']

    ## Save data

    if isinstance(export_use_ros_path, str):
        save_df(use_ros1b, export_use_ros_path, index=False)
        save_df(ann_ros1, export_ann_use_ros_path, index=False)

    return([use_ros1b, ann_ros1])


def restr_days(select, period='A-JUN', months=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], min_sites_shp='S:/Surface Water/shared/GIS_base/vector/low_flows/min_flows_sites_Cant.shp', sites_col='ReferenceN', export=True, export_path='restr_days.csv'):
    """
    Function to determine the number of days on restriction per period according to the LowFlows database.

    Arguments:\n
    select -- Can either be a list of gauging site numbers or a shapefile polygon of an area that contains min flow sites.\n
    period -- Pandas time series code for the time period.
    """
    from pandas import merge, to_datetime, read_csv, concat
    from core.misc import printf, select_sites
    from core.ecan_io import rd_sql
    from core.spatial import sel_sites_poly
    from numpy import ndarray, in1d, nan, array

    ########################################
    ### Parameters

    ## Query fields - Be sure to use single quotes for the names!!!

    restr_fields = ['SiteID', 'RestrictionDate', 'BandNo', 'BandAllocation']
#    sites_fields = ['SiteID', 'RefDBaseKey','RecordNo', 'WellNo']
    crc_fields = ['SiteID', 'BandNo', 'RecordNo']
    sites_fields = ['Siteid', 'RefDBaseKey']


    ## Equivelant short names for analyses - Use these names!!!

    restr_names = ['SiteID', 'dates', 'band_num', 'band_restr']
#    sites_names = ['SiteID', 'gauge_num', 'crc', 'wap']
    crc_names = ['SiteID', 'band_num', 'crc']
    sites_names = ['SiteID', 'gauge_num']

    ## Databases

    #statement = "SELECT * FROM "

    # daily restrictions

    server1 = 'SQL2012PROD03'
    database1 = 'LowFlows'

    restr_table = 'LowFlows.dbo.LowFlowSiteRestrictionDaily'
    restr_where = {'SnapshotType': 'Live'}

    # Sites info

    server2 = 'SQL2012PROD03'
    database2 = 'LowFlows'

    sites_table = 'LowFlows.dbo.vLowFlowSite'

    # crc, sites, and bands

    server3 = 'SQL2012PROD03'
    database3 = 'LowFlows'

    crc_table = 'LowFlows.dbo.vLowFlowConsents2'

    ########################################
    ## Make the sites selection
    if isinstance(select, str):
        if select.endswith('.shp'):
            sites3 = sel_sites_poly(select, min_sites_shp)[sites_col].unique()
        else:
            sites3 = read_csv(select)[sites_col].unique()
    elif isinstance(select, (list, ndarray)):
        sites3 = select_sites(select)

    ########################################
    ### Read in data

    sites = rd_sql(server2, database2, sites_table, sites_fields)
    sites.columns = sites_names

    sites4 = sites.loc[sites.gauge_num.isin(sites3.astype(str)), 'SiteID'].unique().astype('int32').tolist()

    restr_where.update({'SiteID': sites4})

    restr = rd_sql(server1, database1, restr_table, restr_fields, restr_where).drop_duplicates(keep='last')
    restr.columns = restr_names

    crc = rd_sql(server3, database3, crc_table, crc_fields)
    crc.columns = crc_names

    ##################################
    ### Calculate the number of days on full and partial restriction

    ## Remove anything above 100%
    restr1 = restr[restr.band_restr <= 100]

    ## Recategorize band restr
    partial_index = (restr1.band_restr > 0) & (restr1.band_restr < 100)

    restr1.loc[partial_index, 'band_restr'] = 101
    restr1.loc[restr1.band_restr == 100, 'band_restr'] = 103
    restr1.loc[restr1.band_restr == 0, 'band_restr'] = 102

    ## Restrict by months
    mon_index = restr1.dates.dt.month.isin(months)
    restr1 = restr1[mon_index]

    ## Do the work
    def sp_count(df, num):
        df.index = df.dates
        df_grp = df[df.band_restr == num].resample(period)
        df_count = df_grp['band_restr'].count()
        return(df_count)

    restr1_grp = restr1.groupby(['SiteID', 'band_num'])

    partial1 = restr1_grp.apply(sp_count, 101)
    full1 = restr1_grp.apply(sp_count, 102)
#    no1 = restr1_grp.apply(sp_count, 103)

    tot1 = concat([partial1, full1], axis=1)
    tot1.columns = ['partial', 'full']

    tot2 = tot1.reset_index()

    ## Relabel the sites to actually be site number
    sites2 = sites.drop_duplicates()
    tot3 = merge(tot2, sites2, on='SiteID', how='left')
    tot3.loc[tot3.partial.isnull(), 'partial'] = 0
    tot3.loc[tot3.full.isnull(), 'full'] = 0

    tot3 = tot3[tot3.gauge_num.notnull()]

    ## Summarize the results
    restr2 = tot3[['gauge_num', 'band_num', 'dates', 'partial', 'full']]

    if export:
        restr2.to_csv(export_path, index=False)
    return(restr2)


def flow_ros(select=all, start_date='1900-01-01', end_date='2016-06-30', fill_na=False, flow_csv='S:/Surface Water/shared/base_data/flow/flow_data.csv', min_flow_cond_csv='S:/Surface Water/shared/base_data/usage/restrictions/min_flow_cond.csv', min_flow_id_csv='S:/Surface Water/shared/base_data/usage/restrictions/min_flow_id.csv', min_flow_mon_csv='S:/Surface Water/shared/base_data/usage/restrictions/mon_min_flow.csv', min_flow_restr_csv='S:/Surface Water/shared/base_data/usage/restrictions/min_flow_restr.csv'):
    """
    Function to estimate the percent allowable abstraction per band_id.

    Arguments:\n
    select -- Either a list, array, dataframe, or signle column csv file of site numbers.\n
    start_date / end_date -- The start and/or end date for the results as a string.\n
    *_csv -- csv files necessary for the analysis.
    """
    from pandas import read_csv, DataFrame, merge, concat, Series, MultiIndex, to_datetime
    from ast import literal_eval, parse
    from numpy import nan, in1d, where
    from core.misc import select_sites
    from core.ecan_io import rd_ts, rd_hydrotel

    def norm_eval(series):
        if series['lower'] == '0':
            lower1 = '-1'
        else:
            lower1 =  series['lower']
        stmt = '(' + series['object'] + '[' + str(int(series['site'])) + ']' + ' <= ' + series['upper'] + ')' + ' & ' + '(' + series['object'] + '[' + str(int(series['site'])) + ']' + ' > ' + lower1 + ')'
        return(stmt)

    def stmt_set(norm_conds, other_conds):
        if (len(norm_conds) > 0) & (len(other_conds) > 0):
            max1 = norm_conds.ix[norm_conds.index[-1], 'upper']
            new1 = norm_conds.ix[0, :]
            new1.loc['upper'] = '100000'
            new1.loc['lower'] = max1
            new1.loc['cond_id'] = 0
            norm_conds.loc['a', :] = new1
            stmt = [norm_eval(norm_conds.loc[x,:]) for x in norm_conds.index]
            other_stmt = other_conds.other.tolist()
            stmt.append(other_stmt)
            ids = norm_conds.cond_id.tolist()
            ids.append(other_conds.cond_id)
        elif (len(norm_conds) > 0):
            max1 = norm_conds.ix[norm_conds.index[-1], 'upper']
            new1 = norm_conds.iloc[0, :]
            new1.loc['upper'] = '100000'
            new1.loc['lower'] = max1
            new1.loc['cond_id'] = 0
            norm_conds.loc['a', :] = new1
            stmt = [norm_eval(norm_conds.loc[x,:]) for x in norm_conds.index]
            ids = norm_conds.cond_id.tolist()
        elif (len(other_conds) > 0):
            stmt = other_conds.other.tolist()
            ids = other_conds.cond_id.tolist()
        return([stmt, ids])

    def pro_rata(flow, lower, upper):
        perc = (flow - lower) * 100 / (upper - lower)
        perc[perc < 0] = 0
        return(perc)

    ### Read in data tables
    min_flow_cond = read_csv(min_flow_cond_csv).dropna(how='all')
    min_flow_id = read_csv(min_flow_id_csv).dropna(how='all')
    min_flow_mon = read_csv(min_flow_mon_csv).dropna(how='all')
    min_flow_restr = read_csv(min_flow_restr_csv).dropna(how='all')
    if type(flow_csv) is str:
        flow1 = read_csv(flow_csv)
        flow1.loc[:, 'time'] = to_datetime(flow1.loc[:, 'time'])
        flow = flow1.pivot_table('data', 'time', 'site')
    else:
        flow = flow_csv
    flow.columns = flow.columns.astype('int32')

    ### Select specific site bands
    if select is not all:
        bands1 = select_sites(select).astype(str)
        min_flow_id = min_flow_id[in1d(min_flow_id.site.astype(str), bands1)]

    ### Add in additional data from hydrotel if needed
    if sum(min_flow_id.site == 69607) > 0:
        hydrotel_flow_sites = [696501]
        hydrotel_wl_sites = [69660]
        opuha_flow = rd_hydrotel(hydrotel_flow_sites, mtype='flow_tel', resample='day', fun='avg', pivot=True).value
        opuha_flow.columns = opuha_flow.columns.astype(int)
        wl = rd_hydrotel(hydrotel_wl_sites, mtype='swl_tel', resample='day', fun='avg', pivot=True).value
        wl.columns = wl.columns.astype(int)

        UF = (1.288 * flow[69615] + 0.673 * flow[69616] + 2.438 * flow[69618] - 2.415)
        UF.name = 1696297

        flow = concat([flow, opuha_flow, UF], axis=1)
        flow.columns = flow.columns.astype(int)

    ### Create monthly time series of flow restrictions
    mon_series1 = DataFrame(flow.index.month, index=flow.index, columns=['mon'])
    mon_series = merge(mon_series1, min_flow_mon, on='mon', how='left')
    mon_series.index = mon_series1.index

    ### Run through each band
    ## Create blank dataframe
    c1 = min_flow_id.site.tolist()
    c2 = min_flow_id.allo_band_id.tolist()

    index1 = MultiIndex.from_tuples(list(zip(*[c1, c2])))
    if sum(min_flow_id.site == 69607) > 0:
        eval_dict = {'flow': flow, 'wl': wl, 'mon_series': mon_series}
    else:
        eval_dict = {'flow': flow, 'mon_series': mon_series}

    allow1 = DataFrame(nan, index=flow.index, columns=index1)

    for j in min_flow_id.index:
        site_id = min_flow_id.site[j]
        band_id = min_flow_id.allo_band_id[j]
        t1 = min_flow_id.loc[j, :]
        cond_id = literal_eval(t1['cond_id'])
        cond_id.extend([0])
        restr_id = literal_eval(t1['restr_id'])
        restr_id.extend(['r100'])
        cond_restr = dict(zip(cond_id, restr_id))

        conds1 = min_flow_cond[in1d(min_flow_cond.cond_id, cond_id)]
        norm_conds = conds1[conds1.object != 'other']
        other_conds = conds1[conds1.object == 'other']

        stmt, ids = stmt_set(norm_conds, other_conds)

        df1 = concat((eval(x, globals(), eval_dict) for x in stmt), axis=1)
        df1.columns = ids
        df2 = df1.copy()
        df2.loc[:, :] = nan

        perc_restr = {}
        for x in cond_restr:
            if cond_restr[x] != 'pro_rata':
                perc_restr.update({x: eval(min_flow_restr.loc[min_flow_restr.restr_id == cond_restr[x], 'restr_cond'].values[0], globals(), eval_dict)})
            else:
                seta = norm_conds.loc[norm_conds.cond_id == x,:]
                pr1 = pro_rata(flow[int(seta.site)], float(seta.lower), float(seta.upper))
                perc_restr.update({x: pr1})

        for i in perc_restr:
            index = df1[i].dropna().index[where(df1[i].dropna())[0]]
            if type(perc_restr[i]) is Series:
                df2.ix[index, i] = perc_restr[i][index]
            else:
                df2.ix[index, i] = perc_restr[i]

        ## Take the most restrictive between the conditions
        df3 = df2.min(axis=1)

        ### Process exemptions
        if t1['exempt_id'] is not nan:
            exempt_id = literal_eval(t1['exempt_id'])
            exempt_restr_id = literal_eval(t1['exempt_restr_id'])
            exempt_cond_restr = dict(zip(exempt_id, exempt_restr_id))

            conds1 = min_flow_cond[in1d(min_flow_cond.cond_id, exempt_id)]
            norm_conds = conds1[conds1.object != 'other']
            other_conds = conds1[conds1.object == 'other']

            stmt, ids = stmt_set(norm_conds, other_conds)

            df1 = concat((eval(x, globals(), eval_dict) for x in stmt), axis=1)
            df1.columns = ids
            df2 = df1.copy()
            df2.loc[:, :] = nan

            perc_restr = {x: eval(min_flow_restr.loc[min_flow_restr.restr_id == exempt_cond_restr[x], 'restr_cond'].values[0], globals(), eval_dict) for x in exempt_cond_restr}

            for i in perc_restr:
                index = df1[i].dropna().index[where(df1[i].dropna())[0]]
                if type(perc_restr[i]) is Series:
                    df2.ix[index, i] = perc_restr[i][index]
                else:
                    df2.ix[index, i] = perc_restr[i]

            ## Take the most restrictive for the exemptions
            df3_exempt = df2.min(axis=1)

            ### Take the least restrictive between the primary conditions and the exemptions
            allow1.loc[:, (site_id, band_id)] = concat([df3, df3_exempt], axis=1).max(axis=1)

        else:
            allow1.loc[:, (site_id, band_id)] = df3

    ### Constrain results to dates
    allow2 = allow1[start_date:end_date].round(1)
    if fill_na:
        allow2 = allow2.fillna(method='ffill')
    return(allow2)


def crc_band_flow(site_lst=None, crc_lst=None, names=False):
    """
    Function to determine the min flow conditions for each flow site, band, and crc.
    """
    from pandas import merge, to_datetime, read_csv, concat
    from core.ecan_io import rd_sql
    from core.spatial import sel_sites_poly
    from numpy import ndarray, in1d
    from core.misc import select_sites

    ### Database parameters
    # crc, sites, and bands

    server = 'SQL2012PROD03'
    database = 'LowFlows'

    crc_table = 'vLowFlowConsents2'

    # id and gauge site

    gauge_table = 'LowFlowSite'

    # Internal site id, band, and min flow

    min_flow_table = 'LowFlowSiteBandPeriodAllocation'

    ## fields and associated column names
    crc_fields = ['SiteID', 'BandNo', 'RecordNo']
    crc_names = ['id', 'band', 'crc']

    if names:
        gauge_fields = ['SiteID', 'RefDBaseKey', 'Waterway', 'Location']
        gauge_names = ['id', 'site', 'Waterway', 'Location']
    else:
        gauge_fields = ['SiteID', 'RefDBaseKey']
        gauge_names = ['id', 'site']

    min_flow_fields = ['SiteID', 'BandNo', 'PeriodNo', 'Allocation', 'Flow']
    min_flow_names = ['id', 'band', 'mon', 'allo', 'min_flow']

    ### Load in data

    crc = rd_sql(server, database, crc_table, crc_fields)
    crc.columns = crc_names

    gauge = rd_sql(server, database, gauge_table, gauge_fields)
    gauge.columns = gauge_names

    min_flow = rd_sql(server, database, min_flow_table, min_flow_fields)
    min_flow.columns = min_flow_names

    ### Remove min flows that are not restricted
    min_flow1 = min_flow[min_flow.allo < 100]

    ### Lots of table merges!
    crc_min_flow = merge(crc, min_flow1, on=['id', 'band'])
    crc_min_gauge = merge(gauge, crc_min_flow, on='id').drop('id', axis=1)

    ### Query results
    if crc_lst is not None:
        crc_sel = select_sites(crc_lst)
        sel1 = crc_min_gauge[in1d(crc_min_gauge.crc, crc_sel)]
    else:
        sel1 = crc_min_gauge
    if site_lst is not None:
        site_sel = select_sites(site_lst).astype(str)
        sel2 = sel1[in1d(sel1.site, site_sel)]
    else:
        sel2 = sel1

    return(sel2)


def ros_freq(ros, period='water year', min_days=245, norm=False):
    """
    Function to take daily ROS restriction times series and estimate the number of days under partial and full restriction.

    Arguments:\n
    period -- Can be either 'water year' or 'summer'.
    """
    from numpy import in1d, nan
    from pandas import concat

    ## Number of data days
    days = ros.resample('A-JUN').count()

    if period == 'summer':
        sel_index = in1d(ros.index.month, [10, 11, 12, 1, 2, 3, 4])
        ros1 = ros[sel_index]
    else:
        ros1 = ros

    ## Number of days in the year
    ros0 = ros1.iloc[:, 0].fillna(0)
    daysinyear = ros0.resample('A-JUN').count()

    ## Recategorize band restr
    partial_index = (ros1 > 0) & (ros1 < 100)

    ros2 = ros1.copy()
    ros2[partial_index] = 101
    ros2[ros == 100] = 103
    ros2[ros == 0] = 102

    ## Run the stats
    def sp_count(df, num):
        df_grp = df[df == num].resample('A-JUN')
        df_count = df_grp.count()
        df_count[days < min_days] = nan
        return(df_count)

    partial1 = sp_count(ros2, 101)
    full1 = sp_count(ros2, 102)

#    tot1 = concat([partial1, full1], axis=1)
#    tot1.columns = ['partial', 'full']
    if norm:
        partial2 = partial1.div(daysinyear.values, axis='index').round(3)
        full2 = full1.div(daysinyear.values, axis='index').round(3)
    else:
        partial2 = partial1
        full2 = full1

    return([partial2, full2])
