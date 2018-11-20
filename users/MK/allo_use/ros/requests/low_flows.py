# -*- coding: utf-8 -*-
"""
Created on Fri Mar 16 13:39:50 2018

@author: michaelek
"""
import os
import numpy as np
import pandas as pd
from datetime import date, datetime
from pdsql.mssql import rd_sql, rd_sql_ts
#from hydropandas.io.tools.sql_arg_class import sql_arg

#from_date='2018-09-20'
#to_date = '2018-09-20'


###########################################
### Functions


def save_df(df, path_str, index=True, header=True):
    """
    Function to save a dataframe based on the path_str extension. The path_str must  either end in csv or h5.

    df -- Pandas DataFrame.\n
    path_str -- File path (str).\n
    index -- Should the row index be saved? Only necessary for csv.
    """

    path1 = os.path.splitext(path_str)

    if path1[1] in '.h5':
        df.to_hdf(path_str, 'df', mode='w')
    if path1[1] in '.csv':
        df.to_csv(path_str, index=index, header=header)


def telem_corr_sites(site_num=None):
    """
    Function to determine if sites are telemetered or are correlated from telemetered sites in Hydrotel. Output is a list of correlated sites.

    Parameters
    ----------
    site_num: list of str
        Site numbers for the selection.

    Returns
    -------
    List of str
        List of site numbers that are correlated sites.
    """
    ### Parameters
    server = 'SQL2012PROD04'
    database = 'Hydrotel'
    sites_tab = 'Sites'
    obj_tab = 'Objects'

    sites_fields = ['Site', 'ExtSysID']
    obj_fields = ['Site', 'Name']

    where_dict = {'Name': ['calculated flow']}

    ### Read in data
    if isinstance(site_num, list):
        sites = rd_sql(server, database, sites_tab, sites_fields, {'ExtSysID': site_num})
        sites['ExtSysID'] = pd.to_numeric(sites['ExtSysID'], 'coerce')
    else:
        sites = rd_sql(server, database, sites_tab, sites_fields)
        sites['ExtSysID'] = pd.to_numeric(sites['ExtSysID'], 'coerce')
        sites = sites[sites.ExtSysID.notnull()]

    sites['Site'] = sites['Site'].astype('int32')

    where_dict.update({'Site': sites.Site.tolist()})

    obj = rd_sql(server, database, obj_tab, obj_fields, where_dict)
    corr_sites = sites[sites.Site.isin(obj.Site)]

    return corr_sites.ExtSysID.astype('int32').astype(str).tolist()


def min_max_trig(SiteID=None, is_active=True):
    """
    Function to determine the min/max triggers.

    Parameters
    ----------
    SiteID: list of str
        Lowflows internal site numbers for filtering.
    is_active: bool
        Should the output only return active sites/bands?

    Returns
    -------
    DataFrames
        Outputs two DataFrames. The first includes the min and max triggger levels for all bands per site, while the second has the min and max trigger levels for each site and band.
    """

    ######################################
    ### Parameters

    period_fields = ['SiteID', 'BandNo', 'PeriodNo', 'fmDate', 'toDate']
    period_names = ['SiteID', 'band_num', 'period', 'from_date', 'to_date']
    min_flow_fields = ['SiteID', 'BandNo', 'PeriodNo', 'Allocation', 'Flow']
    min_flow_names = ['SiteID', 'band_num', 'period', 'allo_perc', 'trig_level']
    site_type_fields = ['SiteID', 'BandNo']
    site_type_names = ['SiteID', 'band_num']

    ## Databases

    # daily restrictions
    server1 = 'SQL2012PROD03'
    database1 = 'LowFlows'

    # Internal site id, band, and min flow
    min_flow_table = 'LowFlowSiteBandPeriodAllocation'

    # period info
    period_table = 'LowFlowSiteBandPeriod'

    # site band active
    site_type_table = 'LowFlowSiteBand'

    #######################################
    ### Read in data

    periods0 = rd_sql(server1, database1, period_table, period_fields, rename_cols=period_names)

    if isinstance(SiteID, list):
        restr_val = rd_sql(server1, database1, min_flow_table, min_flow_fields, {'SiteID': SiteID}, rename_cols=min_flow_names)
    else:
        restr_val = rd_sql(server1, database1, min_flow_table, min_flow_fields, rename_cols=min_flow_names)

    site_type = rd_sql(server1, database1, site_type_table, site_type_fields, {'isActive': [is_active]}, rename_cols=site_type_names)

    #######################################
    ### Process data

    ## Fix duplicate zero allocations at zero flow
    grp1 = restr_val.groupby(['SiteID', 'band_num', 'period'])
    zeros1 = grp1.min()
    zeros2 = zeros1[zeros1.trig_level == 0]['allo_perc']
    zeros3 = pd.merge(restr_val, zeros2.reset_index(), on=['SiteID', 'band_num', 'period', 'allo_perc'])
    max_zero = zeros3.groupby(['SiteID', 'band_num', 'period', 'allo_perc'])['trig_level'].max()

    all_trig = restr_val.groupby(['SiteID', 'band_num', 'period', 'allo_perc'])['trig_level'].min()

    all_trig[max_zero.index] = max_zero

    ## Periods by month
    periods = pd.merge(periods0, site_type, on=['SiteID', 'band_num'])

    periods['from_mon'] = periods['from_date'].dt.month
    periods['to_mon'] = periods['to_date'].dt.month

    new1_list = []
    for group in periods.itertuples():
        if group.from_mon > group.to_mon:
            first1 = np.arange(group.from_mon, 13).tolist()
            sec1 = np.arange(1, group.to_mon + 1).tolist()
            first1.extend(sec1)
        else:
            first1 = np.arange(group.from_mon, group.to_mon + 1).tolist()

        index1 = [[group.SiteID, group.band_num, group.period]] * len(first1)
        new1 = pd.DataFrame(index1, columns=['SiteID', 'band_num', 'period'])
        new1['mon'] = first1

        new1_list.append(new1)

    periods1 = pd.concat(new1_list).drop_duplicates(['SiteID', 'band_num', 'mon'])

    periods2 = pd.merge(periods1, all_trig.reset_index(), on=['SiteID', 'band_num', 'period']).drop('period', axis=1)
    p_min = periods2[~periods2.allo_perc.isin([103, 105, 106, 107, 108, 109])].groupby(['SiteID', 'band_num', 'mon']).min()
    p_min.columns = ['min_allo_perc', 'min_trig']
    p_max = periods2.groupby(['SiteID', 'band_num', 'mon']).max()
    p_max.columns = ['max_allo_perc', 'max_trig']

    p_min_site = p_min.reset_index().groupby(['SiteID', 'mon'])['min_trig'].min()
    p_max_site = p_max.reset_index().groupby(['SiteID', 'mon'])['max_trig'].max()
    p_set = pd.concat([p_min, p_max], axis=1).reset_index()
    p_set_site = pd.concat([p_min_site, p_max_site], axis=1).reset_index()

    return p_set_site, p_set


def low_flow_restr(sites_num=None, from_date=None, to_date=None, only_restr=False):
    """
    Function to determine the flow sites currently on restriction.

    Parameters
    ----------
    sites_num : list or None
        A list of sites to return, or all sites if None.
    from_date: str
        The start date in the format '2017-01-01'.
    end_date : str
        The end date in the format '2017-01-01'.
    only_restr : bool
        Should only the sites that are on some kind of restriction be returned?

    Returns
    -------
    DataFrames
        Two DataFrames are returned. One is a summary of each site on restriction and one has the sites and bands on restriction.

    Notes
    -----
    This should not be queried for low flows history past the last season as the bands and consents history are not stored. They only reflect active bands and consents.
    """

    ########################################
    ### Parameters

    is_active = True
    hour1 = datetime.now().hour
    today1 = date.today()

    ## Query fields - Be sure to use single quotes for the names!!!
    restr_fields = ['SiteID', 'RestrictionDate', 'BandNo', 'BandAllocation', 'AsmtFlow', 'AsmtOp']
    sites_fields = ['Siteid', 'RefDBaseKey', 'Waterway', 'Location']
    crc_fields = ['SiteID', 'BandNo', 'RecordNo']
    site_type_fields = ['SiteID', 'BandNo', 'RestrictionType']

    ## Equivelant short names for analyses - Use these names!!!
    restr_names = ['SiteID', 'band_num', 'op_flag', 'date', 'flow', 'band_allo']
    sites_names = ['SiteID', 'site', 'waterway', 'location']
    crc_names = ['SiteID', 'band_num', 'crc']
    site_type_names = ['SiteID', 'band_num', 'site_type']
    ass_names = ['SiteID', 'flow_method', 'applies_date', 'date']

    ## Databases

    server1 = 'SQL2012PROD03'
    database1 = 'LowFlows'

    # daily restrictions
    restr_table = 'LowFlowSiteRestrictionDaily'
    restr_where = {'SnapshotType': ['Live']}

    # Sites info
    sites_table = 'LowFlowSite'

    # crc, sites, and bands
    crc_table = 'vLowFlowConsents2'

    # lowflow or residual flow site
    site_type_table = 'LowFlowSiteBand'

    # assessments table
    ass_table = 'LowFlowSiteAssessment'

    # Ass stmt
    ass_stmt = "select SiteID, MethodID, AppliesFromDate, MeasuredDate from LowFlows.dbo.LowFlowSiteAssessment t1 WHERE EXISTS(SELECT 1 FROM LowFlows.dbo.LowFlowSiteAssessment t2 WHERE t2.SiteID = t1.SiteID GROUP BY t2.SiteID HAVING t1.MeasuredDate = MAX(t2.MeasuredDate))"

    ## Method dict
    method_dict = {1: 'Gauged', 2: 'Visually Gauged', 3: 'Telemetered', 4: 'Manually Calculated', 5: 'Correlated from Telem'}


    ########################################
    ### Read in data

    sites = rd_sql(server1, database1, sites_table, sites_fields, {'isActive': [is_active]}, rename_cols=sites_names)

    if only_restr:
        allo_values = list(np.arange(100))
        allo_values.extend(list(np.arange(103, 110)))
    else:
        allo_values = list(np.arange(110))

    restr_day = rd_sql_ts(server=server1, database=database1, table=restr_table, groupby_cols=['SiteID', 'BandNo', 'AsmtOp'], date_col='RestrictionDate',  values_cols=['AsmtFlow', 'BandAllocation'], from_date=from_date, to_date=to_date, where_col={'BandAllocation': allo_values})
    restr_day = restr_day.reset_index()
    restr_day.columns = restr_names

    crc = rd_sql(server1, database1, crc_table, crc_fields, {'isCurrent': [is_active]}, rename_cols=crc_names)

    site_type = rd_sql(server1, database1, site_type_table, site_type_fields, {'isActive': [is_active]}, rename_cols=site_type_names)

    ass1 = rd_sql(server1, database1, stmt=ass_stmt)
    ass1.columns = ass_names

    #######################################
    ### Process data

    restr_day.op_flag = restr_day.op_flag.str.upper().str.strip()

    ## Filter sites if needed
    if isinstance(sites_num, list):
        if isinstance(sites_num[0], str):
            sites = sites[sites.site.isin(sites_num)]
        else:
            raise ValueError('sites_num must be a list of strings')

    ## Periods by month
    p_set_site, p_set = min_max_trig(sites.SiteID.unique().tolist())

    ## Trigger flows
    restr_day['mon'] = restr_day['date'].dt.month
    restr2 = pd.merge(restr_day, p_set, on=['SiteID', 'band_num', 'mon'])

    ## crc counts
    crc_count = crc.groupby(['SiteID', 'band_num'])['crc'].count()
    crc_count.name = 'crc_count'

    ## Combine restr with crc
    restr_crc = pd.merge(restr2, crc_count.reset_index(), on=['SiteID', 'band_num'])

    ## Not only lowflow sites
#    lowflow_site = site_type[site_type.restr_type == 'LowFlow'].copy().drop('restr_type', axis=1)
    restr_crc = pd.merge(restr_crc, site_type, on=['SiteID', 'band_num'], how='inner')

    ## Add in how it was measured and when
    site_type1 = ass1[ass1.SiteID.isin(restr_day.SiteID.unique())].copy()
    tel_sites1 = site_type1[site_type1.flow_method == 3].SiteID
    tel_sites2 = sites.loc[sites.SiteID.isin(tel_sites1), 'site']
    corr_sites1 = telem_corr_sites(tel_sites2.astype('int32').tolist())
    corr_sites2 = sites.loc[sites.site.isin(corr_sites1), 'SiteID']
    site_type1.loc[site_type1.SiteID.isin(corr_sites2), 'flow_method'] = 5
    if to_date is not None:
        site_type1['days_since_flow_est'] = (pd.to_datetime(to_date) - site_type1.date).dt.days
    else:
        site_type1['days_since_flow_est'] = (pd.to_datetime(today1) - site_type1.date).dt.days
    if (hour1 >= 17) | (hour1 < 14):
        site_type1['days_since_flow_est'] = site_type1['days_since_flow_est'] - 1

    site_type2 = site_type1.replace({'flow_method': method_dict}).drop(['applies_date', 'date'], axis=1)

    sites1 = pd.merge(sites, site_type2, on='SiteID')
    num1 = pd.to_numeric(sites1.site, 'coerce')
    sites1.loc[num1.isnull(), 'flow_method'] = 'GW manual'

    ## Aggregate to site and date
    grp1 = restr_crc.sort_values('site_type').groupby(['SiteID', 'date'])
    crcs1 = grp1['crc_count'].sum()
    flow_site = grp1[['site_type', 'flow', 'mon', 'op_flag']].first()
    crc_flow = pd.concat([flow_site, crcs1], axis=1).reset_index()

    restr_sites1 = pd.merge(crc_flow, p_set_site, on=['SiteID', 'mon'], how='left').drop('mon', axis=1)

    ## Add in the restriction categories
    restr_sites1['restr_category'] = 'No'
    restr_sites1.loc[(restr_sites1['flow'] <= restr_sites1['min_trig']), 'restr_category'] = 'Full'
    restr_sites1.loc[(restr_sites1['flow'] < restr_sites1['max_trig']) & (restr_sites1['flow'] > restr_sites1['min_trig']), 'restr_category'] = 'Partial'
    restr_sites1.loc[restr_sites1.op_flag == 'NA', 'restr_category'] = 'Deactivated'
    restr_sites1.drop('op_flag', axis=1, inplace=True)

    ## Add in site numbers
    restr_crc_sites = pd.merge(sites1, restr_crc.drop(['mon', 'min_allo_perc', 'max_allo_perc'], axis=1), on='SiteID').drop('SiteID', axis=1).sort_values(['waterway', 'date', 'min_trig'])
    restr_sites = pd.merge(sites1, restr_sites1, on='SiteID').drop('SiteID', axis=1).sort_values(['waterway', 'date', 'min_trig'])

    ## Correct for duplicate primary keys
    restr_crc_sites.drop_duplicates(['site', 'band_num', 'date'], keep='last', inplace=True)
    restr_sites.drop_duplicates(['site', 'date'], keep='last', inplace=True)

    ######################################
    ### Return

    return restr_sites, restr_crc_sites


def priority_gaugings(num_previous_months=2):
    """
    Function to extract the gauging sites for low flow restrictions that hven't been gauged in a while.

    Parameters
    ----------
    num_previous_months: int
        The number of previous months to query over.

    Returns
    -------
    DataFrame
    """
    ########################################
    ### Parameters

    is_active = True

    ## Query fields - Be sure to use single quotes for the names!!!
    ass_fields = ['SiteID', 'MeasuredDate', 'Flow']
    sites_fields = ['Siteid', 'RefDBaseKey', 'Waterway', 'Location']
    crc_fields = ['SiteID', 'BandNo', 'RecordNo']
    site_type_fields = ['SiteID', 'BandNo', 'RestrictionType']

    ## Equivelant short names for analyses - Use these names!!!
    ass_names = ['SiteID', 'date', 'flow']
    sites_names = ['SiteID', 'site', 'Waterway', 'Location']
    crc_names = ['SiteID', 'band_num', 'crc']
    site_type_names = ['SiteID', 'band_num', 'restr_type']

    ## Databases

    server1 = 'SQL2012PROD03'
    database1 = 'LowFlows'

    # assessments table
    ass_table = 'LowFlowSiteAssessment'

    # Sites info
    sites_table = 'LowFlowSite'

    # crc, sites, and bands
    crc_table = 'LowFlows.dbo.vLowFlowConsents2'

    # lowflow or residual flow site
    site_type_table = 'LowFlowSiteBand'

    ########################################
    ### Read in data

    today1 = pd.to_datetime(date.today())
    from_date = today1 - pd.DateOffset(months=num_previous_months)

    sites = rd_sql(server1, database1, sites_table, sites_fields, {'isActive': [is_active], 'RefDBase': ['Gauging']}, rename_cols=sites_names)

    ass = rd_sql(server1, database1, ass_table, ass_fields, {'MethodID': [1, 2, 3]}, rename_cols=ass_names)

    crc = rd_sql(server1, database1, crc_table, crc_fields, {'isCurrent': [is_active]}, rename_cols=crc_names)

    site_type = rd_sql(server1, database1, site_type_table, site_type_fields, {'isActive': [is_active]}, rename_cols=site_type_names)

    #######################################
    ### Process data

    ## max ass
    max_ass1 = ass.groupby('SiteID')['date'].max().reset_index()
    max_ass = pd.merge(ass, max_ass1, on=['SiteID', 'date']).set_index('SiteID')
    max_ass.columns = ['last_gauging', 'flow']

    ## crc counts
    crc_count = crc.groupby('SiteID')['crc'].count()
    crc_count.name = 'crc_count'

    ## Only low flow sites
    lowflow_site1 = site_type[site_type.restr_type == 'LowFlow'].SiteID.unique()
    lowflow_sites = sites[sites.SiteID.isin(lowflow_site1)].set_index('SiteID')

    ## Combine all df
    sites_lastg = pd.concat([lowflow_sites, max_ass, crc_count], axis=1, join='inner')

    ## Filter out sites that have been gauged recently
    sites_lastg2 = sites_lastg[sites_lastg['last_gauging'] < from_date].sort_values('crc_count', ascending=False)

    ## Add in min and max triggers
    basic, complete = min_max_trig()
    basic2 = basic[basic.mon == today1.month].copy().drop('mon', axis=1).set_index('SiteID')
    basic2['trig_date'] = today1.date()

    ## Combine
    sites_lastg3 = pd.concat([sites_lastg2, basic2], axis=1, join='inner').set_index('site')

    ### Return
    return sites_lastg3




