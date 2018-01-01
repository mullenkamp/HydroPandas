# -*- coding: utf-8 -*-
"""
Created on Tue Jan 02 10:05:15 2018

@author: MichaelEK
Functions to read hydrotel data.
"""


def rd_hydrotel(sites, mtype='river_flow_cont_raw', from_date=None, to_date=None, resample_code='D', period=1,
                fun='mean', val_round=3, min_count=None, pivot=False, export_path=None):
    """
    Function to extract time series data from the hydrotel database.

    Parameters
    ----------
    sites : list, array, dataframe, or str
        Site list or a str path to a single column csv file of site names/numbers.
    mtype : str
        'flow_tel', 'gwl_tel', 'precip_tel', 'swl_tel', or 'wtemp_tel'.
    from_date : str or None
        The start date in the format '2000-01-01'.
    to_date : str or None
        The end date in the format '2000-01-01'.
    resample_code : str
        The Pandas time series resampling code. e.g. 'D' for day, 'W' for week, 'M' for month, etc.
    period : int
        The number of resampling periods. e.g. period = 2 and resample = 'D' would be to resample the values over a 2 day period.
    fun : str
        The resampling function. i.e. mean, sum, count, min, or max. No median yet...
    val_round : int
        The number of decimals to round the values.
    pivot : bool
        Should the output be pivotted into wide format?
    export_path : str or None
        The path and file name to be saved.

    Returns
    -------
    Series or DataFrame
        A MultiIndex Pandas Series if pivot is False and a DataFrame if True
    """

    #### mtypes dict
    mtypes_dict = {'river_flow_cont_raw': 'Flow Rate', 'aq_wl_cont_raw': 'Water Level',
                   'atmos_precip_cont_raw': 'Rainfall Depth', 'river_wl_cont_raw': 'Water Level',
                   'river_wtemp_cont_raw': 'Water Temperature'}

    #### Database parameters
    server = 'SQL2012PROD05'
    database = 'Hydrotel'

    data_tab = 'Hydrotel.dbo.Samples'
    points_tab = 'Hydrotel.dbo.Points'
    objects_tab = 'Hydrotel.dbo.Objects'
    mtypes_tab = 'Hydrotel.dbo.ObjectVariants'
    sites_tab = 'Hydrotel.dbo.Sites'

    data_col = ['Point', 'DT', 'SampleValue']
    points_col = ['Point', 'Object']
    objects_col = ['Object', 'Site', 'Name', 'ObjectVariant']
    mtypes_col = ['ObjectVariant', 'Name']
    sites_col = ['Site', 'Name', 'ExtSysId']

    #### Import data and select the correct sites

    sites = select_sites(sites)
    if mtype == 'atmos_precip_cont_raw':
        site_ob1 = rd_sql(server, database, objects_tab, ['Site', 'ExtSysId'], 'ExtSysId',
                          sites.astype('int32').tolist())
        site_val0 = rd_sql(server, database, sites_tab, ['Site', 'Name'], 'Site', site_ob1.Site.tolist())
        site_val1 = merge(site_val0, site_ob1, on='Site')
    elif mtype == 'aq_wl_cont_raw':
        site_val0 = rd_sql(server, database, sites_tab, ['Site', 'Name'])
        site_val0.loc[:, 'Name'] = site_val0.apply(lambda x: x.Name.split(' ')[0], axis=1)
        site_val1 = site_val0[site_val0.Name.isin(sites)]
        site_val1.loc[:, 'ExtSysId'] = site_val1.loc[:, 'Name']
    else:
        site_val1 = rd_sql(server, database, sites_tab, sites_col, 'ExtSysId', sites.astype('int32').tolist())

    if site_val1.empty:
        raise ValueError('No site(s) in database')

    site_val1.loc[:, 'ExtSysId'] = to_numeric(site_val1.loc[:, 'ExtSysId'], errors='ignore')
    site_val = site_val1.Site.astype('int32').tolist()
    if isinstance(mtype, (list, ndarray, Series)):
        mtypes = [mtypes_dict[i] for i in mtype]
    elif isinstance(mtype, str):
        mtypes = [mtypes_dict[mtype]]
    else:
        raise ValueError('mtype must be a str, list, ndarray, or Series.')
    mtypes_val = rd_sql(server, database, mtypes_tab, mtypes_col, 'Name', mtypes)

    where_col = {'Site': site_val, 'ObjectVariant': mtypes_val.ObjectVariant.astype('int32').tolist()}

    object_val1 = rd_sql(server, database, objects_tab, objects_col, where_col)
    if mtype == 'gwl_tel':
        object_val1 = object_val1[object_val1.Name == 'Water Level']
    if mtype == 'precip_tel':
        object_val1 = object_val1[object_val1.Name == 'Rainfall']
    object_val = object_val1.Object.values.astype(int).tolist()

    #### Rearrange data
    point_val1 = rd_sql(server, database, points_tab, points_col, where_col='Object', where_val=object_val)
    point_val = point_val1.Point.values.astype(int).tolist()

    #### Big merge
    comp_tab1 = merge(site_val1, object_val1[['Object', 'Site']], on='Site')
    comp_tab2 = merge(comp_tab1, point_val1, on='Object')
    comp_tab2.set_index('Point', inplace=True)

    #### Pull out the data
    ### Make SQL statement
    data1 = rd_sql_ts(server, database, data_tab, 'Point', 'DT', 'SampleValue', resample_code, period, fun, val_round,
                      {'Point': point_val}, from_date=from_date, to_date=to_date, min_count=min_count)['SampleValue']

    data1.index.names = ['site', 'time']
    data1.name = 'value'
    site_numbers = [comp_tab2.loc[i, 'ExtSysId'] for i in data1.index.levels[0]]
    data1.index.set_levels(site_numbers, level='site', inplace=True)

    if pivot:
        data3 = data1.unstack(0)
    else:
        data3 = data1

    #### Export and return
    if export_path is not None:
        save_df(data3, export_path)

    return data3
