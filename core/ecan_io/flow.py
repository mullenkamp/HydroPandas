# -*- coding: utf-8 -*-
"""
Functions for importing flow data.
"""


def rd_ts(csv, index=1, header='infer', skiprows=0, reg=False, **kwargs):
    """
    Simple function to read in time series data and make it regular if needed.
    """
    from pandas import read_csv
    from core.ts import tsreg

    ts = read_csv(csv, parse_dates=[index - 1], infer_datetime_format=True, index_col=0, dayfirst=True, skiprows=skiprows, header=header)
    if reg:
        ts = tsreg(ts, **kwargs)
    return(ts)


def rd_henry(sites, sites_col=1, from_date=None, to_date=None, agg_day=True, sites_by_col=False, min_filter=None, export=False, export_path='gauge_flows.csv'):
    """
    Function to read in gaugings data from the "Henry DB". Hopefully, they keep this around for a while longer.

    Arguments:\n
    sites -- Either a list of site names or a file path string that contains a column of site names.\n
    sites_col -- If 'sites' is a path string, then the column that contains site names.\n
    from_date -- A date string for the start of the data (e.g. '2010-01-01').\n
    to_date -- A date string for the end of the data.\n
    agg_day -- Should the gauging dates be aggregated down to the day as opposed to having the hour and minute. Gaugings are aggregated by the mean.\n
    sites_by_col -- 'False' does not make a time series, rather it is organized by site, date, and gauging. 'True' creates a time series with the columns as gauging sites (will create many NAs).\n
    min_filter -- Minimum number of days required for the gaugings output.
    """
    from core.ecan_io import rd_sql
    from pandas import read_csv
    from numpy import ndarray, in1d
    from core.misc import select_sites

    def resample1(df):
        df.index = df.date
        df2 = df.resample('D').mean()
        return(df2)

    #### Fields and names for databases

    ## Query fields - Be sure to use single quotes for the names!!!

    fields = ['SiteNo', 'SampleDate', 'Flow']

    ## Equivelant short names for analyses - Use these names!!!

    names = ['site', 'date', 'flow']

    #### Databases

    ### Gaugings data

    server = 'SQL2012PROD03'
    database = 'DataWarehouse'

    table = 'DataWarehouse.dbo.F_SG_BGauging'
    where_col = 'SiteNo'

    ## Will change to the following!!! Or stay as a duplicate...

    #database1 = 'Hydstra'

    #table1 = 'Hydstra.dbo.GAUGINGS'

    ########################################
    ### Read in data

    if sites is not None:
        sites1 = select_sites(sites).tolist()
        data = rd_sql(server=server, database=database, table=table, col_names=fields, where_col=where_col, where_val=sites1).dropna()
        data.columns = names

    ### Aggregate duplicates

    data2 = data.groupby(['site', 'date']).mean().reset_index()

    ### Aggregate by day

    if agg_day:
        data3 = data2.groupby(['site']).apply(resample1).reset_index().dropna()
    else:
        data3 = data2

    ### Filter out sites with less than min_filter
    if min_filter is not None:
        count1 = data3.groupby('site')['flow'].count()
        count_index = count1[count1 >= min_filter ].index
        data3 = data3[in1d(data3.site.values, count_index)]

    ### Select within date range
    if from_date is not None:
        data3 = data3[data3.date >= from_date]
    if to_date is not None:
        data3 = data3[data3.date <= to_date]

    ### reorganize data with sites as columns and dates as index

    if sites_by_col:
        data4 = data3.pivot(index='date', columns='site').xs('flow', axis=1).round(4)
    else:
        data4 = data3.round(4)

    if export:
        if sites_by_col:
            data4.to_csv(export_path)
        else:
            data4.to_csv(export_path, index=False)

    return(data4)


def rd_hydstra_csv(csv_path, qual_codes=False, min_qual_code=30, min_filter=False, min_yrs=25):
    """
    Read in one csv file exported by Hydstra via the HYDCSV tool.
    It returns a pandas time series dataframe.

    Arguments:\n
    csv_path -- Full path to the Hydstra csv file.\n
    min_filter -- Should the data be filtered based on a minimum number of
    years?\n
    min_yrs -- The minimum number of years to be filtered.
    """
    from pandas import read_table, DataFrame
    from core.ts.sw import flow_stats
    from numpy import nan, isnan

    if qual_codes:
        t1_index = read_table(csv_path, sep=',', nrows=1, header=None)
        t1_index2 = t1_index.values[0]
        t1_index3 = [i for i in t1_index2[1:] if not isnan(i)]
        col_num = range(len(t1_index2))
        t1 = read_table(csv_path, sep=',', parse_dates=[0], header=None, infer_datetime_format=True, skiprows=3, index_col=0, dayfirst=True)
        qual_index = t1[t1.columns[1::2]] > min_qual_code
        t2 = t1[t1.columns[::2]]
        t2.columns = t1_index3
        for i in range(len(t2.columns)):
            df_col = t2.columns[i]
            t2.loc[qual_index.iloc[:,i], df_col] = nan
        t3 = t2.dropna(axis=1, how='all')
        t3.index.name = 'date'

    else:
        t1_index = read_table(csv_path, sep=',', nrows=1, header=None)
        t1_index2 = t1_index.values[0]
        col_num = range(len(t1_index2))
        t1 = read_table(csv_path, sep=',', parse_dates=[0], infer_datetime_format=True, skiprows=2, header=0, index_col=0, names=t1_index2, usecols=col_num, dayfirst=True)
        t2 = DataFrame(t1)
        t3 = t2.dropna(axis=1, how='all')

    if min_filter:
        stats1 = flow_stats(t3).loc['Tot data yrs']
        t4 = t3.loc[:, stats1 >= min_yrs]
    else:
        t4 = t3

    return(t4)


def rd_hydstra_dir(input_path, min_filter=False, min_yrs=25, export=False, export_path='', export_name='hydsra_export.csv'):
    """
    Read in all csv files exported by Hydstra via the HYDCSV tool
    within a specified directory.
    It returns a pandas time series dataframe.

    Arguments:\n
    input_path -- Full path to the directory containing the csv files.\n
    min_filter -- Should the data be filtered based on a minimum number of
    years?\n
    min_yrs -- The minimum number of years to be filtered.\n
    output_name -- Name of the csv file for the export.\n
    output_path -- Path where the export csv will be saved. If None then
    output_path = input_path.\n
    """
    from pandas import concat
    from os import listdir, path
    from fnmatch import filter
    from core.ecan_io import rd_hydstra_csv
    from core.misc import rd_dir

    files = rd_dir(input_path, 'csv', False)

    t1 = concat((rd_hydstra_csv(path.join(input_path, f), min_filter=min_filter, min_yrs=min_yrs) for f in files), axis=1)

    if export:
        t1.to_csv(path.join(export_path, export_name))
    return(t1)


def rd_hydrotel(select=None, mtype='flow_tel', from_date=None, to_date=None, use_site_name=False, resample='day', fun='avg', pivot=False, export=False, export_path='hydrotel_data.csv'):
    """
    Function to extract time series data from the hydrotel database.

    Arguments:\n
    select -- Either a list, array, dataframe, or signle column csv file of site names or numbers.\n
    input_type -- What the values in 'select' are. Either 'number' or 'name'.\n
    mtype -- 'flow_tel', 'gwl_tel', 'precip_tel', 'swl_tel', or 'wtemp_tel'.\n

    Resampling of the time series can be performed by the w_resample function. Any associated resampling parameters can be passed.
    """
    from core.ecan_io import rd_sql
    from pandas import to_datetime, merge, to_numeric, Series, Timestamp
    from numpy import ndarray
    from core.misc.misc import select_sites

    #### mtypes dict
    mtypes_dict = {'flow_tel': 'Flow Rate', 'gwl_tel': 'Water Level', 'precip_tel': 'Rainfall Depth', 'swl_tel': 'Water Level', 'wtemp_tel': 'Water Temperature'}

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

    if select is not None:
        sites = select_sites(select)
        if mtype == 'precip_tel':
            site_ob1 = rd_sql(server, database, objects_tab, ['Site', 'ExtSysId'], 'ExtSysId', sites.astype('int32').tolist())
            site_val0 = rd_sql(server, database, sites_tab, ['Site', 'Name'], 'Site', site_ob1.Site.tolist())
            site_val1 = merge(site_val0, site_ob1, on='Site')
        elif mtype == 'gwl_tel':
            site_val0 = rd_sql(server, database, sites_tab, ['Site', 'Name'])
            site_val0.loc[:, 'Name'] = site_val0.apply(lambda x: x.Name.split(' ')[0], axis=1)
            site_val1 = site_val0[site_val0.Name.isin(sites)]
            site_val1.loc[:, 'ExtSysId'] = site_val1.loc[:, 'Name']
        else:
            site_val1 = rd_sql(server, database, sites_tab, sites_col, 'ExtSysId', sites.astype('int32').tolist())
        site_val1.loc[:, 'ExtSysId'] = to_numeric(site_val1.loc[:,'ExtSysId'], errors='ignore')
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
    else:
        site_val1 = rd_sql(server, database, sites_tab, sites_col)
        site_val1.loc[:,'ExtSysId'] = to_numeric(site_val1.loc[:,'ExtSysId'], errors='ignore')
        site_val1 = site_val1[site_val1['ExtSysId'].notnull()]
        site_val1.loc[:,'ExtSysId'] = site_val1.loc[:,'ExtSysId'].astype('int32')
        site_val = site_val1.Site.values.tolist()
        where_col = {'Site': site_val, 'Name': [mtype]}
        object_val1 = rd_sql(server, database, objects_tab, objects_col, where_col)
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
    where_col_stmt = 'Point IN (' + str(point_val)[1:-1] + ')'

    if isinstance(from_date, str):
        from_date1 = to_datetime(from_date, errors='coerce')
        if isinstance(from_date1, Timestamp):
            from_date2 = from_date1.strftime('%Y-%m-%d')
            where_from_date = "DT >= " + from_date2.join(['\'', '\''])
    else:
        where_from_date = ''

    if isinstance(to_date, str):
        to_date1 = to_datetime(to_date, errors='coerce')
        if isinstance(to_date1, Timestamp):
            to_date2 = to_date1.strftime('%Y-%m-%d')
            where_to_date = "DT <= " + to_date2.join(['\'', '\''])
    else:
        where_to_date = ''

    where_lst = [i for i in [where_col_stmt, where_from_date, where_to_date] if len(i) > 0]

    if resample is not None:
        stmt1 = "SELECT " + "Point AS site, DATEADD(" + resample + ", DATEDIFF(" + resample + ", 0, DT), 0) AS time, round(" + fun + "(SampleValue), 3) AS value" + " FROM " + data_tab + " where " + " and ".join(where_lst) + "GROUP BY Point, DATEADD(" + resample + ", DATEDIFF(" + resample + ", 0, DT), 0) ORDER BY site, time"
    else:
        stmt1 = "SELECT Point AS site, DT AS time, SampleValue AS value FROM " + data_tab + " where " + " and ".join(where_lst)

    data1 = rd_sql(server, database, data_tab, stmt=stmt1)
#    data1.columns = ['site', 'time', 'value']
    data1.set_index(['site', 'time'], inplace=True)
    site_numbers = [comp_tab2.loc[i, 'ExtSysId'] for i in data1.index.levels[0]]
    data1.index.set_levels(site_numbers, level='site', inplace=True)

    if pivot:
        data3 = data1.unstack(0)
    else:
        data3 = data1

    #### Export and return
    if export:
        data3.to_csv(export_path)

    return(data3)


def flow_import(rec_sites='None', gauge_sites='None', min_flow_only=False, site_ref_csv='hydstra_recorder_numbers.csv', start='1900-01-01', end='2100-01-01', min_days=365, RAW=False, export_flow=False, export_stats=False, export_shp=False, export_rec_path='all_rec_data.csv', export_gauge_path='all_gauge_data.csv', export_stats_path='all_rec_stats.csv', export_rec_shp_path='all_rec_loc.shp', export_gauge_shp_path='all_gauge_loc.shp'):
    """
    Function to import recorder and gauging data. Should be the top level import function for this data.
    """
    from pandas import read_csv, concat
    from core.ecan_io import rd_henry, rd_hydstra_db, rd_sql, rd_hydrotel, rd_site_geo
    from core.ts.sw import flow_stats, malf7d, fre_accrual
    from core.ts import  w_resample
    from core.spatial import sel_sites_poly
    from geopandas import read_file, GeoDataFrame
    from numpy import in1d
    from core.misc import select_sites
    from os.path import join, dirname, realpath
    from core.ecan_io import flow

    script_dir = dirname(flow.__file__)

    #### Additional parameters and imports
    site_dict = {100: [100, 140, 1], 140: [140, 140, 1], 143: [143, 143, 0.001]}
    site_ref = read_csv(join(script_dir, site_ref_csv))
    site_ref.columns = site_ref.columns.astype(int)

    ### Import from databases
    min_flow_sites = rd_sql('SQL2012PROD05', 'Wells', '"vMinimumFlowSites+Consent+Well"', col_names=['RefDbase', 'RefDbaseKey', 'restrictionType', 'RecordNo', 'WellNo'], where_col='RefDbase', where_val=['Gauging', 'Hydrotel'])
    min_flow_sites.columns = ['type', 'site', 'restr', 'crc', 'wap']
    min_flow_sites['site'] = min_flow_sites['site'].astype(int)
    min_flow_sites = min_flow_sites[min_flow_sites.restr == 'LowFlow']

    if export_shp or (type(rec_sites) is str) or (type(rec_sites) is GeoDataFrame):
        site_geo = rd_site_geo()

    #### prepare sites
    if (rec_sites is 'None') and (gauge_sites is 'None'):
        print('You need to define rec_sites or gauge_sites!')

    ### Recorder sites
    if rec_sites is not 'None':
        if type(rec_sites) is str:
            if rec_sites is 'All':
                r_sites = site_ref
                r_sites_sel_geo = site_geo[in1d(site_geo.site, site_ref.stack().values)]
            elif rec_sites.endswith('.shp'):
                poly = read_file(rec_sites)
                r_sites_sel_geo = sel_sites_poly(poly, site_geo)
                r_sites_sel = r_sites_sel_geo.site.values
                r_sites = site_ref.apply(lambda x: x[in1d(x, r_sites_sel)], axis=0)
        elif type(rec_sites) is GeoDataFrame:
                r_sites_sel_geo = sel_sites_poly(rec_sites, site_geo)
                r_sites_sel = r_sites_sel_geo.site.values
                r_sites = site_ref.apply(lambda x: x[in1d(x, r_sites_sel)], axis=0)
        else:
            r_sites_sel = select_sites(rec_sites)
            r_sites = site_ref.apply(lambda x: x[in1d(x, r_sites_sel)], axis=0)
        if min_flow_only:
            r_sites = r_sites.apply(lambda x: x[in1d(x, min_flow_sites.site.values)], axis=0)

        ## Import required sites
        lst = []
        for i in r_sites.dropna(axis=1, how='all').columns:
            sites_set1 = r_sites[i]
            sites_set2 = sites_set1[sites_set1.notnull()].astype(int).unique()
            if RAW:
                multiplier = site_dict[i][2]
                flow1 = rd_hydrotel(sites_set2) * multiplier
                flow = w_resample(flow1, period='day', fun='mean')
            else:
                varfrom = site_dict[i][0]
                varto = site_dict[i][1]
                multiplier = site_dict[i][2]
                flow = rd_hydstra_db(sites_set2, start_time=0, end_time=0, varfrom=varfrom, varto=varto) * multiplier
            lst.append(flow)

        r_flow1 = concat(lst, axis=1)

        # Restrain dates
        r_flow = r_flow1[r_flow1.first_valid_index():end]
        r_flow = r_flow[start:end]
        r_flow.columns = r_flow.columns.astype(int)

    ### Gauging sites
    if gauge_sites is not 'None':
        if type(gauge_sites) is str:
            if gauge_sites.endswith('.shp'):
                poly = read_file(gauge_sites)
                g_sites_sel_geo = sel_sites_poly(poly, site_geo)
                g_sites_sel = g_sites_sel_geo.site.values
                g_sites = g_sites_sel[~in1d(g_sites_sel, site_ref.stack().values)]
        elif type(gauge_sites) is GeoDataFrame:
                g_sites_sel_geo = sel_sites_poly(gauge_sites, site_geo)
                g_sites_sel = g_sites_sel_geo.site.values
                g_sites = g_sites_sel[~in1d(g_sites_sel, site_ref.stack().values)]
        else:
            g_sites = select_sites(gauge_sites)

        if min_flow_only:
            g_sites = g_sites[in1d(g_sites, min_flow_sites.site.values)]

        ## Import sites
        g_flow = rd_henry(g_sites, start=start, end=end, min_filter=5)
        g_flow['site'] = g_flow.site.astype(int)

    #### Run stats if required
    if export_stats or export_shp:
        if rec_sites is not 'None':
            ### recorder sites
            stats1 = flow_stats(r_flow)
            malf, alf, alf_mising = malf7d(r_flow)
            fre3 = fre_accrual(r_flow).round(3)
            stats2 = concat([stats1, malf, fre3], axis=1).reset_index()
        if gauge_sites is not 'None':
            ### Gauging sites
            gauge_grp = g_flow.groupby('site')['date']
            n_gauge = gauge_grp.count()
            gauge_start = gauge_grp.first().astype(str)
            gauge_end = gauge_grp.last().astype(str)
            gauge_stats = concat([gauge_start, gauge_end, n_gauge], axis=1).reset_index()
            gauge_stats.columns = ['site', 'Start Date', 'End Date', 'n gaugings']

    #### Export stats if desired
    if export_stats and (rec_sites is not 'None'):
        stats2.to_csv(export_stats_path, index=False)

    #### Export SHP if desired
    if export_shp:
        if rec_sites is not 'None':
            r_sites_sel2 = r_sites.stack().values
            r_sites_geo1 = site_geo[in1d(site_geo.site, r_sites_sel2)]
            r_sites_geo = r_sites_geo1.merge(stats2, on='site')
            r_sites_geo.to_file(export_rec_shp_path)

        if gauge_sites is not 'None':
            g_sites_geo1 = site_geo[in1d(site_geo.site, g_sites)]
            g_sites_geo = g_sites_geo1.merge(gauge_stats, on='site')
            g_sites_geo.to_file(export_gauge_shp_path)

    #### Export flow data if desired
    if export_flow:
        if rec_sites is not 'None':
            r_flow.to_csv(export_rec_path)

        if gauge_sites is not 'None':
            g_flow.to_csv(export_gauge_path)

    #### Return data
    if (rec_sites is not 'None') and (gauge_sites is not 'None'):
        return([r_flow, g_flow])
    if (rec_sites is not 'None'):
        return(r_flow)
    if (gauge_sites is not 'None'):
        return(g_flow)


def rd_nc(poly_shp, nc_path, poly_epsg=4326, poly_id='Station_ID', x_col='longitude', y_col='latitude', data_col='rain', as_ts=True, export=True, export_path='nc_data.csv'):
    """
    Function to read in netCDF files, select locations based on a polygon, and export the results.
    """
    import xarray as xr
    from core.spatial import sel_sites_poly, xy_to_gpd, pts_poly_join
    from geopandas import read_file
    from numpy import in1d
    from pandas import merge

    ### Read in all data
    poly = read_file(poly_shp)[[poly_id, 'geometry']].to_crs(epsg=poly_epsg)
    nc = xr.open_dataset(nc_path)

    ### Filter nc data
    df1 = nc.to_dataframe().drop('time_bnds', axis=1).reset_index()
    df1 = df1[df1.nb2 == 0].drop('nb2', axis=1)

    ### convert x and y to geopandas
    df1_xy = df1[[y_col, x_col]].drop_duplicates()
    df1_xy['id'] = range(len(df1_xy))
    pts = xy_to_gpd('id', x_col, y_col, df1_xy, poly_epsg)

    ### Mask the points from the polygon
    join1, poly2 = pts_poly_join(pts, poly, poly_id, dissolve=False)
    join2 = join1[['id', poly_id]]

    ### Select the associated data
    sel_xy = merge(df1_xy, join2, on='id').drop('id', axis=1)
    df2 = merge(df1, sel_xy, on=[y_col, x_col])

    ### Convert to time series
    if as_ts:
        df3 = df2[[poly_id, 'time', data_col]].groupby([poly_id, 'time']).first().reset_index()
        df4 = df3.pivot(index='time', columns=poly_id, values=data_col).round(2)
        if export:
            df4.to_csv(export_path)
    else:
        df4 = df2
        if export:
            df4.to_csv(export_path)

    return(df4)


def hydstra_site_mod_time(sites=None):
    """
    Function to extract modification times from Hydstra data archive files. Returns a DataFrame of sites by modification date. The modification date is in GMT.

    Parameters
    ----------
    sites : list, array, Series, or None
        If sites is not None, then return only the given sites.

    Returns
    -------
    DataFrame
    """
    from core.misc import rd_dir, select_sites
    from os import path
    from pandas import to_datetime, DataFrame, DateOffset

    site_files_path = r'\\fileservices02\ManagedShares\Data\Hydstra\prod\hyd\dat\hyd'
    files1 = rd_dir(site_files_path, 'A')
    file_sites = [path.splitext(i)[0] for i in files1]

    if sites is not None:
        sites1 = select_sites(sites).astype(str)
        sites2 = [i.replace('/', '_') for i in sites1]
        file_sites1 = [i for i in file_sites if i in sites2]
    else:
        file_sites1 = file_sites

    mod_times = to_datetime([round(path.getmtime(path.join(site_files_path, i + '.A'))) for i in file_sites1], unit='s')

    df = DataFrame({'site': file_sites1, 'mod_time': mod_times})
    return(df)



