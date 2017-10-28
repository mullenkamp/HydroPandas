# -*- coding: utf-8 -*-
"""
Functions for importing mssql data.
"""


def rd_sql(server=None, database=None, table=None, col_names=None, where_col=None, where_val=None, where_op='AND', geo_col=False, epsg=2193, from_date=None, to_date=None, date_col=None, rename_cols=None, stmt=None, export_path=None):
    """
    Function to import data from an MSSQL database. Specific columns can be selected and specific queries within columns can be selected. Requires the pymssql package.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    col_names : list
        The column names that should be retrieved. e.g.: ['SiteID', 'BandNo', 'RecordNo']
    where_col : str or dict
        Must be either a string with an associated where_val list or a dictionary of strings to lists.'. e.g.: 'SnapshotType' or {'SnapshotType': ['value1', 'value2']}
    where_val : list
        The WHERE query values for the where_col. e.g. ['value1', 'value2']
    where_op : str
        If where_col is a dictionary and there are more than one key, then the operator that connects the where statements must be either 'AND' or 'OR'.
    geo_col : bool
        Is there a geometry column in the table?.
    epsg : int
        The coordinate system as EPSG code.
    from_date : str
        The start date in the form '2010-01-01'.
    to_date : str
        The end date in the form '2010-01-01'.
    date_col : str
        The SQL table column that contains the dates.
    stmt : str
        Custom SQL statement to be directly passed to the database table. This will ignore all prior arguments except server and database.
    export_path : str
        The export path for a csv file if desired. If None, then nothing is exported.

    Returns
    -------
    DataFrame
    """
    from pymssql import connect
    from pandas import read_sql, to_datetime, Timestamp

    if stmt is None:

        if (server is None) or (database is None) or (table is None):
            raise ValueError('Must provide input for server, database, and table.')

        if col_names is not None:
            if isinstance(col_names, (str, int)):
                col_names1 = '[' + str(col_names) + ']'
            col_names1 = ['[' + i.encode('ascii', 'ignore') + ']' for i in col_names]
            col_stmt = ', '.join(col_names1)
        else:
            col_stmt = '*'

        if where_col is not None:
            if isinstance(where_col, str) & isinstance(where_val, list):
                if len(where_val) > 10000:
                    raise ValueError('The number of values in where_val cannot be over 10000 (or so). MSSQL limitation. Break them into smaller chunks.')
                where_val = [str(i) for i in where_val]
                where_stmt = str(where_col) + ' IN (' + str(where_val)[1:-1] + ')'
            elif isinstance(where_col, dict):
                where_stmt1 = []
                for i in where_col:
                    if not isinstance(where_col[i], list):
                        if isinstance(where_col[i], str):
                            where1 = [where_col[i].encode('ascii', 'ignore')]
                        else:
                            where1 = [where_col[i]]
                    else:
                        where1 = [str(j) for j in where_col[i]]
                    s1 = i + ' IN (' + str(where1)[1:-1] + ')'
                    where_stmt1.append(s1)
                where_stmt = (' ' + where_op + ' ').join(where_stmt1)
            else:
                raise ValueError('where_col must be either a string with an associated where_val list or a dictionary of strings to lists.')
        else:
            where_stmt = ''

        if isinstance(from_date, str):
            from_date1 = to_datetime(from_date, errors='coerce')
            if isinstance(from_date1, Timestamp):
                from_date2 = from_date1.strftime('%Y-%m-%d')
                where_from_date = date_col + " >= " + from_date2.join(['\'', '\''])
        else:
            where_from_date = ''

        if isinstance(to_date, str):
            to_date1 = to_datetime(to_date, errors='coerce')
            if isinstance(to_date1, Timestamp):
                to_date2 = to_date1.strftime('%Y-%m-%d')
                where_to_date = date_col + " <= " + to_date2.join(['\'', '\''])
        else:
            where_to_date = ''

        where_lst = [i for i in [where_stmt, where_from_date, where_to_date] if len(i) > 0]

        if len(where_lst) > 0:
            stmt1 = "SELECT " + col_stmt + " FROM " + table + " where " + " and ".join(where_lst)
        else:
            stmt1 = "SELECT " + col_stmt + " FROM " + table

    elif isinstance(stmt, str):
        stmt1 = stmt

    else:
        raise ValueError('stmt must either be an SQL string or None.')

    conn = connect(server, database=database)
    df = read_sql(stmt1, conn)
    if rename_cols is not None:
        df.columns = rename_cols

    ## Read in geometry if required
    if geo_col:
        from shapely.wkt import loads
        from geopandas import GeoDataFrame
        from pycrs.parser import from_epsg_code

        geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + table + "\'" + " AND DATA_TYPE='geometry'"
        geo_col = str(read_sql(geo_col_stmt, conn).iloc[0,0])
        if len(where_lst) > 0:
            stmt2 = "SELECT " + geo_col + ".STGeometryN(1).ToString()" + " FROM " + table + " where " + " and ".join(where_lst)
        else:
            stmt2 = "SELECT " + geo_col + ".STGeometryN(1).ToString()" + " FROM " + table
        df2 = read_sql(stmt2, conn)
        df2.columns = ['geometry']
        geometry = [loads(x) for x in df2.geometry]
        df = GeoDataFrame(df, geometry=geometry, crs=from_epsg_code(epsg).to_proj4())

    conn.close()
    if export_path is not None:
        df.to_csv(export_path, index=False)

    return(df)


#def rd_site_geo():
#    """
#    Convenience function to read in all flow sites shapefile and reformat.
#    """
#    from core.ecan_io import rd_sql
#    site_geo = rd_sql('SQL2012PROD05', 'GIS', 'vGAUGING_NZTM', col_names=['SiteNumber', 'RIVER', 'SITENAME'], geo_col=True)
#    site_geo.columns = ['site', 'river', 'site_name', 'geometry']
#    site_geo['river'] = site_geo.river.apply(lambda x: x.title())
#    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.title())
#    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace(' (Recorder)', ''))
#    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Sh', 'SH'))
#    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Ecs', 'ECS'))
#    return(site_geo)


def rd_sw_rain_geo(sites=None):
    from core.spatial import xy_to_gpd
    from pandas import to_numeric

    if sites is not None:
        site_geo = rd_sql('SQL2012PROD05', 'Bgauging', 'RSITES', col_names=['SiteNumber', 'River', 'SiteName', 'NZTMX', 'NZTMY'], where_col='SiteNumber', where_val=sites)

    site_geo.columns = ['site', 'river', 'name', 'NZTMX', 'NZTMY']
    site_geo.loc[:, 'site'] = to_numeric(site_geo.loc[:, 'site'], errors='ignore')

    site_geo2 = xy_to_gpd(df=site_geo, id_col=['site', 'river', 'name'], x_col='NZTMX', y_col='NZTMY')
    site_geo3 = site_geo2.loc[site_geo2.site > 0, :]
    site_geo3.loc[:, 'site'] = site_geo3.loc[:, 'site'].astype('int32')
    site_geo3['river'] = site_geo3.river.apply(lambda x: x.title())
    site_geo3['name'] = site_geo3.name.apply(lambda x: x.title())
    site_geo3['name'] = site_geo3.name.apply(lambda x: x.replace(' (Recorder)', ''))
    site_geo3['name'] = site_geo3.name.apply(lambda x: x.replace('Sh', 'SH'))
    site_geo3['name'] = site_geo3.name.apply(lambda x: x.replace('Ecs', 'ECS'))
    return(site_geo3.set_index('site'))


def rd_squalarc(sites, mtypes=None, from_date=None, to_date=None, convert_dtl=False, dtl_method=None, export_path=None):
    """
    Function to read in "squalarc" data. Which is atually stored in the mssql db.

    Arguments:\n
    sites -- The site names as a list, array, csv with the first column as the site names, or a polygon shapefile of the area of interest.\n
    mtypes -- A list of measurement type names to be in the output. Leaving it empty returns all mtypes.\n
    from_date -- A start date string in of '2010-01-01'.\n
    to_date -- A end date string in of '2011-01-01'.\n
    convert_dtl -- Should values under the detection limit be converted to numeric?\n
    dtl_method -- The method to use to convert values under a detection limit to numeric. None or 'standard' takes half of the detection limit. 'trend' is meant as an output for trend analysis with includes an additional column dtl_ratio referring to the ratio of values under the detection limit.
    """
    from core.ecan_io import rd_sql
    from core.misc import select_sites
    from geopandas import GeoDataFrame
    from core.spatial import xy_to_gpd, sel_sites_poly
    from pandas import Series, to_datetime, to_numeric, Timestamp, concat, merge, DataFrame
    from numpy import nan, ceil

    #### Read in sites
    sites1 = select_sites(sites)

    #### Extract by polygon
    if isinstance(sites1, GeoDataFrame):
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
        sites2 = Series(sites1, name='site').astype(str).tolist()

    #### Extract the rest of the data
    if len(sites2) > 10000:
        n_chunks = int(ceil(len(sites2) * 0.0001))
        sites3 = [sites2[i::n_chunks] for i in xrange(n_chunks)]
        samples_tab = DataFrame()
        for i in sites3:
            samples_tab1 = rd_sql('SQL2012PROD05', 'Squalarc', '"SQL_SAMPLE_METHODS+"', col_names=['Site_ID', 'SAMPLE_NO', 'ME_TYP', 'Collect_Date', 'Collect_Time', 'PA_NAME', 'PARAM_UNITS', 'SRESULT'], where_col='Site_ID', where_val=i)
            samples_tab1.columns = ['site', 'sample_id', 'source', 'date', 'time', 'parameter', 'units', 'val']
            samples_tab1.loc[:, 'source'] = samples_tab1.loc[:, 'source'].str.lower()
            samples_tab = concat([samples_tab, samples_tab1])
    else:
        samples_tab = rd_sql('SQL2012PROD05', 'Squalarc', '"SQL_SAMPLE_METHODS+"', col_names=['Site_ID', 'SAMPLE_NO', 'ME_TYP', 'Collect_Date', 'Collect_Time', 'PA_NAME', 'PARAM_UNITS', 'SRESULT'], where_col='Site_ID', where_val=sites2)
        samples_tab.columns = ['site', 'sample_id', 'source', 'date', 'time', 'parameter', 'units', 'val']
        samples_tab.loc[:, 'source'] = samples_tab.loc[:, 'source'].str.lower()

    samples_tab2 = samples_tab.copy()
    num_test = to_numeric(samples_tab2.loc[:, 'time'], 'coerce')
    samples_tab2.loc[num_test.isnull(), 'time'] = '0000'
    samples_tab2.loc[:, 'time'] = samples_tab2.loc[:, 'time'].str.replace('.', '')
    samples_tab2 = samples_tab2[samples_tab2.date.notnull()]
#    samples_tab2.loc[:, 'time'] = samples_tab2.loc[:, 'time'].str.replace('9999', '0000')
    time1 = to_datetime(samples_tab2.time, format='%H%M', errors='coerce')
    time1[time1.isnull()] = Timestamp('2000-01-01 00:00:00')
    datetime1 = to_datetime(samples_tab2.date.dt.date.astype(str) + ' ' + time1.dt.time.astype(str))
    samples_tab2.loc[:, 'date'] = datetime1
    samples_tab2 = samples_tab2.drop('time', axis=1)
    samples_tab2.loc[samples_tab2.val.isnull(), 'val'] = nan
    samples_tab2.loc[samples_tab2.val == 'N/A', 'val'] = nan

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
            data2.loc[less1, 'val'] = to_numeric(data.loc[less1, 'val'].str.replace('<', ''), errors='coerce') * 0.5
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
                combo1 = concat([count1, count_dtl, count_dtl_val], axis=1, join='inner')
                combo1['dtl_ratio'] = (combo1['dtl_count'] / combo1['tot_count']).round(2)

                ## conditionals
    #            param1 = combo1[(combo1['dtl_ratio'] <= 0.4) | (combo1['dtl_ratio'] == 1)]
    #            under_40 = data['parameter'].isin(param1.index)
                param2 = combo1[(combo1['dtl_ratio'] > 0.4) & (combo1['dtl_val_count'] != 1)]
                over_40 = data['parameter'].isin(param2.index)

                ## Calc detection limit values
                data3 = merge(data, combo1['dtl_ratio'].reset_index(), on='parameter', how='left')
                data3.loc[:, 'val_dtl'] = data2['val']

                max_dtl_val = data2[over_40 & less1].groupby('parameter')['val'].transform('max')
                max_dtl_val.name = 'dtl_val_max'
                data3.loc[over_40 & less1, 'val_dtl'] = max_dtl_val
        else:
            data3 = data
    else:
        data3 = data

    #### Return and export
    if isinstance(export_path, str):
        data3.to_csv(export_path, encoding='utf-8', index=False)
    return(data3)


def write_sql(server, database, table, df, dtype_dict, create_table=True, drop_table=False):
    """
    Function to write pandas dataframes to mssql server tables. Must have write permissions to database!

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    df : DataFrame
        DataFrame to be saved.
    dtype_dict : dict of str
        Dictionary of df columns to the associated sql data type. Examples below.
    create_table : bool
        Should a new table be created or should it be appended to an existing table?
    drop_table : bool
        If the table already exists, should it be dropped?

    Returns
    -------
    None

    dtype strings for matching python data types to SQL
    ---------------------------------------------------
    str: 'VARCHAR(19)'

    date: 'DATE'

    datetime: "DATETIME'

    float: 'NUMERIC(10, 1)' or 'FLOAT'

    int: 'INT'
    """
    from pandas import to_datetime
    from pymssql import connect

    #### Parameters and functions
    py_sql = {'NUMERIC': float, 'DATE': str, 'DATETIME': str, 'INT': 'int32', 'VARCHAR': str, 'FLOAT': float}

    def chunker(seq, size):
        return([seq[pos:pos + size] for pos in xrange(0, len(seq), size)])

    #### Make sure the df has the correct dtypes
    if len(dtype_dict) != len(df.columns):
        raise ValueError('dtype_dict must have the same number of keys as columns in df.')
    if not all(df.columns.isin(dtype_dict.keys())):
        raise ValueError('dtype_dict must have the same column names as the columns in the df.')

    for i in df.columns:
        dtype1 = dtype_dict[i]
        if (dtype1 == 'DATE') | (dtype1 == 'DATETIME'):
            time1 = to_datetime(df[i]).astype(str)
            df.loc[:, i] = time1
        elif 'VARCHAR' in dtype1:
            df.loc[:, i] = df.loc[:, i].astype(str)
        elif 'NUMERIC' in dtype1:
            df.loc[:, i] = df.loc[:, i].astype(float)
        elif not dtype1 in py_sql.keys():
            raise ValueError('dtype must be one of ' + str(py_sql.keys()))
        else:
            df.loc[:, i] = df.loc[:, i].astype(py_sql[dtype1])

    #### Convert df to set of tuples to be ingested by sql
    list1 = df.values.tolist()
    tup1 = [str(tuple(i)) for i in list1]
    tup2 = chunker(tup1, 1000)

    #### Initial create table and insert statements
    d1 = [str(i) + ' ' + dtype_dict[i] for i in df.columns]
    d2 = ', '.join(d1)
    tab_create_stmt = "create table " + table + " (" + d2 + ")"
    insert_stmt1 = "insert into " + table + " values "

    conn = connect(server, database=database)
    cursor = conn.cursor()

    #### Drop table if it exists
    if drop_table:
        drop_stmt = "IF OBJECT_ID(" + str([str(table)])[1:-1] + ", 'U') IS NOT NULL DROP TABLE " + table
        cursor.execute(drop_stmt)
        conn.commit()

    #### Create table in database
    if create_table:
        cursor.execute(tab_create_stmt)
        conn.commit()

    #### Insert data into table
    for i in tup2:
        rows = ','.join(i)
        insert_stmt2 = insert_stmt1 + rows
        cursor.execute(insert_stmt2)
    conn.commit()

    #### Close everything!
    cursor.close()
    conn.close()


















