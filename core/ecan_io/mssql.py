# -*- coding: utf-8 -*-
"""
Functions for importing mssql data.
"""


def rd_sql(server, database, table=None, col_names=None, where_col=None, where_val=None, where_op='AND', geo_col=False, from_date=None, to_date=None, date_col=None, rename_cols=None, stmt=None, export_path=None):
    """
    Function to import data from an MSSQL database. Requires the pymssql package.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    col_names : list of str
        The column names that should be retrieved. e.g.: ['SiteID', 'BandNo', 'RecordNo']
    where_col : str or dict
        Must be either a string with an associated where_val list or a dictionary of strings to lists.'. e.g.: 'SnapshotType' or {'SnapshotType': ['value1', 'value2']}
    where_val : list
        The WHERE query values for the where_col. e.g. ['value1', 'value2']
    where_op : str
        If where_col is a dictionary and there are more than one key, then the operator that connects the where statements must be either 'AND' or 'OR'.
    geo_col : bool
        Is there a geometry column in the table?.
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
    from pandas import read_sql
    from core.ecan_io.mssql import rd_sql_geo, sql_where_stmts
    from geopandas import GeoDataFrame
    from core.misc import save_df

    ## Create where statements
    if stmt is None:

        if table is None:
            raise ValueError('Must at least provide input for server, database, and table.')

        if col_names is not None:
            if isinstance(col_names, str):
                col_names = [col_names]
            col_names1 = ['[' + i.encode('ascii', 'ignore') + ']' for i in col_names]
            col_stmt = ', '.join(col_names1)
        else:
            col_stmt = '*'

        where_lst = sql_where_stmts(where_col=where_col, where_val=where_val, where_op=where_op, from_date=from_date, to_date=to_date, date_col=date_col)

        if isinstance(where_lst, list):
            stmt1 = "SELECT " + col_stmt + " FROM " + table + " where " + " and ".join(where_lst)
        else:
            stmt1 = "SELECT " + col_stmt + " FROM " + table

    elif isinstance(stmt, str):
        stmt1 = stmt

    else:
        raise ValueError('stmt must either be an SQL string or None.')

    ## Create connection to database and execute sql statement
    conn = connect(server, database=database)
    df = read_sql(stmt1, conn)
    conn.close()
    if rename_cols is not None:
        df.columns = rename_cols

    ## Read in geometry if required
    if geo_col:
        geometry, proj = rd_sql_geo(server=server, database=database, table=table, where_lst=where_lst)
        df = GeoDataFrame(df, geometry=geometry, crs=proj)

    ## save and return
    if export_path is not None:
        save_df(df, export_path, index=False)

    return(df)


def rd_sql_ts(server, database, table, groupby_cols, date_col, values_cols, resample_code, period=1, fun='mean', val_round=3, where_col=None, where_val=None, where_op='AND', from_date=None, to_date=None, export_path=None):
    """
    Function to specifically read and possibly aggregate time series data stored in MSSQL tables. Returns a MultiIndex DataFrame.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    groupby_cols : str or list of str
        The columns in the SQL table to grouped and returned with the time series data.
    date_col : str
        The date column in the table.
    values_cols : str or list of str
        The column(s) of the value(s) that should be resampled.
    resample_code : str
        The Pandas time series resampling code. e.g. 'D' for day, 'W' for week, 'M' for month, etc.
    period : int
        The number of resampling periods. e.g. period = 2 and resample = 'D' would be to resample the values over a 2 day period.
    fun : str
        The resampling function. i.e. mean, sum, count, min, or max. No median yet...
    val_round : int
        The number of decimals to round the values.
    where_col : str or dict
        Must be either a string with an associated where_val list or a dictionary of strings to lists.'. e.g.: 'SnapshotType' or {'SnapshotType': ['value1', 'value2']}
    where_val : list
        The WHERE query values for the where_col. e.g. ['value1', 'value2']
    where_op : str
        If where_col is a dictionary and there are more than one key, then the operator that connects the where statements must be either 'AND' or 'OR'.
    from_date : str
        The start date in the form '2010-01-01'.
    to_date : str
        The end date in the form '2010-01-01'.
    export_path : str
        The export path for a csv file if desired. If None, then nothing is exported.

    Returns
    -------
    DataFrame
        Pandas DataFrame with MultiIndex of groupby_cols and date_col
    """
    from core.ecan_io.mssql import sql_ts_agg_stmt, sql_where_stmts
    from pymssql import connect
    from pandas import read_sql
    from core.misc import save_df

    ## Create where statement
    where_lst = sql_where_stmts(where_col=where_col, where_val=where_val, where_op=where_op, from_date=from_date, to_date=to_date, date_col=date_col)

    ## Create ts statement and append earlier where statement
    if isinstance(resample_code, str):
        sql_stmt1 = sql_ts_agg_stmt(table, groupby_cols=groupby_cols, date_col=date_col, values_cols=values_cols, resample_code=resample_code, period=period, fun=fun, val_round=val_round, where_lst=where_lst)
    elif resample_code is None:
        if isinstance(groupby_cols, str):
            groupby_cols = [groupby_cols]
        col_names1 = ['[' + i.encode('ascii', 'ignore') + ']' for i in groupby_cols]
        col_stmt = ', '.join(col_names1)

        if isinstance(where_lst, list):
            sql_stmt1 = "SELECT " + col_stmt + " FROM " + table + " where " + " and ".join(where_lst)
        else:
            sql_stmt1 = "SELECT " + col_stmt + " FROM " + table

    ## Create connection to database and execute sql statement
    conn = connect(server, database=database)
    df = read_sql(sql_stmt1, conn)
    conn.close()

    ## set the index
    if isinstance(groupby_cols, str):
        groupby_cols = [groupby_cols]
    groupby_cols.append('time')
    df1 = df.set_index(groupby_cols)

    ## Save and return
    if export_path is not None:
        save_df(df1, export_path)

    return(df1)


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


def sql_where_stmts(where_col=None, where_val=None, where_op='AND', from_date=None, to_date=None, date_col=None):
    """
    Function to take various input parameters and convert them to a list of where statements for SQL.

    Parameters
    ----------
    where_col : str or dict
        Either a str with an associated where_val list or a dictionary of string keys to list values. If a str, it should represent the table column associated with the 'where' condition.
    where_val : list or None
        If where_col is a str, then where_val must be a list of associated condition values.
    where_op : str of either 'AND' or 'OR'
        The binding operator for the where conditions.
    from_date : str or None
        The start date in the form '2010-01-01'.
    to_date : str or None
        The end date in the form '2010-01-01'.
    date_col : str or None
        The SQL table column that contains the dates.

    Returns
    -------
    list of str or None
        Returns a list of str where conditions to be passed to an SQL execution function. The function needs to bind it with " where " + " and ".join(where_lst)
    """
    from pandas import to_datetime, Timestamp

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
            raise ValueError('where_col must be either a string with an associated where_val list or a dictionary of string keys to list values.')
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
    if len(where_lst) == 0:
        where_lst = None
    return(where_lst)


def sql_ts_agg_stmt(table, groupby_cols, date_col, values_cols, resample_code, period=1, fun='mean', val_round=3, where_lst=None):
    """
    Function to create an SQL statement to pass to an SQL driver to resample a time series table.

    Parameters
    ----------
    table : str
        The SQL table name.
    groupby_cols : str or list of str
        The columns in the SQL table to grouped and returned with the time series data.
    date_col : str
        The date column in the table.
    values_cols : str or list of str
        The column(s) of the value(s) that should be resampled.
    resample_code : str
        The Pandas time series resampling code. e.g. 'D' for day, 'W' for week, 'M' for month, etc.
    period : int
        The number of resampling periods. e.g. period = 2 and resample = 'D' would be to resample the values over a 2 day period.
    fun : str
        The resampling function. i.e. mean, sum, count, min, or max. No median yet...
    val_round : int
        The number of decimals to round the values.
    where_lst : list or None
        A list of where statements to be passed and added to the final SQL statement.

    Returns
    -------
    str
        A full SQL statement that can be passed directly to an SQL connection driver like pymssql through pandas read_sql function.
    """
    if isinstance(groupby_cols, str):
        groupby_cols = [groupby_cols]
    elif not isinstance(groupby_cols, list):
        raise TypeError('groupby must be either a str or list of str.')
    if isinstance(values_cols, str):
        values_cols = [values_cols]
    elif not isinstance(values_cols, list):
        raise TypeError('values must be either a str or list of str.')

    pandas_dict = {'D': 'day', 'W': 'week', 'H': 'hour', 'M': 'month', 'Q': 'quarter', 'T': 'minute', 'A': 'year'}
    fun_dict = {'mean': 'avg', 'sum': 'sum', 'count': 'count', 'min': 'min', 'max': 'max'}

    groupby_str = ", ".join(groupby_cols)
    values_lst = ["round(" + fun_dict[fun] + "(" + i + "), " + str(val_round) + ") AS " + i for i in values_cols]
    values_str = ", ".join(values_lst)

    if isinstance(where_lst, list):
        where_stmt = " where " + " and ".join(where_lst)
    else:
        where_stmt = ""

    stmt1 = "SELECT " + groupby_str + ", DATEADD(" + pandas_dict[resample_code] + ", DATEDIFF(" + pandas_dict[resample_code] + ", 0, " + date_col + ")/ " + str(period) + " * " + str(period) + ", 0) AS time, " + values_str + " FROM " + table + where_stmt + " GROUP BY " + groupby_str + ", DATEADD(" + pandas_dict[resample_code] + ", DATEDIFF(" + pandas_dict[resample_code] + ", 0, " + date_col + ")/ " + str(period) + " * " + str(period) + ", 0) ORDER BY " + groupby_str + ", time"

    return(stmt1)


def rd_sql_geo(server, database, table, where_lst=None):
    """
    Function to extract the geometry and coordinate system from an SQL geometry field. Returns a shapely geometry object and a proj4 str.

    Parameters
    ----------
    server : str
        The server name. e.g.: 'SQL2012PROD03'
    database : str
        The specific database within the server. e.g.: 'LowFlows'
    table : str
        The specific table within the database. e.g.: 'LowFlowSiteRestrictionDaily'
    where_lst : list
        A list of where statements to be passed and added to the final SQL statement.

    Returns
    -------
    list of shapely geometry objects
        The main output is a list of shapely geometry objects for all queried rows of the SQL table.
    str
        The second output is a proj4 str of the projection system.
    """
    from shapely.wkt import loads
    from pycrs.parser import from_epsg_code
    from pymssql import connect
    from pandas import read_sql

    ## Create connection to database
    conn = connect(server, database=database)

    geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + table + "\'" + " AND DATA_TYPE='geometry'"
    geo_col = str(read_sql(geo_col_stmt, conn).iloc[0,0])
    geo_srid_stmt = "select distinct " + geo_col + ".STSrid from " + table
    geo_srid = int(read_sql(geo_srid_stmt, conn).iloc[0,0])
    if where_lst is not None:
        if len(where_lst) > 0:
            stmt2 = "SELECT " + geo_col + ".STGeometryN(1).ToString()" + " FROM " + table + " where " + " and ".join(where_lst)
    else:
        stmt2 = "SELECT " + geo_col + ".STGeometryN(1).ToString()" + " FROM " + table
    df2 = read_sql(stmt2, conn)
    df2.columns = ['geometry']
    geo = [loads(x) for x in df2.geometry]
    proj4 = from_epsg_code(geo_srid).to_proj4()

    return(geo, proj4)


## Archive


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









