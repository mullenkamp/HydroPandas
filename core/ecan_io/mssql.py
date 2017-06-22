# -*- coding: utf-8 -*-
"""
Functions for importing mssql data.
"""


def rd_sql(server=None, database=None, table=None, col_names=None, where_col=None, where_val=None, where_op='AND', code=None, geo_col=False, epsg=2193, data_source_csv='ecan_mssql_data_sources.csv', stmt=None, export=False, path='save.csv'):
    """
    Function to import data from a MSSQL database. Specific columns can be selected and specific queries within columns can be selected. Requires the pymssql package, which must be separately installed.

    Arguments:\n
    server -- The server name (str). e.g.: 'SQL2012PROD03'\n
    database -- The specific database within the server (str). e.g.: 'LowFlows'\n
    table -- The specific table within the database (str). e.g.: 'LowFlowSiteRestrictionDaily'\n
    code -- Use predefined codes from data sources csv to assign database parameters. Will overwrite server, database, table, and geo_col parameters.\n
    col_names -- The column names that should be retrieved (list). e.g.: ['SiteID', 'BandNo', 'RecordNo']\n
    where_col -- Must be either a string with an associated where_val list or a dictionary of strings to lists.'. e.g.: 'SnapshotType' or {'SnapshotType': ['value1', 'value2']}\n
    where_val -- The WHERE query values for the where_col (list). e.g. ['value1', 'value2']\n
    where_op -- If where_col is a dictionary and there are more than one key, then the operator that connects the where statements must be either 'AND' or 'OR'.\n
    geo_col -- Is there a geometry column in the table?.\n
    epsg -- The coordinate system (int).\n
    stmt -- Custom SQL statement to be directly passed to the database table. This will ignore all prior arguments except server and database.\n
    export -- Should the data be exported?\n
    path -- The path and csv name for the export if 'export' is True (str).
    """
    from pymssql import connect
    from pandas import read_sql, read_csv
    from os.path import join, dirname, realpath
    from core.ecan_io import mssql

    script_dir = dirname(mssql.__file__)

    if stmt is None:

        if code is not None:
            dbs = read_csv(join(script_dir, data_source_csv))
            dbs_code = dbs[dbs.Code == code]
            server = dbs_code.Server.values[0]
            database = dbs_code.Database.values[0]
            table = dbs_code.Table.values[0]
            col_names = dbs_code.db_fields.values[0].split(', ')
            rename_cols = dbs_code.rename_fields.values[0].split(', ')
            geo_col = dbs_code.geo_col.values[0]
            if dbs_code.where_col.values[0] != 'None':
                where_col = dbs_code.where_col.values[0]
                where_val = dbs_code.where_val.values[0].split(', ')

        if col_names is not None:
            if not isinstance(col_names, list):
                col_names = [col_names]
            col_stmt = str(col_names).replace('\'', '"')[1:-1]
        else:
            col_stmt = '*'

        if where_col is not None:
            if isinstance(where_col, str) & isinstance(where_val, list):
                where_stmt = ' WHERE ' + str([where_col]).replace('\'', '"')[1:-1] + ' IN (' + str(where_val)[1:-1] + ')'
            elif isinstance(where_col, dict):
                where_stmt1 = []
                for i in where_col:
                    if not isinstance(i, list):
                        col1 = [i]
                    else:
                        col1 = i
                    if not isinstance(where_col[i], list):
                        where1 = [where_col[i]]
                    else:
                        where1 = where_col[i]
                    s1 = str(col1).replace('\'', '"')[1:-1] + ' IN (' + str(where1)[1:-1] + ')'
                    where_stmt1.append(s1)
                where_stmt = ' WHERE ' + (' ' + where_op + ' ').join(where_stmt1)
            else:
                raise ValueError('where_col must be either a string with an associated where_val list or a dictionary of strings to lists.')
        else:
            where_stmt = ''

        stmt1 = 'SELECT ' + col_stmt + ' FROM ' + table + where_stmt

    elif isinstance(stmt, str):
        stmt1 = stmt

    else:
        raise ValueError('stmt must either be an SQL string or None.')

    conn = connect(server, database=database)
    df = read_sql(stmt1, conn)
    if code is not None:
        df.columns = rename_cols

    ## Read in geometry if required
    if geo_col:
        from shapely.wkt import loads
        from geopandas import GeoDataFrame
        from pycrs.parser import from_epsg_code

        geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + table + "\'" + " AND DATA_TYPE='geometry'"
        geo_col = str(read_sql(geo_col_stmt, conn).iloc[0,0])
        if where_col is None:
            stmt2 = 'SELECT ' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + table
        else:
            stmt2 = 'SELECT ' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + table + ' WHERE ' + str([where_col]).replace('\'', '"')[1:-1] + ' IN (' + str(where_val)[1:-1] + ')'
        df2 = read_sql(stmt2, conn)
        df2.columns = ['geometry']
        geometry = [loads(x) for x in df2.geometry]
        df = GeoDataFrame(df, geometry=geometry, crs=from_epsg_code(epsg).to_proj4())

    conn.close()
    if export:
        df.to_csv(path, index=False)

    return(df)


def rd_site_geo():
    """
    Convenience function to read in all flow sites shapefile and reformat.
    """
    from core.ecan_io import rd_sql
    site_geo = rd_sql('SQL2012PROD05', 'GIS', 'vGAUGING_NZTM', col_names=['SiteNumber', 'RIVER', 'SITENAME'], geo_col=True)
    site_geo.columns = ['site', 'river', 'site_name', 'geometry']
    site_geo['river'] = site_geo.river.apply(lambda x: x.title())
    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.title())
    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace(' (Recorder)', ''))
    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Sh', 'SH'))
    site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Ecs', 'ECS'))
    return(site_geo)


def rd_squalarc(sites, mtypes=None, from_date=None, to_date=None, convert_dtl=True, dtl_method=None, export=False, export_path='WQ_data.csv'):
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
    from pandas import Series, to_datetime, to_numeric, Timestamp, concat, merge
    from numpy import nan

    #### Read in sites
    sites1 = select_sites(sites)

    #### Extract by polygon
    if isinstance(sites1, GeoDataFrame):
        ## Surface water sites
        sw_sites_tab = rd_sql('SQL2012PROD05', 'Squalarc', 'SITES', col_names=['SITE_ID', 'NZTMX', 'NZTMY'])
        sw_sites_tab.columns = ['site', 'NZTMX', 'NZTMY']
        gdf_sw_sites = xy_to_gpd('site', 'NZTMX', 'NZTMY', sw_sites_tab)
        sw_sites2 = sel_sites_poly(gdf_sw_sites, sites1).drop('geometry', axis=1)

        ## Groundwater sites
        gw_sites_tab = rd_sql('SQL2012PROD05', 'GIS', 'vWELLS_NZTM_BASIC', col_names=['WELL_NO'], geo_col=True)
        gw_sites_tab.columns = ['site', 'geometry']
        gw_sites2 = sel_sites_poly(gw_sites_tab, sites1).drop('geometry', axis=1)

        sites2 = sw_sites2.site.append(gw_sites2.site)
    else:
        sites2 = Series(sites1, name='site')

    #### Extract the rest of the data
    samples_tab = rd_sql('SQL2012PROD05', 'Squalarc', '"SQL_SAMPLE_METHODS+"', col_names=['Site_ID', 'SAMPLE_NO', 'ME_TYP', 'Collect_Date', 'Collect_Time', 'PA_NAME', 'PARAM_UNITS', 'SRESULT'], where_col='Site_ID', where_val=sites2.astype(str).tolist())
    samples_tab.columns = ['site', 'sample_id', 'source', 'date', 'time', 'parameter', 'units', 'val']
    samples_tab.loc[:, 'source'] = samples_tab.loc[:, 'source'].str.lower()

    num_test = to_numeric(samples_tab.loc[:, 'time'], 'coerce')
    samples_tab.loc[num_test.isnull(), 'time'] = '0000'
    samples_tab.loc[:, 'time'] = samples_tab.loc[:, 'time'].str.replace('.', '')
#    samples_tab.loc[:, 'time'] = samples_tab.loc[:, 'time'].str.replace('9999', '0000')
    time1 = to_datetime(samples_tab.time, format='%H%M', errors='coerce')
    time1[time1.isnull()] = Timestamp('2000-01-01 00:00:00')
    datetime1 = to_datetime(samples_tab.date.dt.date.astype(str) + ' ' + time1.dt.time.astype(str))
    samples_tab.loc[:, 'date'] = datetime1
    samples_tab = samples_tab.drop('time', axis=1)
    samples_tab.loc[samples_tab.val.isnull(), 'val'] = nan
    samples_tab.loc[samples_tab.val == 'N/A', 'val'] = nan

    #### Select within time range
    if isinstance(from_date, str):
        samples_tab = samples_tab[samples_tab['date'] >= from_date]
    if isinstance(to_date, str):
        samples_tab = samples_tab[samples_tab['date'] <= to_date]

    if mtypes is not None:
        mtypes1 = select_sites(mtypes)
        data = samples_tab[samples_tab.parameter.isin(mtypes1)].reset_index(drop=True)
    else:
        data = samples_tab.reset_index(drop=True)

    #### Convert detection limit values
    if convert_dtl:
        less1 = data['val'].str.match('<')
        less1.loc[less1.isnull()] = False
        data2 = data.copy()
        data2.loc[less1, 'val'] = to_numeric(data.loc[less1, 'val'].str.replace('<', '')) * 0.5
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

    #### Return and export
    if export:
        data3.to_csv(export_path, index=False)
    return(data3)


def write_sql(server, database, table, df, dtype_dict, create_table=True, drop_table=False):
    """
    Function to write pandas dataframes to mssql server tables. Must have write permissions!

    Arguments:\n
    server -- The server name (str). e.g.: 'SQL2012PROD03'\n
    database -- The specific database within the server (str). e.g.: 'LowFlows'\n
    table -- The specific table within the database to be written to (str). e.g.: 'flow_data'\n
    df -- Pandas DataFrame.\n
    dtype_dict -- Dictionary of df columns to the associated sql data type (as a string).\n
    create_table -- Should a new table be created or should it be appended to an existing table?.\n
    drop_table -- If the table already exists, should it be dropped?
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
        drop_stmt = "IF OBJECT_ID(" + str([table])[1:-1] + ", 'U') IS NOT NULL DROP TABLE " + table
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


















