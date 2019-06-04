# -*- coding: utf-8 -*-
"""
Created on Tue Jan 02 09:14:28 2018

@author: MichaelEK
Functions for loading in guaging data from the "Henry" (bgauging) system.
"""
import numpy as np
from hydropandas.util.misc import select_sites
from hydropandas.io.tools.mssql import rd_sql


def rd_henry(sites, from_date=None, to_date=None, agg_day=True, sites_by_col=False, min_filter=None, export=None):
    """
    Function to read in gaugings data from the "Henry DB". Hopefully, they keep this around for a while longer.

    Parameters
    ----------
    sites: list or str
        Either a list of site names or a file path string that contains a column of site names.
    from_date: str
        A date string for the start of the data (e.g. '2010-01-01').
    to_date: str
        A date string for the end of the data.
    agg_day: bool
        Should the gauging dates be aggregated down to the day as opposed to having the hour and minute. Gaugings are aggregated by the mean.
    sites_by_col: bool
        'False' does not make a single DateTimeIndex, rather it is indexed by site and date (long format). 'True' creates a single DateTimeIndex with the columns as gauging sites (will create many NAs).
    min_filter: int or None
        Minimum number of days required for the gaugings output.
    export: str or None
        Either a string path to a csv file or None.
    """

    def resample1(df):
        df.index = df.date
        df2 = df.resample('D').mean()
        return df2

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

    # database1 = 'Hydstra'

    # table1 = 'Hydstra.dbo.GAUGINGS'

    ########################################
    ### Read in data

    sites1 = select_sites(sites).tolist()
    data = rd_sql(server=server, database=database, table=table, col_names=fields, where_col=where_col,
                  where_val=sites1).dropna()
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
        count_index = count1[count1 >= min_filter].index
        data3 = data3[np.in1d(data3.site.values, count_index)]

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

    if isinstance(export, str):
        if sites_by_col:
            data4.to_csv(export)
        else:
            data4.to_csv(export, index=False)

    return data4

