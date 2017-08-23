# -*- coding: utf-8 -*-
"""
Hilltop read functions.
Hilltop uses a fixed base date as 1940-01-01, while the standard unix/POSIT base date is 1970-01-01.
"""


def parse_dsn(dsn_path):
    """
    Function to parse a dsn file and all sub-dsn files into paths to hts files. Returns a list of hts paths.
    """
    from ConfigParser import ConfigParser
    from os import path

    base_path = path.dirname(dsn_path)

    dsn = ConfigParser()
    dsn.read(dsn_path)
    files1 = [path.join(base_path, i[1]) for i in dsn.items('Hilltop') if 'file' in i[0]]
    hts1 = [i for i in files1 if i.endswith('.hts')]
    dsn1 = [i for i in files1 if i.endswith('.dsn')]
    while dsn1:
        for f in dsn1:
            base_path = path.dirname(f)
            p1 = ConfigParser()
            p1.read(f)
            files1 = [path.join(base_path, i[1]) for i in p1.items('Hilltop') if 'file' in i[0]]
            hts1.extend([i for i in files1 if i.endswith('.hts')])
            dsn1.remove(f)
            dsn1[0:0] = [i for i in files1 if i.endswith('.dsn')]
    return(hts1)


def rd_hilltop_sites(hts):
    """
    Function to read the site names, measurement types, and units of a Hilltop hts file. Returns a DataFrame.

    Arguments:\n
    hts -- Path to the hts file (str).
    """
    from win32com.client import Dispatch
    from pandas import DataFrame, to_datetime

    cat = Dispatch("Hilltop.Catalogue")
    if not cat.Open(hts):
        raise ValueError(cat.errmsg)

    dfile = Dispatch("Hilltop.DataRetrieval")
    try:
        dfile.Open(hts)
    except ValueError:
        print(dfile.errmsg)

    sites = []

    cat.StartSiteEnum
    iter2 = cat.GetNextSite
    while iter2:
        name1 = cat.SiteName
        cat.GetNextDataSource
        mtype1 = cat.DataSource
        cat.GetNextMeasurement
        unit1 = cat.Units

        try:
            start1 = to_datetime(cat.DataStartTime.Format('%Y-%m-%d %H:%M'))
            end1 = to_datetime(cat.DataEndTime.Format('%Y-%m-%d %H:%M'))
        except ValueError:
            bool_site = dfile.FromSite(name1, mtype1, 1)
            if bool_site:
                start1 = to_datetime(dfile.DataStartTime.Format('%Y-%m-%d %H:%M'))
                end1 = to_datetime(dfile.DataEndTime.Format('%Y-%m-%d %H:%M'))
            else:
                print('No site data for ' + name1 + '...for some reason...')

        if unit1 == '%':
            print('Site ' + name1 + ' has no units')
            unit1 = ''
        iter2 = cat.GetNextSite
        sites.append([name1, mtype1, unit1, start1.strftime('%Y-%m-%d %H:%M'), end1.strftime('%Y-%m-%d %H:%M')])

    sites_df = DataFrame(sites, columns=['site', 'mtype', 'unit', 'start_date', 'end_date'])
    dfile.Close()
    cat.Close()
    return(sites_df)


def rd_hilltop_data(hts, sites=None, mtypes=None, start=None, end=None, agg_period=None, agg_n=1, fun=None):
    """
    Function to read data from an hts file and optionally select specific sites and aggregate the data.

    Arguments:\n
    hts -- Path to the hts file (str).\n
    sites -- A list of site names within the hts file.\n
    mtypes -- A list of measurement types that should be returned.\n
    start -- The start date to retreive from the data in ISO format (e.g. '2011-11-30 00:00').\n
    end -- The end date to retreive from the data in ISO format (e.g. '2011-11-30 00:00').\n
    agg_period -- The resample period (e.g. 'day', 'month').\n
    agg_n -- The number of periods (e.g. 1 for 1 day).\n
    fun --  The resampling function.
    """
    from core.ecan_io.hilltop import rd_hilltop_sites
    from win32com.client import Dispatch
    from pandas import DataFrame, to_datetime, Series, concat
    from core.misc import time_switch

    agg_name_dict = {'sum': 4, 'count': 5, 'mean': 1}
    agg_unit_dict = {'l/s': 1, 'm3/s': 1, 'm3/hour': 1, 'mm': 1, 'm3': 4}
    unit_convert = {'l/s': 0.001, 'm3/s': 1, 'm3/hour': 1, 'mm': 1, 'm3': 4}

    ### First read all of the sites in the hts file and select the ones to be read
    sites_df = rd_hilltop_sites(hts)

    if sites is not None:
        sites_df = sites_df[sites_df.site.isin(sites)]
    if mtypes is not None:
        sites_df = sites_df[sites_df.mtype.isin(mtypes)]

    sites_df.set_index('site', inplace=True)

    ### Open the hts file
    dfile = Dispatch("Hilltop.DataRetrieval")
    try:
        dfile.Open(hts)
    except ValueError:
        print(dfile.errmsg)

    ### Iterate through he hts file
    df_lst = []
    for i in sites_df.index:
        mtype = sites_df.loc[i, 'mtype']
        unit = sites_df.loc[i, 'unit']
        if mtype == 'Flow':
            mtype = 'Flow [Flow]'
        if fun is None:
            agg_val = agg_unit_dict[unit]
        else:
            agg_val = agg_name_dict[fun]
        dfile.FromSite(i, mtype, 1)

        ## Set up start and end times and aggregation initiation
        if (start is None):
            if (agg_period is not None):
                start1 = to_datetime(dfile.DataStartTime.Format('%Y-%m-%d %H:%M')).ceil(str(agg_n) + time_switch(agg_period))
            else:
                start1 = dfile.DataStartTime
        else:
            start1 = start
        if end is None:
            end1 = dfile.DataEndTime
        else:
            end1 = end
        if (agg_period is not None):
            dfile.FromTimeRange(start1, end1)
            dfile.SetMode(agg_val, str(agg_n) + ' ' + agg_period)
        else:
            dfile.FromTimeRange(start1, end1)

        ## Extract data
        iter1 = dfile.getsinglevbs
        data = []
        time = []
        while iter1 == 0:
            data.append(dfile.value)
            time.append(dfile.time.Format('%Y-%m-%d %H:%M:%S'))
            iter1 = dfile.getsinglevbs
        if data:
            if isinstance(data[0], (str, unicode)):
                print('site ' + i + ' has nonsense data')
            else:
                df_temp = DataFrame({'time': time, 'data': data})
                df_temp['site'] = i
                df_temp['mtype'] = sites_df.loc[i, 'mtype']
                df_lst.append(df_temp)

    dfile.Close()
    if df_lst:
        df1 = concat(df_lst)
        df1.loc[:, 'time'] = to_datetime(df1.loc[:, 'time'])
        df2 = df1.set_index(['mtype', 'site', 'time']).data * unit_convert[unit]
        return(df2)
    else:
        return(DataFrame())


def rd_ht_xml_sites(xml):
    """
    Function to read a Hilltop xml file and return the site names. The xml file should be a complete export of an hts file.
    """
    from lxml import objectify, etree
    from pandas import to_datetime, DataFrame, merge

    ### Parse xml
    root = etree.iterparse(xml, tag='Measurement')

    ### Iterate
    sites = []
    mtypes = []
    for event, elem in root:
        sites.append(elem.values()[0])
        ds = elem.find('DataSource')
        mtypes.append(ds.values()[0])
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    ### Return
    df = DataFrame([sites, mtypes]).transpose()
    df.columns = ['site', 'mtype']
    return(df)


def parse_ht_xml(xml, ht_fun, select=None, corr_csv=r'C:\ecan\hilltop\ht_corrections.csv'):
    """
    Function to read a Hilltop xml file and apply a function on each individual site time series. The input to the function is a single pandas time series. The output should be a Series or DataFrame. Specific sites with specific mtypes can be passed in the form of a two column DataFrame with headers as 'site' and 'mtype'.
    """
    from lxml import objectify, etree
    from pandas import to_datetime, DataFrame, concat, read_csv
    from os.path import basename

    ### Base parameters
    rem_s = 10958*24*60*60
    corr = read_csv(corr_csv)
    xml_name = basename(xml)

    ### Select corrections
    corr1 = corr[corr.file_name == xml_name]

    ### Parse xml
    root = etree.iterparse(xml, tag='Measurement')

    ### Iterate
    results1 = []
    for event, elem in root:
        ## Get data
        site = elem.values()[0]
        mtype = elem.find('DataSource').values()[0]

        if (select is not None):
            if (not isinstance(select, DataFrame)):
                raise ValueError('Make sure the input is a DataFrame with two columns!')
            elif all(select.columns == ['site', 'mtype']):
                site_check = any([set([site, mtype]) == set([select.loc[i].site, select.loc[i].mtype]) for i in select.index])
                if not site_check:
                    continue

#        units = elem.find('DataSource').find('ItemInfo').find('Units').text
        site_data = [j.text.split() for j in elem.find('Data').findall('V')]

        ## Convert to dataframe
        o2 = DataFrame(site_data, columns=['date', 'val'])
        o2.loc[:,['date', 'val']] = o2.loc[:,['date', 'val']].astype(float)
        o2.loc[:, 'date'] = to_datetime(o2.loc[:, 'date'] - rem_s, unit='s')
        o2.set_index('date', inplace=True)

        ## Make corrections
        corr_index = (corr1.orig_site == site) & (corr1.orig_mtype == mtype)
        if any(corr_index):
            site, mtype = corr1.loc[corr_index, ['new_site', 'new_mtype']].values.tolist()[0]

        ## Clear element from memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

        ## Do stats
        stats1 = ht_fun(o2, mtype, site)

        ## Add additional site specific columns/data
#        stats1.loc[:, 'site'] = site
#        stats1.loc[:, 'mtype'] = mtype
#        stats1.loc[:, 'units'] = units

        ## Append
        results1.append(stats1)

    ### Combine data
    df_out = concat(results1)

    ### Return
    return(df_out)


def data_check_fun(data, mtype, site):
    """
    Various data checks on the hilltop data. This function should be an input to parse_ht_xml.
    """
    from pandas import infer_freq, concat, DataFrame

    def infer_freq1(x):
        if len(x) > 7:
            freq1 = infer_freq(x.index[3:6], warn=False)
        else:
            freq1 = None
        return(freq1)

    ## Run stat checks
    first1 = data.index.min()
    last1 = data.index.max()
    n_neg = (data < 0).sum()[0]
    n_zero = (data == 0).sum()[0]
    count1 = data.count()[0]
    freq = infer_freq1(data)
    neg_ratio = round(n_neg/float(count1), 2)
    zero_ratio = round(n_zero/float(count1), 2)

    ## Construct output
    out1 = [first1, last1, freq, count1, n_neg, n_zero, neg_ratio, zero_ratio]
    out_names = ['start_date', 'end_date', 'time_res', 'n_data', 'n_neg', 'n_zero', 'n_neg/n_data', 'n_zero/n_data']
    df_out = DataFrame([out1], columns=out_names)
    df_out.loc[:, 'site'] = site
    df_out.loc[:, 'mtype'] = mtype

    ### return
    return(df_out)


def iter_xml_dir(fpath, stats_fun, with_xml=False, select=None, export=False, export_name='results.csv'):
    from core.ecan_io.hilltop import parse_ht_xml
    from core.misc import rd_dir
    from os.path import join
    from pandas import concat, DataFrame

    ### Read files in directory
    files = rd_dir(fpath, 'xml')

    ### Iterate through each file
    list_out = []
    for i in files:
        print(i)
        if select is None:
            out1 = parse_ht_xml(join(fpath, i), stats_fun)
        elif isinstance(select, DataFrame) & (len(select.columns) == 3):
            select1 = select.loc[select.file_name == i, ['site', 'mtype']]
            out1 = parse_ht_xml(join(fpath, i), stats_fun, select1)
        if with_xml:
            out1['xml'] = [i] * len(out1)
        list_out.append(out1)

    ### Combine
    df_out = concat(list_out)
    df_out.index.name = 'date'

    ### Export and return
    if export:
        df_out.to_csv(join(fpath, export_name), encoding='utf-8')
    return(df_out)


def all_data_fun(data, mtype, site):
    """
    Function for parse_ht_xml to return all data.
    """
    data.loc[:, 'site'] = site
    data.loc[:, 'mtype'] = mtype
    return(data)


def proc_use_data(data, mtype, site, time_period='D', n_std=4):
    """
    Function for parse_ht_xml to process the data and aggregate it to a defined resolution.
    """
    from numpy import nan, abs
    from pandas import Series

    ### Select the process sequence based on the mtype and convert to period volume
    data[data < 0] = nan
    count1 = float(data.count().values[0])

    if mtype == 'Water Meter':
        ## Check to determine whether it is cumulative or period volume
        diff1 = data.diff()[1:]
        neg_index = diff1 < 0
        neg_ratio = sum(neg_index.values)/count1
        if neg_ratio > 0.1:
            outliers = abs(data - data.mean()) > (data.std() * n_std)
            data[outliers] = nan
            vol = data
        else:
            # Replace the negative values with zero and the very large values
            diff1[diff1 < 0] = data[diff1 < 0]
            outliers = abs(diff1 - diff1.mean()) > (diff1.std() * n_std)
            diff1[outliers] = nan
            vol = diff1
    elif (mtype == 'Abstraction Volume') | (mtype == 'Average Flow'):
        outliers = abs(data - data.mean()) > (data.std() * n_std)
        data[outliers] = nan
        vol = data
    elif mtype == 'Flow':
        outliers = abs(data - data.mean()) > (data.std() * n_std)
        data[outliers] = nan

        # Determine the diff index
        t1 = Series(data.index).diff().dt.seconds.shift(-1)
        t1.iloc[-1] = t1.iloc[-2]
        t1.index = data.index
        # Convert to volume
        vol = data.multiply(t1, axis=0) * 0.001

    ## Estimate the NAs
    vol2 = vol.fillna(method='ffill')

    ## Resample the volumes
    vol_res = vol2.resample(time_period).sum()
    vol_res.loc[:, 'site'] = site

    return(vol_res)


def convert_site_names(names):
    from numpy import nan

    names1 = names.str.replace('[:\.]', '/')
#    names1.loc[names1 == 'L35183/580-M1'] = 'L35/183/580-M1' What to do with this one?
    names1.loc[names1 == 'L370557-M1'] = 'L37/0557-M1'
    names1.loc[names1 == 'L370557-M72'] = 'L37/0557-M72'
    names1.loc[names1 == 'BENNETT K38/0190-M1'] = 'K38/0190-M1'
    names1.loc[names1.str.contains(' ')] = nan
    names1 = names1.str.split('-', expand=True)[0]
    names1.loc[~names1.str.contains('\d\d\d', na=True)] = nan
    #names1.loc[names1.str.contains('-M')] = nan
    names1 = names1.str.upper()
    return(names1)


def proc_ht_use_data(ht_data, n_std=4):
    """
    Function for parse_ht_xml to process the data and aggregate it to a defined resolution.
    """
    from numpy import nan, abs
    from pandas import Series, concat

    ### Groupby mtypes and sites
    grp = ht_data.groupby(level=['mtype', 'site'])

    res1 = []
    for index, data1 in grp:
        data = data1.copy()
        mtype, site = index
#        units = sites[(sites.site == site) & (sites.mtype == mtype)].unit

        ### Select the process sequence based on the mtype and convert to period volume

        data[data < 0] = nan

        if mtype == 'Water Meter':
            ## Check to determine whether it is cumulative or period volume
            count1 = float(data.count())
            diff1 = data.diff()
            neg_index = diff1 < 0
            neg_ratio = sum(neg_index.values)/count1
            if neg_ratio > 0.1:
                outliers = abs(data - data.mean()) > (data.std() * n_std)
                data[outliers] = nan
                vol = data
            else:
                # Replace the negative values with zero and the very large values
                diff1[diff1 < 0] = data[diff1 < 0]
                outliers = abs(diff1 - diff1.mean()) > (diff1.std() * n_std)
                diff1.loc[outliers] = nan
                vol = diff1
        elif mtype == 'Abstraction Volume':
            outliers = abs(data - data.mean()) > (data.std() * n_std)
            data.loc[outliers] = nan
            vol = data
        elif mtype == 'Flow':
            outliers = abs(data - data.mean()) > (data.std() * n_std)
            data.loc[outliers] = nan

#            # Determine the diff index
#            t1 = Series(data.index).diff().dt.seconds.shift(-1)
#            t1.iloc[-1] = t1.iloc[-2]
#            t1.index = data.index
#            # Convert to volume
#            vol = data.multiply(t1, axis=0) * 0.001
            vol = (data * 60*60*24).fillna(method='ffill').round(4)
        elif mtype == 'Average Flow':
            outliers = abs(data - data.mean()) > (data.std() * n_std)
            data.loc[outliers] = nan
            vol = (data * 24).fillna(method='ffill').round(4)

        res1.append(vol)

    ### Convert to dataframe
    df1 = concat(res1).reset_index()

    ### Drop the mtypes level and uppercase the sites
    df2 = df1.drop('mtype', axis=1)
    df2.loc[:, 'site'] = df2.loc[:, 'site'].str.upper()

    ### Remove duplicate WAPs
    df3 = df2.groupby(['site', 'time']).data.last()

    return(df3)

















