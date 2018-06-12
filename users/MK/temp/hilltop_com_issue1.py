# -*- coding: utf-8 -*-
"""
Created on Fri May 18 08:31:30 2018

@author: MichaelEK
"""
import pandas as pd
from win32com.client import Dispatch
from datetime import datetime
import Hilltop


def pytime_to_datetime(pytime):
    """
    Function to convert a PyTime object to a datetime object.
    """

    dt1 = datetime(year=pytime.year, month=pytime.month, day=pytime.day, hour=pytime.hour, minute=pytime.minute)
    return dt1


#############################################
### Parameters

hts = '\\\\hilltop01\\Hilltop\\Data\\Telemetry\\Loncel.hts'

sites = ['L36/1764-M1']

############################################
### COM

## Extract sites and measurement types

cat = Dispatch("Hilltop.Catalogue")
if not cat.Open(hts):
    raise ValueError(cat.errmsg)

dfile = Dispatch("Hilltop.DataRetrieval")
try:
    dfile.Open(hts)
except ValueError:
    print(dfile.errmsg)

sites_lst = []

cat.StartSiteEnum
while cat.GetNextSite:
    site_name = cat.SiteName.encode('ascii', 'ignore').decode()
    if sites is None:
        pass
    elif site_name in sites:
        pass
    else:
        continue
    while cat.GetNextDataSource:
        ds_name = cat.DataSource.encode('ascii', 'ignore').decode()
        try:
            start1 = pytime_to_datetime(cat.DataStartTime)
            end1 = pytime_to_datetime(cat.DataEndTime)
        except ValueError:
            bool_site = dfile.FromSite(site_name, ds_name, 1)
            if bool_site:
                start1 = pytime_to_datetime(cat.DataStartTime)
                end1 = pytime_to_datetime(cat.DataEndTime)
            else:
                print('No site data for ' + site_name + '...for some reason...')
        while cat.GetNextMeasurement:
            mtype1 = str(cat.Measurement.encode('ascii', 'ignore').decode())
            if mtype1 == 'Item2':
                continue
            else:
                pass
            divisor = cat.Divisor
            unit1 = str(cat.Units.encode('ascii', 'ignore').decode())
            if unit1 == '%':
#                    print('Site ' + name1 + ' has no units')
                unit1 = ''
            sites_lst.append([site_name, ds_name, mtype1, unit1, divisor, str(start1), str(end1)])

sites_df = pd.DataFrame(sites_lst, columns=['site', 'data_source', 'mtype', 'unit', 'divisor', 'start_date', 'end_date'])
dfile.Close()
cat.Close()


## Extract ts data
agg_unit_dict = {'l/s': 1, 'm3/s': 1, 'm3/hour': 1, 'mm': 1, 'm3': 4}

dfile = Dispatch("Hilltop.DataRetrieval")
try:
    dfile.Open(hts)
except ValueError:
    print(dfile.errmsg)

df_lst = []
for i in sites_df.index:
    site = sites_df.loc[i, 'site']
    mtype = sites_df.loc[i, 'mtype']
    unit = sites_df.loc[i, 'unit']

    agg_val = agg_unit_dict[unit]

    if dfile.FromSite(site, mtype, 1):

        ## Set up start and end times and aggregation initiation
        start_time = pytime_to_datetime(dfile.DataStartTime)
        end_time = pytime_to_datetime(dfile.DataEndTime)
        if (start_time.year < 1900) | (end_time.year < 1900):
            print('Site ' + site + ' has a start or end time prior to 1900')
            continue
        start = dfile.DataStartTime
        end = dfile.DataEndTime
        if not dfile.FromTimeRange(start, end):
            continue
        dfile.SetMode(agg_val, '1 day')

        ## Extract data
        data = []
        time = []
        if dfile.getsinglevbs == 0:
            t1 = dfile.value
            if isinstance(t1, str):
                print('site ' + site + ' has nonsense data')
            else:
                data.append(t1)
                time.append(str(pytime_to_datetime(dfile.time)))
                while dfile.getsinglevbs != 2:
                    data.append(dfile.value)
                    time.append(str(pytime_to_datetime(dfile.time)))
                if data:
                    df_temp = pd.DataFrame({'time': time, 'data': data, 'site': site, 'mtype': mtype})
                    df_lst.append(df_temp)

dfile.Close()
if df_lst:
    df1 = pd.concat(df_lst)
    df1.loc[:, 'time'] = pd.to_datetime(df1.loc[:, 'time'])
    tsdata = df1.set_index(['mtype', 'site', 'time']).data

## Results for COM

sites_com = sites_df
tsdata_com = tsdata

### Python package

## Extract sites and measurement types

dfile1 = Hilltop.Connect(hts)
site_list = Hilltop.SiteList(dfile1)

if isinstance(sites, list):
    site_list = [i for i in site_list if i in sites]

site_info = pd.DataFrame()

for i in site_list:
    try:
        info1 = Hilltop.MeasurementList(dfile1, i)
    except SystemError as err:
        print('Site ' + str(i) + " didn't work. Error: " + str(err))
        continue
    info1.loc[:, 'site'] = i.encode('ascii', 'ignore').decode()
    site_info = pd.concat([site_info, info1])
site_info.reset_index(drop=True, inplace=True)

site_info.loc[:, 'Start Time'] = pd.to_datetime(site_info.loc[:, 'Start Time'], format='%d-%b-%Y %H:%M:%S')
site_info.loc[:, 'End Time'] = pd.to_datetime(site_info.loc[:, 'End Time'], format='%d-%b-%Y %H:%M:%S')

len_all = len(site_list)
len_got = len(site_info.site.unique())
print('Missing ' + str(len_all - len_got) + ' sites, which is ' + str(round(100 * ((len_all - len_got)/len_all), 1)) + '% of the total')

## Extract ts data

data1 = []

for i in site_info.index:
    site = site_info.loc[i, 'site']
    mtype = site_info.loc[i, 'Measurement']
    start = site_info.loc[i, 'Start Time'].ceil('1D').strftime('%d-%b-%Y %H:%M:%S')
    end = site_info.loc[i, 'End Time'].strftime('%d-%b-%Y %H:%M:%S')

    d1 = Hilltop.GetData(dfile1, site, mtype, start, end, method='Average', interval='1 day', alignment='00:00')

    d1.name = 'data'
    d1.index.name = 'time'
    d2 = d1.reset_index()
    d2.loc[:, 'site'] = site
    d2.loc[:, 'mtype'] = mtype
    data1.append(d2)

tsdata1 = pd.concat(data1)
tsdata2 = tsdata1.set_index(['mtype', 'site', 'time']).data

Hilltop.Disconnect(dfile1)

## Results for python library
sites_py = site_info
tsdata_py = tsdata2

############################################
## Results comparison

print(sites_com)
print(sites_py)

print(tsdata_com)
print(tsdata_py)









