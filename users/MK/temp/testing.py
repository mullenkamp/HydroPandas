# -*- coding: utf-8 -*-
"""
Created on Fri Oct 07 18:16:59 2016

@author: MichaelEK
"""

from os import path
from pandas import read_table, DataFrame, concat, merge, Timedelta, datetime, to_datetime, DateOffset, date_range, Timestamp, read_csv, to_numeric, Series
from misc_fun import printf
from allo_use_fun import stream_nat, flow_ros
from stats_fun import lin_reg
from import_fun import rd_hydstra_csv, rd_vcn, rd_ts, rd_henry
from spatial_fun import grid_interp_iter, catch_net, pts_poly_join, catch_agg, grid_catch_agg
from geopandas import read_file
from ts_stats_fun import malf7d, flow_stats, est_low_flows, flow_dur
import matplotlib.pyplot as plt
import seaborn as sns
from numpy import nan, log, in1d, polyfit, poly1d, interp, mean, median
from scipy import stats, optimize
import statsmodels.api as sm
import xray as xr
from ast import literal_eval
from shapely.wkt import loads

#########################################
#### xray N-dimentional array library

base_path = 'Y:/VirtualClimate/projections/2006-2100'

precip_nc = 'RAW/ECan_nTotalPrecipCorr_VCSN_GFDL-CM3_2006-2100_RCP6.0.nc'
et_nc = 'RAW/ECan_PE_VCSN_GFDL-CM3_2006-2100_RCP6.0.nc'

poly_shp = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/vcsn_shp/73_staions_polyg.shp'
nc_path = path.join(base_path, precip_nc)

export_path = path.join(base_path, 'nc_rain.csv')

#t1_nc = 'C:/ecan/local/Projects/Waimakariri/GIS/vector/results/ECan_test.nc'

t1 = xr.open_dataset(path.join(base_path, precip_nc))

t2 = t1.to_dataframe().drop('time_bnds', axis=1).reset_index()
t2 = t2[t2.nb2 == 0].drop('nb2', axis=1)

t2[:1000].to_csv('C:/ecan/local/Projects/Waimakariri/GIS/vector/results/ECan_test_nc.csv')


#######################################
#### Flow duration test

flow_csv = 'C:\\ecan\\shared\\base_data\\flow\\all_flow_data.csv'

flow = rd_ts(flow_csv)
stats1 = flow_stats(flow)

site = ['71135', '71129']

flow = flow[site]

p1 = polyfit(both2.rank1, both2.flow1, 6)
p2 = poly1d(p1)

flow2 = flow1['71129']
rank2 = flow2.rank(axis=0, pct=True, ascending=False)

both2 = concat([flow2, rank2], axis=1).dropna()
both2.columns = ['flow1', 'rank1']
both3 = both2.sort_values('rank1')

both2.plot(x='rank1', y='flow1', kind='scatter', xlim=[0, 1], ylim=[0, max(both2.flow1)])

plt1 = plt.plot(both2['rank1'], both2['flow1'], '.', both2['rank1'], p2(both2['rank1']), '.')

interp(.0001, both3['rank1'], both3['flow1'])

fdc1 = flow_dur(flow, plot=True)

t1 = fdc1[0]
t1a = t1[::-1]

t1 = fdc1[1]
t1a = t1[::-1]

interp(mean(t1a['flow1']), t1a['flow1'], t1a['rank1'])

interp(.01, t1['rank1'], t1['flow1'])

########################################
#### Reliability

select=[69635]
start_date='1900-01-01'
end_date='2016-06-30'
fill_na=False
flow_csv='C:/ecan/shared/base_data/flow/all_flow_data.csv'
min_flow_cond_csv='C:/ecan/shared/base_data/usage/restrictions/min_flow_cond.csv'
min_flow_id_csv='C:/ecan/shared/base_data/usage/restrictions/min_flow_id.csv'
min_flow_mon_csv='C:/ecan/shared/base_data/usage/restrictions/mon_min_flow.csv'
min_flow_restr_csv='C:/ecan/shared/base_data/usage/restrictions/min_flow_restr.csv'

ros1 = flow_ros(select)
ros3 = ros1['2013']
#ros2[select][sel1]
ros3[select][sel1]
ros3 = ros1['2000-03']
ros1['2000-03']


tem1 = ros1['temuka_a'].dropna()
tem_flow = flow[69602][tem1.index]

flow[69514][ros2.index]
ros2['orari_a']

######################################
#### mssql to shapely

server = 'SQL2012PROD05'
database = 'GISPUBLIC'

table = 'PLAN_NZTM_SURFACE_WATER_ALLOCATION_ZONES'

col_names = ['ZONE_NAME', 'ZONE_GROUP_NAME']

df1 = rd_sql(server, database, table, col_names, geo_col=True)

####################################
#### Determine the min flow conditions for each flow site, band, and crc

#crc_lst = ['CRC164043', 'CRC063564']

crc_lst = 'C:/ecan/local/Projects/requests/ashley_river/crc_ash.csv'

t1 = crc_band_flow(crc_lst=crc_lst, names=True)

t1.to_csv('C:/ecan/local/Projects/requests/ashley_river/ash_crc_site_min_flow2.csv', index=False)

t2 = t1.site.unique()
len(t2)

### Otop

site_lst = 'C:/ecan/local/Projects/otop/GIS/tables/otop_min_flow_sites.csv'

b1 = crc_band_flow(site_lst=site_lst)
b2 = b1.drop('crc', axis=1).drop_duplicates()


##########################################
#### Kivy testing

from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.textinput import TextInput


class LoginScreen(GridLayout):

    def __init__(self, **kwargs):
        super(LoginScreen, self).__init__(**kwargs)
        self.cols = 2
        self.add_widget(Label(text='User Name'))
        self.username = TextInput(multiline=False)
        self.add_widget(self.username)
        self.add_widget(Label(text='password'))
        self.password = TextInput(password=True, multiline=False)
        self.add_widget(self.password)


class MyApp(App):

    def build(self):
        return LoginScreen()


if __name__ == '__main__':
    MyApp().run()


####################################
#### Land cover

data_csv = 'C:/ecan/shared/projects/otop/reports/current_state_2016/Pareora_landcover.csv'

data = read_csv(data_csv)

data2 = data.pivot(index='Catchment', columns='Landcover', values='AREA_HA').round(1)
data2[data2.isnull()] = 0

data2.to_csv('C:/ecan/shared/projects/otop/reports/current_state_2016/Pareora_landcover_pivot.csv')


##################################
#### Hydstra import with hydllp

from import_fun import rd_hydstra_db
from numpy import unique

sites = [69618, 69614]

export_path = 'C:\\ecan\\local\\temp\\hydstra\\test1.csv'

df = rd_hydstra_db(sites, export=False, export_path=export_path)


##########################################
#### Read hilltop dll

from ctypes import *

dll_file = 'H:\\Hydrolib_Net.dll'
dll_file = 'H:\\Hydrolib.dll'


lib = WinDLL(dll_file)
#libc = CDLL(dll_file)

open1 = lib['Open']

import win32com.client

win32com.client


#######################################
#### Convert VCSN data from csv to NetCDF

from pandas import read_csv, date_range, Timestamp
from import_fun import rd_vcn
import xarray as xr
from numpy import in1d, where, random, genfromtxt, concatenate, append, expand_dims, dstack, stack, repeat
import numpy as np

comp_table='Y:/VirtualClimate/VCN_id_comp_table.csv'

comp_tab = read_csv(comp_table)

data = rd_vcn()

labels = [comp_tab.loc[(comp_tab.ecan_id == i), 'net_id'].iloc[0] for i in data.columns]

temp = 15 + 8 * random.randn(2, 2, 3)
precip = 10 * random.rand(2, 2, 3)
lon = [[-99.83, -99.32], [-99.79, -99.23]]
lat = [[42.25, 42.21], [42.63, 42.59]]

ds = xr.Dataset({'temperature': (['x', 'y', 'time'],  temp),
                 'precipitation': (['x', 'y', 'time'], precip)},
                  coords={'lon': (['x', 'y'], lon),
                          'lat': (['x', 'y'], lat),
                          'time': date_range('2014-09-06', periods=3),
                          'reference_time': Timestamp('2014-09-05')})

lon1 = [-99.83, -99.32]
lat1 = [42.25, 42.21]

ds1 = xr.Dataset({'temp': (['lon', 'lat', 'time'],  temp),
                 'precip': (['lon', 'lat', 'time'], precip)},
                  coords={'lon': lon1,
                          'lat': lat1,
                          'time': date_range('2014-09-06', periods=3)})


xr.DataArray(y1, dims=[ 'date', 'x', 'y'], coords={'x': x[:2], 'y': y[:2], 'date': y1.index})

y3 = y2.set_index(['x', 'y', 'date'])
xr.Dataset.from_dataframe(y3)



f1 = read_csv(path.join(data_dir, f), usecols=[1,2])

xr.Dataset({'precip': (['x', 'y', 'time'], h2[:,0]), 'ET': (['x', 'y', 'time'], h2[:,1])}, coords={'x': x[:1], 'y': y[:1], 'date': y1.index})


h2 = concatenate((h1, h1), axis=0)
expand_dims(h2, axis=0)

h2 = stack((h1, h1))


h1 = read_csv(path.join(data_dir, f), usecols=[1,2])

len1 = len(y1.index)
h2 = h1.set_index([y1.index, repeat(x[0], len1), repeat(y[0], len1)])
f2 = f1.set_index([y1.index, repeat(x[1], len1), repeat(y[1], len1)])

f3 = concat([h2, f2])
f3.index.names = ['time', 'x', 'y']


xr.Dataset.from_dataframe(f3)

da1 = xr.DataArray()


###############################################
#### flow import

rec_sites='All'
gauge_sites='None'
site_ref_csv='S:/Surface Water/shared/base_data/flow/hydstra_recorder_numbers.csv'
start='1900-01-01'
end='2016-09-30'
min_days=365
export_flow=True
export_stats=True
export_shp=True
export_flow_path='all_flow_data.csv'
export_stats_path='all_flow_stats.csv'
export_rec_shp_path = 'C:/ecan/local/Projects/otop/GIS/vector/rec_loc.shp'
export_gauge_shp_path = 'C:/ecan/local/Projects/otop/GIS/vector/gauge_loc.shp'


rec_sites = gauge_sites = 'C:/ecan/local/Projects/otop/GIS/vector/otop_catchments.shp'


##############################################
#### Removing files
from misc_fun import lst_rem_files

path = r'C:\music\soundtracks\temp\Mozart - Requiem (Bohm)'
pattern = '.flac'

t1 = lst_rem_files(path, pattern, True)


#############################################
#### 69607 prior to dam estimations

import statsmodels.api as sm
import statsmodels.formula.api as smf
from pandas import read_csv, concat
from import_fun import rd_ts, rd_henry, flow_import
from ts_stats_fun import flow_stats, gauge_proc
from stats_fun import lin_reg
import seaborn as sns

flow_csv = r'C:\ecan\shared\base_data\flow\all_flow_rec_data.csv'
catch_shp = r'C:\ecan\local\Projects\otop\GIS\vector\UF_SH1\opihi_catch1.shp'
sites = [69615, 69616, 69618, 69607]

r_flow_shp = r'C:\ecan\local\Projects\otop\GIS\vector\UF_SH1\r_flow.shp'
g_flow_shp = r'C:\ecan\local\Projects\otop\GIS\vector\UF_SH1\g_flow.shp'

g_sites = [169603, 169601]
g_sites2 = [69615, 69616, 69614, 69661, 69618, 69635, 69607]
r_sites1 = [69614, 69618, 69635, 69661, 69607]

flow = rd_ts(flow_csv)
flow.columns = flow.columns.astype(int)
flow = flow[sites]
flow1 = flow.dropna(how='all')

gauge = rd_henry(g_sites2, sites_by_col=True)
gauge.columns = gauge.columns.astype(int)
#gauge2 = gauge.dropna(subset=[69615, 69616, 69607])

r_flow, g_flow = flow_import(rec_sites=catch_shp, gauge_sites=catch_shp, end='2010', export_shp=True, export_rec_shp_path=r_flow_shp, export_gauge_shp_path=g_flow_shp)

t1 = r_flow[[69614, 69607]].dropna()

g_flow1 = g_flow.pivot('date', 'site', 'flow')
g_flow2 = concat([g_flow1, gauge], axis=1)

g_flow3 = g_flow2[:'1998']
count1 = g_flow3.count()
count1[count1 > 20]


g_stats = flow_stats(g_flow2)
r_stats = flow_stats(r_flow)


g_sites = g_flow2[sites]

proc1 = gauge_proc(g_sites.loc[:'1997', [69615, 69607]], min_gauge=2)


t2 = r_flow[r_sites1].dropna()

t2['est'] = t2[[69614, 69618, 69635]].sum(axis=1)

diff1 = t2[69607] - t2['est']


t3 = r_flow[[69614, 69661]].dropna()

site3 = 69607
t4 = concat([r_flow.loc[:'1997', [69614, 69618]], g_flow2.loc[:'1997', site3]], axis=1).dropna()
t4['est'] = t4[[69614, 69618, 69635]].sum(axis=1)
diff1 = (t4[69607] - t4['est'])

reg2 = lin_reg(t4[69618], t4[69607], log_x=False, log_y=False)

sns.regplot(x=t4[69618], y=t4[69607])

t4[t4 == 0] = nan
t4.loc[t4[69618] > 3.3, 69618] = nan
#sns.regplot(x=log(t4[69618]), y=log(t4[69607]))


############################################
#### Okuku River flows

from import_fun import flow_import, rd_hydrotel
from ts_stats_fun import flow_stats

t1 = flow_import(rec_sites=[66213], export_shp=True, export_rec_shp_path='C:/ecan/shared/GIS_base/vector/hydro_sites/test1.shp')

t2 = rd_hydrotel([66213])
flow_stats(t2)

t2['2017-01-01':].plot()

###########################################
#### Geopandas pandas export

from import_fun import rd_sql
from fiona import crs
from geopandas import read_file, GeoDataFrame
from shapely.wkt import loads
from shapely.geometry import Point
import fiona
import pycrs

epsg = 2193

locs = 'C:/ecan/shared/GIS_base/vector/hydro_sites/all_rec_loc.shp'
t1 = read_file(locs)[['site', 'geometry']][:3]
t1.crs
t1.to_file('C:/ecan/shared/GIS_base/vector/hydro_sites/working_export.shp')

t2 = t1.to_crs(epsg=2193)
t2.to_file('C:/ecan/shared/GIS_base/vector/testing/test4.shp')


x = t1.geometry.apply(lambda p: p.x).values
y = t1.geometry.apply(lambda p: p.y).values
geometry = [Point(xy) for xy in zip(x, y)]
df = GeoDataFrame(t1['site'], geometry=geometry, crs=fromcrs_proj4)
df = GeoDataFrame(t1['site'], geometry=geometry, crs=t1.crs)
df.to_file('C:/ecan/shared/GIS_base/vector/testing/test5.shp')

site_geo = rd_sql('SQL2012PROD05', 'GIS', 'vGAUGING_NZTM', col_names=['SiteNumber', 'RIVER', 'SITENAME'], geo_col=True)
site_geo.columns = ['site', 'river', 'site_name', 'geometry']
site_geo['river'] = site_geo.river.apply(lambda x: x.title())
site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.title())
site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace(' (Recorder)', ''))
site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Sh', 'SH'))
site_geo['site_name'] = site_geo.site_name.apply(lambda x: x.replace('Ecs', 'ECS'))

site_geo.crs

site_geo.to_file('C:/ecan/shared/GIS_base/vector/hydro_sites/test3.shp')

crs.from_epsg(2193)



wgs84 = read_file('C:/ecan/shared/GIS_base/vector/hydro_sites/wgs84a.shp')
wgs84.crs = fromcrs_proj4
wgs84.to_file('C:/ecan/shared/GIS_base/vector/hydro_sites/wgs84b.shp')


t5 = fiona.open('C:/ecan/shared/GIS_base/vector/hydro_sites/wgs84a.shp')

with t5 as t:
    crs1 = t.crs

t2 = t1.to_crs(proj_dict[4326])
t2.crs
t2.to_file('C:/ecan/shared/GIS_base/vector/hydro_sites/test1.shp')


proj_dict = {2193: '+ellps=GRS80 +k=0.9996 +lat_0=0 +lon_0=173 +no_defs +proj=tmerc +units=m +x_0=1600000 +y_0=10000000', 4326: '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'}


t3 = t2.to_crs(proj_dict[2193])
t3.crs
t3.to_file('C:/ecan/shared/GIS_base/vector/hydro_sites/test2.shp')

from fiona import crs


crs.from_epsg(2193)
to_string(crs.from_epsg(2193))


nz1 = pycrs.parser.from_epsg_code(4326).to_proj4()
nz2 = nz1.to_proj4()

t2 = t1.to_crs(nz2)
t3 = t2.to_crs(nz2)


#######################################
#### Watershed delineation with pygeoprocessing

import pygeoprocessing
from pygeoprocessing.routing.routing import delineate_watershed, fill_pits

dem_uri = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\dem_loc.tif'
pp_uri = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\flow_sites_corr_v01.shp'
snap_dis = 20
flow_thres = 500
poly_out = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\catch_out1.shp'
pp_snap_out = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\flow_sites_snap.shp'
str_out = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\stream_dem.shp'

delineate_watershed(dem_uri, pp_uri, snap_dis, flow_thres, poly_out, pp_snap_out, str_out)

dem_fill_out = r'C:\ecan\local\Projects\otop\GIS\vector\malf_check\test\dem_fill1.tif'
fill_pits(dem_uri, dem_fill_out)

from osgeo import gdal



######################################
####

t1 = crc_band_flow([70105])

#####################################
#### Hydrograph plotting

from import_fun import rd_ts, rd_hydrotel
import seaborn as sns
from pandas import concat
import matplotlib.pyplot as plt
from ts_stats_fun import w_resample
from numpy import where, in1d, intersect1d

data_csv = r'C:\ecan\local\Projects\requests\Otematata\Otematata.csv'
data = rd_ts(data_csv)

flow = data.iloc[:,3]
precip = data.iloc[:,:3]

flow = concat([flow, flow*.5], axis=1)
flow.columns = ['orig', 'new']


flow_site = [71109]
rain_site = [409310, 408401]
start = '2017-01-21 12:00:00'
end = '2017-01-23 12:00:00'
time_format='%d-%m %H:%M'
x_period='hours'
x_n_periods=4
x_grid = True
precip_units='mm/hour'
path = r'C:\ecan\local\Projects\requests\Otematata'
filename = '71109_plot.png'

flow1 = rd_hydrotel(flow_site, use_site_name=True)[start:end]
precip1 = rd_hydrotel(rain_site, use_site_name=True, dtype='Rainfall')[start:end]

precip = w_resample(precip1, period='hour', fun='sum')
flow = w_resample(flow1, period='min', n_periods=15, fun='mean')


plt2 = hydrograph_plot(flow, precip, x_period, x_n_periods, time_format, precip_units=precip_units, x_grid=x_grid, export_path=path, export_name=filename)


#######################################
#### Water use
from numpy import in1d
from import_fun import rd_ts, rd_hydrotel, rd_sql

##Check sites for doubling usage
check1 = ['M35/1653', 'M35/1606', 'M35/1546', 'M35/3128']

use_daily2.index = use_daily2.date
use_daily2 = use_daily2.drop('date', axis=1)
use_mon1 = use_daily2[:100000].groupby(['wap', TimeGrouper('M')]).sum().round(2)




len(use1.wap.unique())
#len(crc_wap.wap.unique())

len(use1.wap[in1d(use1.wap.unique(), crc_wap.wap.unique())])


#len(crc_wap.crc.unique())
len(allo_ts.crc.unique())

len(allo_ts.crc[in1d(allo_ts.crc.unique(), crc_wap.crc.unique())])


shp = r'C:\ecan\local\Projects\otop\GIS\vector\Opihi_Catchment.shp'



crc = 'CRC011399'
wap = 'M35/5579'

t1 = lw[(lw.use_type == 'water_supply') & lw.usage.notnull()]
t1.sort_values(['crc', 'dates'])
t2 = t1[t1.crc == crc]

crc_wap[crc_wap.crc == crc]


ht_use_id = rd_sql('SQL2012TEST01', 'WaterTake', 'UsageWap', ['Id', 'Name'])
ht_use_id.columns = ['wap_id', 'wap']

ht_use_id.loc[:, 'wap'] = ht_use_id.wap.str.replace('[:\.]', '/')
#    ht_use_id.loc[ht_use_id.Name == 'L35183/580-M1', 'Name'] = 'L35/183/580-M1' What to do with this one?
ht_use_id.loc[ht_use_id.wap == 'L370557-M1', 'wap'] = 'L37/0557-M1'
ht_use_id.loc[ht_use_id.wap == 'L370557-M72', 'wap'] = 'L37/0557-M72'
ht_use_id = ht_use_id[~ht_use_id.wap.str.contains(' ')]
ht_use_id.loc[:, 'wap'] = ht_use_id.wap.str.split('-', expand=True)[0]
ht_use_id = ht_use_id[ht_use_id.wap.str.contains('\d\d\d')]
ht_use_id.loc[:, 'wap'] = ht_use_id.loc[:, 'wap'].str.upper()

wap_id = ht_use_id.loc[ht_use_id.wap == wap, 'wap_id'].values.astype('int32').tolist()

ht_use = rd_sql('SQL2012TEST01', 'WaterTake', 'UsageReading', ['UsageWap', 'Date', 'Value'], 'UsageWap', wap_id)
ht_use.columns = ['wap_id', 'date', 'usage']



flow_sites = rd_sql(code='flow_sites_gis')


ht_use = rd_sql('SQL2012TEST01', 'WaterTake', 'UsageReading', ['UsageWap', 'Date', 'Value'])
ht_use.columns = ['wap_id', 'date', 'usage']

ht_use_id = rd_sql('SQL2012TEST01', 'WaterTake', 'UsageWap', ['Id', 'Name'])
ht_use_id.columns = ['wap_id', 'wap']

#### Process WAP/CRC IDs
ht_use_id.loc[:, 'wap'] = ht_use_id.wap.str.replace('[:\.]', '/')
#    ht_use_id.loc[ht_use_id.Name == 'L35183/580-M1', 'Name'] = 'L35/183/580-M1' What to do with this one?
ht_use_id.loc[ht_use_id.wap == 'L370557-M1', 'wap'] = 'L37/0557-M1'
ht_use_id.loc[ht_use_id.wap == 'L370557-M72', 'wap'] = 'L37/0557-M72'
ht_use_id = ht_use_id[~ht_use_id.wap.str.contains(' ')]
ht_use_id.loc[:, 'wap'] = ht_use_id.wap.str.split('-', expand=True)[0]
ht_use_id = ht_use_id[ht_use_id.wap.str.contains('\d\d\d')]
ht_use_id.loc[:, 'wap'] = ht_use_id.loc[:, 'wap'].str.upper()



#PARENT_B1_ALT_ID='CRC951874.1'
cols = ['crc', 'use_type', 'dates', 'mon_vol', 'usage']
crc = 'CRC011399'
wap = 'J39/0654'

crc = 'CRC151368'
wap = 'I37/0040'

t1 = lw[(lw.use_type == 'water_supply') & lw.usage.notnull()]
t1.sort_values(['crc', 'dates'])
t2 = t1[t1.crc == crc]

crc_wap[crc_wap.crc == crc]


t2 = lw[(lw.use_type == 'irrigation') & lw.usage.notnull()]
t2[cols][t2.mon_vol > 10000000].sort_values(['crc', 'dates'])


wap_id = ht_use_id[ht_use_id.wap == wap].wap_id.values[0]

ht_use[ht_use.wap_id == wap_id]




#######################################
#### Hilltop

import win32com.client
import pythoncom
from pandas import to_datetime, DataFrame, Series, TimeGrouper, Grouper
from core.ecan_io import rd_hilltop_sites
from time import clock

hts = r'H:\Data\Annual\Anonymous_Volume.hts'
hts = r'H:\Data\Annual\Anonymous_flow.hts'
hts = r'H:\Data\Annual\Anonymous_Accumulated.hts'
hts = r'C:\ecan\hilltop\xml_test\annual\Anonymous_Volume.hts'

sites1 = rd_hilltop_sites(hts)

wap = "J36/0008-M1"
wap = "K36/0128-M1"

dfile = win32com.client.Dispatch("Hilltop.DataRetrieval")
if not dfile.Open(path):
    print (dfile.errormsg)
    exit

#dfile.FromSite(wap, "Abstraction Volume", 1)

index = 2
dfile.FromSite(sites1.site[index], sites1.mtype[index], 1)

#cat1 = dfile.Catalogue


#print(dfile.units)
#print(dfile.DataEndTime)

start1 = to_datetime(dfile.DataStartTime.Format('%Y-%m-%d %H:%M:%S')).ceil('D')


#dfile.DataStartTime.Format('%Y-%m-%d %H:%M:%S')
#
#dfile.DataEndTime.Format('%Y-%m-%d %H:%M:%S')

result = dfile.FromTimeRange(start1, dfile.DataEndTime.Format('%Y-%m-%d %H:%M:%S'))

dfile.SetMode(3, "1 day")
#dfile.Method(86400, True)



iter1 = dfile.getsinglevbs
data = []
time = []
while iter1 == 0:
    data.append(dfile.value)
    time.append(dfile.time.Format('%Y-%m-%d %H:%M:%S'))
    iter1 = dfile.getsinglevbs

s1 = Series(data, index=to_datetime(time), name=sites1.site[index])
s1.index.name = 'time'

dfile.Close()

t1 = dfile.time
to_datetime(dfile.time)

getsinglevbs
s2.resample('D').sum()


cat = win32com.client.Dispatch("Hilltop.Catalogue")
if not cat.Open(path):
    print (cat.errormsg)
    exit

sites = []

cat.StartSiteEnum
iter2 = cat.GetNextSite
while iter2:
    name1 = cat.SiteName
    cat.GetNextDataSource
    mtype1 = cat.DataSource
    cat.GetNextMeasurement
    unit1 = cat.Units
    sites.append([name1, mtype1, unit1])
    iter2 = cat.GetNextSite

sites_df = DataFrame(sites, columns=['site', 'mtype', 'unit'])

cat.Close()


sites = rd_hilltop_sites(path)




grp1 = df1.groupby([Grouper(level='mtype'), Grouper(level='site'), TimeGrouper('D', level='time')])




##################################
#### ET calcs

from import_fun import rd_ts


#########
### Parameters

csv_path = r'C:\ecan\local\temp\met_data\3925_met_data1.csv'

col_names = ['U_z', 'precip', 'T_mean', 'T_max', 'T_min', 'vpd', 'P_atmos', 'n_sun', 'R_s', 'soilm']

#########
### Load in data

df = rd_ts(csv_path)
df.columns = col_names



ETo_FAO['2016-04-12':'2016-04-15'] = nan



#################################
#### Converting a csv flow ts

from import_fun import rd_ts
from pandas import read_csv
from ts_stats_fun import w_resample, tsreg
from misc_fun import printf

csv = r'C:\ecan\local\Projects\hilltop\tests\J39_0654_Water_Use.csv'

#c1 = rd_ts(csv, header=None)
#c1 = read_csv(csv, header=None)

ts = read_csv(csv, parse_dates=[[0,1]], infer_datetime_format=True, index_col=0, dayfirst=True, header=None)
ts.name = 'volume_m3'
ts.index.name = 'date'

ts.index.strftime('%d/%m/%Y')

## Clean
min1 = ts.index.minute
ts2 = ts[min1 != 18]

## Regularize
ts2a = tsreg(ts2, freq='15min', interp=True)

## calc inst volumes
ts3 = ts2a * 60*15/1000.0

## output
c1 = read_csv(csv, header=None)[[0, 1]][min1 != 18]
c1.columns = ['date', 'time']
c1['vol_m3'] = ts3.values

c1.to_csv(r'C:\ecan\local\Projects\hilltop\tests\J39_0654_Water_Use_vol.csv', index=False)

ts_day = ts3.resample('D').sum()


###################################
#### Convert flow rates to daily volumes

from import_fun import rd_ts

csv = r'C:\ecan\local\Projects\requests\graham\2017-02-09\I39_0033_CRC144880_usage.csv'

data1 = rd_ts(csv)*60*15/1000
ts_day = data1.resample('D').sum().round()
ts_day.to_csv(r'C:\ecan\local\Projects\requests\graham\2017-02-09\I39_0033_CRC144880_usage_vol.csv')

###################################
### grid aggregation function testing

data = 'T:\Temp\Patrick\geostats\HRM_MPS_Summary_FULL.csv'
n_levels = 4
export_path = 'T:\Temp\Patrick\geostats'

##################################
### Allocation stuff

from core.ecan_io import rd_sql
from core.misc import rd_dir

allo_cav = rd_sql(code='crc_ann_vol_acc')
allo_use = rd_sql(code='crc_use_type_acc')
takes = rd_sql(code='crc_acc')
crc = 'CRC991620.4'
crc = 'CRC000116.2'


t1 = allo_cav.groupby(['crc'])['cav'].count()
t1[t1 > 1]

t1 = allo_use.groupby(['crc', 'take_type'])['irr_area'].count()
t1[t1 > 1]

t1 = takes.groupby(['crc', 'take_type'])['use_type'].count().reset_index()
t2 = t1[t1.use_type > 1]
t2[t2.duplicated('crc', keep=False)]
t1[t1.duplicated('crc', keep=False)]

takes[takes.crc == crc]

allo_cav[allo_cav.crc == crc]
allo_use[allo_use.crc == crc]



crc_wap = rd_sql(code=crc_wap_code)

ht_use = rd_sql('SQL2012TEST01', 'WaterTake', 'UsageReading', ['UsageWap', 'Date', 'Value'])
ht_use.columns = ['wap_id', 'date', 'usage']
ht_use_id = rd_sql('SQL2012TEST01', 'WaterTake', 'UsageWap', ['Id', 'Name'])
ht_use_id.columns = ['wap_id', 'wap']

#### Process WAP/CRC IDs
ht_use_id.loc[:, 'wap'] = ht_use_id.wap.str.replace('[:\.]', '/')
#    ht_use_id.loc[ht_use_id.Name == 'L35183/580-M1', 'Name'] = 'L35/183/580-M1' What to do with this one?
ht_use_id.loc[ht_use_id.wap == 'L370557-M1', 'wap'] = 'L37/0557-M1'
ht_use_id.loc[ht_use_id.wap == 'L370557-M72', 'wap'] = 'L37/0557-M72'
ht_use_id = ht_use_id[~ht_use_id.wap.str.contains(' ')]
ht_use_id.loc[:, 'wap'] = ht_use_id.wap.str.split('-', expand=True)[0]
ht_use_id = ht_use_id[ht_use_id.wap.str.contains('\d\d\d')]
ht_use_id.loc[:, 'wap'] = ht_use_id.loc[:, 'wap'].str.upper()

#### Merge ht use with IDs
ht_use2 = merge(ht_use, ht_use_id, on='wap_id').drop('wap_id', axis=1)

grp1 = ht_use2.groupby('wap')['usage']

### Negative/zero values

min1 = grp1.min()
neg1 = min1[min1 <= 0]

### Max values

quan1 = grp1.quantile(0.75)
max1 = grp1.max()

quan_max = max1/quan1

len(quan_max[quan_max > 10])

### Missing data

count1 = grp1.count()

grp2 = ht_use2.sort_values(['wap', 'date']).groupby('wap')['date']
start1 = grp2.first()
end1 = grp2.last()

diff1 = (end1 - start1).astype('timedelta64[D]').astype(int)

miss1 = diff1 - count1

len(crc_wap.wap.unique())
len(crc_wap[(crc_wap.max_rate_wap <= 0)].wap.unique())
len(crc_wap[crc_wap.max_rate_wap.isnull()].wap.unique())

len(crc_wap[(crc_wap.max_vol <= 0)].wap.unique())
len(crc_wap[crc_wap.max_vol.isnull()].wap.unique())

len(crc_wap[(crc_wap.return_period <= 0)].wap.unique())
len(crc_wap[crc_wap.return_period.isnull()].wap.unique())
crc_wap[crc_wap.wap == 'Migration: Not Classified']


wap1 = 'J39/0654'

ht_use2[ht_use2.wap.isin([wap1])]
usage[usage.wap.isin([wap1])]

###############################################
#### Allocation checks

lw1 = lw[(lw.take_type == 'Take Surface Water') & (lw.use_type == 'irrigation') & lw.dates.isin(['2015-06-30', '2016-06-30'])][['crc', 'dates', 'mon_vol', 'usage']]
lw1.sort_values('mon_vol', ascending=False)

waps2 = crc_wap[crc_wap.crc == 'CRC120695'].wap.unique()
wap_loc[wap_loc.wap.isin(waps2)]
waps_all2[waps_all2.crc == 'CRC120695']

lw1 = lw[['crc', 'dates', 'take_type', 'mon_vol', 'usage']]
lw2 = lw1.sort_values('mon_vol', ascending=False)

export = r'C:\ecan\local\Projects\requests\test\2017-02-17\otop_top_100.csv'

lw2[:100].to_csv(export, index=False)

['CRC070924.1', 'CRC001229.1', 'CRC042094.1']
'K37/3262'




##############################################
#### hydro class testing

import numpy as np
import pandas as pd
from core.ecan_io import rd_ts, rd_henry
from geopandas import read_file, GeoDataFrame
from xarray import Dataset, DataArray
from shapely.wkt import loads
from pycrs.parser import from_epsg_code
from core.classes.hydro import hydro

met_csv = r'C:\ecan\local\temp\met_data\3925_met_data1.csv'
sites_shp = r'C:\ecan\local\Projects\python\hydro_class\hydro_loc.shp'
flow_csv = 'S:/Surface Water/shared/base_data/flow/all_flow_rec_data.csv'
exp_csv = r'C:\ecan\local\Projects\python\hydro_class\test1.csv'
nc_path = r'C:\ecan\local\Projects\python\hydro_class\test1.nc'

iterables = [['flow', 'swl'], [69607, 70105]]
index = pd.MultiIndex.from_product(iterables, names=['mtype', 'site'])
df = pd.DataFrame(np.random.randn(10, 4), index=pd.date_range('2011-01-01', periods=10), columns=index)

df2a = pd.DataFrame(np.random.randn(20, 4), index=pd.date_range('2011-01-01', periods=20, freq='2H'), columns=index)

df3 = rd_ts(met_csv).iloc[:, :5]
df4 = df3.copy()
mtypes1 = ['U_z', 'precip', 'T_mean', 'T_max', 'T_min']
df3.columns = mtypes1
df3.index.name = 'date'

df5 = rd_henry(sites=[70105, 70103])

#t1 = hydro(df2)
#t2 = hydro(df3, site_names=70105)


sites_loc = read_file(sites_shp)[['site', 'river', 'site_name', 'geometry']]
sites_loc = sites_loc[sites_loc.site == 70105]

t3 = hydro(df4, mtypes=mtypes1, sites=70105)
t3.add_data(df2a)

t3.add_geo_loc(sites_loc, 'site')
t3.find_geo_loc()

t3.add_data(df5, mtypes='m_flow', index='date', sites='site')
t3.add_data(df5, mtypes='m_swl', index='date', sites='site')

t3.sel_ts(mtypes=['flow', 'precip'], pivot=True)
t3.sel_ts(mtypes=['m_flow', 'm_swl'], sites=70103)
t3.sel_ts(mtypes='m_swl', sites=70103)
t3.sel_ts(mtypes=['flow', 'precip', 'T_mean'], require=['flow', 'precip'])
sel1, geo1 = t3.sel(mtypes=['flow', 'precip', 'T_mean'], require=['flow', 'precip'], geo=True)

t7 = rd_ts_csv(flow_csv, mtypes='flow')
t7.find_geo()
t7.missing_geo_sites()

### xarray/netcdf

df1 = t3.flow.copy().values
df2 = t3.precip.copy()

xa1 = DataArray(t3.flow.values, coords=[t3.flow.index, t3.flow.columns.tolist()], dims=[t3.flow.index.name, 'flow_site'], name='flow')
xa2 = DataArray(t3.precip.values, coords=[t3.precip.index, t3.precip.columns.tolist()], dims=[t3.precip.index.name, 'precip_site'], name='precip')

x0 = Dataset()
x0 = x0.merge(xa1.to_dataset())
x0 = x0.merge(xa2.to_dataset())

df2 = x1.to_dataframe()

t3.flow.unstack()

ds0 = Dataset()
for i in t3.mtypes:
    if in1d(i, mtypes_norm)[0]:
        df1 = getattr(t3, i)
        ds1 = DataArray(df1.values, coords=[df1.index, df1.columns.tolist()], dims=[df1.index.name, i + '_site'], name=i).to_dataset()
        ds0 = ds0.merge(ds1)

geo1 = t3.geo.copy()
geo1['x'] = geo1.geometry.apply(lambda x: x.x)
geo1['y'] = geo1.geometry.apply(lambda x: x.y)
geo2 = geo1.drop('geometry', axis=1)
geo2.columns = ['geo_' + i for i in geo2.columns]

geo_ds = Dataset(geo2)
ds2 = ds0.merge(Dataset(geo2))

ds2.data_vars.keys()
geo_cols = [s for s in ds2.data_vars.keys() if 'geo_' in s]
geo_ds1 = ds2[geo_cols]
geo_ds1.to_dataframe()

t3.to_netcdf(nc_path)


from core.classes.hydro.indexing import sel_ts
from xarray import Dataset, DataArray
from numpy import in1d
from time import clock
from core.ts.sw import flow_stats, malf7d
from timeit import timeit
from pandas import melt
from core.ecan_io import rd_ts

ds0 = Dataset()
for i in t3.mtypes:
    df1 = getattr(t3, i).copy()
    if in1d(i, mtypes_norm)[0]:
        ds1 = DataArray(df1.values, coords=[df1.index, df1.columns.tolist()], dims=[df1.index.name, i + '_site'], name=i).to_dataset()
        ds0 = ds0.merge(ds1)
    else:
        df1.reset_index('site', inplace=True)
        df1.index.name = i + '_' + df1.index.name
        df1.rename(columns={'site': i + '_site', i: i + '_val'}, inplace=True)
        ds1 = Dataset(df1)
        ds0 = ds0.merge(ds1)

### Export data
ds0.to_netcdf(nc_path)


ds1 = DataArray(df1.values, coords=df1.index, dims=[df1.index.name, i + '_site'], name=i).to_dataset()

flow = rd_ts(flow_csv)
flow1 = flow.stack()
flow1.index.names = ['date', 'site']
flow1.name = 'val'

flow1.index.get_level_values('date')

flow1.groupby(level=1).mean()

%timeit flow_stats(flow)
%timeit malf7d(flow)
%timeit flow.mean()
%timeit flow1.mean(level=1)
%timeit flow.stack()
%timeit flow1.groupby(level=1).mean()
%timeit








### Convert polygon/line to string
epsg = 2193
catch_shp = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/catch1.shp'
out_shp_test = r'C:\ecan\local\Projects\python\hydro_class\trans_test1.shp'

p1 = read_file(catch_shp)
p1.rename(columns={'GRIDCODE': 'site'}, inplace=True)
p2 = p1.geometry.apply(lambda x: x.to_wkt())
p3 = GeoDataFrame(p1[['GRIDCODE', 'ID']], geometry=[loads(x) for x in p2], crs=from_epsg_code(epsg).to_proj4())

p1.to_file(out_shp_test)


### More testing with all possible input data
import numpy as np
import pandas as pd
from core.ecan_io import rd_ts, rd_henry
from geopandas import read_file, GeoDataFrame
from xarray import Dataset, DataArray
from shapely.wkt import loads
from pycrs.parser import from_epsg_code
from core.classes.hydro.base import hydro
from pandas import Grouper

met_csv = r'C:\ecan\local\temp\met_data\3925_met_data1.csv'
sites_shp = r'C:\ecan\local\Projects\python\hydro_class\hydro_loc.shp'
flow_csv = 'S:/Surface Water/shared/base_data/flow/all_flow_rec_data.csv'
exp_csv = r'C:\ecan\local\Projects\python\hydro_class\test1.csv'
nc_path = r'C:\ecan\local\Projects\python\hydro_class\test1.nc'
catch_shp = 'C:/ecan/local/Projects/otop/GIS/vector/min_flow/catch1.shp'
test_shp = r'C:\ecan\local\Projects\python\hydro_class\test1.shp'
sites = poly_shp = r'C:\ecan\local\Projects\requests\fouad\2017-04-03\vcs_border.shp'

iterables = [['flow', 'swl'], [69607, 70105]]
index = pd.MultiIndex.from_product(iterables, names=['mtype', 'site'])
df = pd.DataFrame(np.random.randn(10, 4), index=pd.date_range('2011-01-01', periods=10), columns=index)

df2a = pd.DataFrame(np.random.randn(20, 4), index=pd.date_range('2011-01-01', periods=20, freq='2H'), columns=index)

df3 = rd_ts(met_csv).iloc[:, :5]
df4 = df3.copy()
mtypes1 = ['U_z', 'precip', 'T_mean', 'T_max', 'T_min']
df4.columns = mtypes1
p1 = read_file(catch_shp)[['GRIDCODE', 'geometry']]
p1.rename(columns={'GRIDCODE': 'site'}, inplace=True)
p2 = p1.dissolve(by='site', aggfunc='sum')
p2.crs = p1.crs
df5 = rd_henry(sites=[70105, 70103])
df5['mtype'] = 'm_flow'
flow = rd_ts(flow_csv)

t3 = hydro(df4, sites=70103, dformat='wide')
t3 = t3.add_data(df2a, mtypes='mtype', sites='site', dformat='wide')
t3 = t3.add_data(df5, mtypes='mtype', time='date', sites='site', values='flow', dformat='long')
t3.get_geo_loc()
t3.add_geo_catch(p2)

t3.to_netcdf(nc_path)
t3.to_csv(exp_csv, mtypes=['flow', 'm_flow'])
t3.to_csv(exp_csv, mtypes=['flow', 'precip'], pivot=True)

t4 = hydro().rd_csv(exp_csv, mtypes='mtype', time='time', sites='site', values='data', dformat='long')

t3.sel_sites(mtypes=mtypes)
t3.sel_ts(mtypes=mtypes)
#t3.add_site_attr(df)
t3.get_site_attr()

t3.sel_sites_by_poly(catch_shp, buffer_dis=100)
t3.sel_ts_by_poly(catch_shp, buffer_dis=100, mtypes=['flow', 'swl'], pivot=True)

t4 = hydro().rd_netcdf(nc_path)

t3.sel_ts(mtypes=['flow', 'm_flow'])

flow1 = flow.stack().reset_index()
flow1.columns = ['time', 'site', 'data']
flow1['mtype'] = 'flow'

t5 = hydro(flow, mtypes='flow', dformat='wide')
t5 = hydro(flow1, mtypes='mtype', sites='site', values='data', time='time', dformat='long')

t5.get_geo_loc()
t5.add_geo_catch(p2)
t5.get_site_attr()
t5.sel_ts(mtypes='flow', sites=None, pivot=True)
t5.malf7d()
t5.stats('flow')
n1 = t5.sel_by_poly(catch_shp)
malf1 = t5.sel_by_poly(catch_shp).malf7d()
buff1 = t5._comp_by_buffer(30000)
res1 = t5.resample('month')


new_t3 = t3.sel(mtypes='flow')
t5['flow']
t6 = t5.sel('flow', [70105, 70103])

t5.to_netcdf(nc_path)
t8 = hydro().rd_netcdf(nc_path)
t5.to_csv(exp_csv, mtypes=['flow'])
t5.to_csv(exp_csv, mtypes=['flow'], pivot=True)

t9 = hydro().rd_csv(exp_csv, mtypes='mtype', time='time', sites='site', values='data', dformat='long')
t9 = hydro().rd_csv(exp_csv, mtypes='flow', time='time', dformat='wide')


t10 = hydro()._rd_hydro_mssql('SQL2012DEV01', 'Hydro', 'usage_data', ['K36/0911', 'BY20/0089', 'K36/1036', 'K36/1031'])
t10 = hydro()._rd_hydro_mssql('SQL2012DEV01', 'Hydro', 'flow_data', [69607, 70103, 70105])

t1 = hydro().get_data('flow', sites)
t1 = hydro().get_data('flow', [69607, 70103, 70105])
t2 = t1.get_data('usage', ['K36/0911', 'BY20/0089', 'K36/1036', 'K36/1031'])

sites = [69607, 70103, 70105]
mtypes = ['flow']
server = 'SQL2012DEV01'
database = 'Hydro'
table = 'flow_data'
table = 'site_geo_attr'

t1 = hydro().get_data(['flow'], sites)



h1 = t1._rd_hydro_mssql('SQL2012DEV01', 'Hydro', 'flow_data', sites=[66])

geo_dict = {'catch_grp': 701, 'cwms': 'Orari-Temuka-Opihi-Pareora'}



#############################################
#### Add details to recorder sites shp
from geopandas import read_file
from core.spatial import pts_sql_join
from core.ecan_io import flow_import

sites_shp = r'C:\ecan\shared\GIS_base\vector\hydro_sites\all_rec_loc.shp'
sites_export = r'C:\ecan\shared\GIS_base\vector\hydro_sites\all_rec_loc_details.shp'
sql_join_codes = ['swaz_gis', 'catch_gis', 'cwms_gis']

pts = read_file(sites_shp)
pts_stuff = pts_sql_join(pts, sql_join_codes)

pts_stuff.to_file(sites_export)

df = flow_import(gauge_sites=[70105, 69607])

############################################
#### Read hts files...maybe...

import struct
from pandas import to_datetime, Timedelta, DataFrame, concat, Series

fpath = r'C:\ecan\local\Projects\hilltop\brownsrock.hts'

with open(fpath, mode='rb') as file: # b is important -> binary
    fileContent = file.read()

struct.unpack("iiiii", fileContent[:20])
t1 = struct.unpack("i" * ((len(fileContent) -24) // 4), fileContent[20:-4])

d1 = to_datetime('2010-09-11 05:30')
d2 = to_datetime('2016-10-05 13:50')

d3 = d2 - d1

import numpy as np
import xml.etree.ElementTree as ET
from collections import defaultdict
from untangle import parse
from xmltodict import parse
from lxml import objectify       # very likely how the xml was crerated
from time import clock


fpath = r'C:\ecan\local\Projects\hilltop\test2.xml'
xml = r'C:\ecan\local\Projects\hilltop\test2.xml'
xml = r'C:\ecan\local\Projects\hilltop\xml_test\WaitakiDC.xml'

#root = etree.parse(xml)
#root = ET.parse(xml).getroot()

root = objectify.parse(xml).getroot()
root = o1.getroot()
t1 = root.getchildren()[1].getchildren()

t1 = root.getchildren()[1:]
[i.attrib for i in t1]

site = []
meas = []
data = []
for i in t1:
    site.append(i.values())
    meas.append(i.DataSource.values()[0])
    data.append([j for j in i.Data.itertext()])














#obj = parse(fpath, process_namespaces=True)
for child in root:
     print child.tag, child.attrib

for meas in root.iter('Datasource'):
     print meas.attrib

for child in root:
    print child.tag, child.attrib
    for meas in child:
        print meas.tag, meas.attrib

sites = []
source = []
for child in root:
    if 'SiteName' in child.attrib.keys():
        sites.append(child.attrib)
        for meas in child:
            source.append(meas.attrib)
            for data in meas:
                print(data.attrib)

mtypes = []
for i in root.iter('DataSource'):
    mtypes.append(i.get('Name'))

sites = []
for i in root.iter('Measurement'):
    sites.append(i.get('SiteName'))

units = []
for i in root.iter('DataSource'):
    units.append(i.find('ItemInfo').find('Units').text)

data = []
for i in root.iter('Data'):
    data.append([j.text for j in i.iter('V')])

data = []
for i in root.iter('Data'):
    data.append([j.text.split() for j in i.iter('V')])

d1 = {sites[i]: data[i] for i in range(len(sites))}

sub2 = 10958*24*60*60
o1 = concat(DataFrame(d1[i], index=[i] * len(d1[i])) for i in d1)
o1.columns = ['date', 'val']
o1.loc[:,['date', 'val']] = o1.loc[:,['date', 'val']].astype(float)
o1.loc[:, 'date'] = to_datetime(o1.loc[:, 'date'] - sub2, unit='s')

## Date time stuff

mow = 2380184400
sub2 = 10958*24*60*60
new = mow - sub2
sub3 = Timedelta('10958 days 00:00:00')
s1 = Timestamp('1940-01-01')
e1 = Timestamp('1970-01-01')
sub1 = Timedelta(30, unit='Y')
t1 = to_datetime(mow, unit='s')
#to_datetime(mow, infer_datetime_format=True)
t2 = to_datetime(new, unit='s')
t3 = to_datetime(mow, unit='s') - sub3
d1 = t1 - t2

x = np.arange(1326706251, 1326706260)
x = x * 1e6
x = x.astype(np.datetime64)
np.datetime64(2380184400,'s')

e1 - s1

"""
Hilltop uses a fixed base date as 1940-01-01, while the standard unix/POSIT base date is 1970-01-01.
"""

xml = r'C:\ecan\hilltop\xml_test\annual\Anonymous_Flow.xml'
xml = r'C:\ecan\hilltop\xml_test\annual\WaimateDC.xml'
xml = r'C:\ecan\hilltop\xml_test\annual\Silverfern.xml'

tic = clock()
output = parse_ht_xml(xml, all_data_fun)
toc = clock()
toc - tic


fpath = r'C:\ecan\hilltop\xml_test\tel'
export_name = 'tel.csv'

tick = clock()
r1 = iter_xml_dir(fpath, stats_fun=data_check_fun, export=True, export_name=export_name)
tock = clock()
tock - tick

t5 = output.loc[output.site == 'BX22/0006-M1', 'val']
t6 = t5.diff()
t5.plot()

t7 = t6['2016-06']
t7[t7 < 0] = nan
t8 = t7.interpolate('time', limit=10)

neg_index = t6 < 0
t6.loc[neg_index] = t5[neg_index]
t6.plot()

t6[t6 > 200]
t5['2016-02-29']


file_name = 'Silverfern.xml'
df5 = df4.loc[df4.file_name == file_name, ['wap', 'mtype']]
df5.columns = ['site', 'mtype']
select = df5



##########################################
#### read/write file types testing

from xarray import Dataset, open_dataset
from core.ecan_io import rd_sql
from pandas import to_datetime, DataFrame, read_csv
from numpy import save, load
from time import clock

npy = r'C:\ecan\local\temp\export\wus.npy'
nc = r'C:\ecan\local\temp\export\wus.nc'
csv = r'C:\ecan\local\temp\export\wus.csv'

## Functions


def df_to_nc(df, path):
    nc1 = Dataset.from_dataframe(df)
    nc1.to_netcdf(path)

def df_to_npy(df, path):
    np1 = df.to_records()
    save(path, np1)


## Data
wus = rd_sql(code='wus_day')
wus.loc[:, 'usage'] = wus.loc[:, 'usage'].round(2)
wus.set_index('date', inplace=True)
wus.loc[:, 'wap'] = wus.loc[:, 'wap'].str.upper()
wus.index = to_datetime(wus.index)

## Test

# to and from csv
tic = clock()
wus.to_csv(csv)
toc = clock()
csv_w_time = toc - tic

tic = clock()
wus = read_csv(csv)
toc = clock()
csv_r_time = toc - tic

# to and from npy
tic = clock()
df_to_npy(wus, npy)
toc = clock()
npy_w_time = toc - tic

tic = clock()
wus = DataFrame(load(npy))
toc = clock()
npy_r_time = toc - tic

# to and from netcdf

tic = clock()
df_to_nc(wus, nc)
toc = clock()
nc_w_time = toc - tic

tic = clock()
wus = open_dataset(nc).to_dataframe()
toc = clock()
nc_r_time = toc - tic


bike = 60 + 60 + 60 + 60
ex_class = 60
walk = 60 + 60 + 50 + 50 + 120 + 120 + 60 + 50 + 60 + 30 + 60 + 120 + 120 + 120 + 30 + 120 + 360 + 480 + 360 + 480 + 480 + 120 + 60 + 60

################################
#### Recoder check

from core.ecan_io import flow_import
from core.ts.sw import flow_stats, malf7d

flow_csv = 'S:/Surface Water/shared/base_data/flow/all_flow_rec_data.csv'

site = [64304]

t1 = flow_import(rec_sites=site)
t2 = read_csv(flow_csv)

flow_stats(t1)


##############################
#### hydrotel junk

from core.ecan_io import rd_hydrotel
from core.ts.plot import hydrograph_plot

date = '2017-01-25'

flow1 = rd_hydrotel([68801])
flow2 = flow1[date]

plt1 = hydrograph_plot(flow2, x_period='hour')



##########################
#### Squalarc

from core.ecan_io import rd_squalarc

sites = 'E:/ecan/local/Projects/otop/GIS/vector/min_flow/catch1.shp'
sites = ['SQ35872', 'J39/0109']
#sites = ['SQ36017']
mtypes = ['Water Temperature']
from_date = '2010-01-01'
to_date = '2016-01-01'

t1 = rd_squalarc(sites, mtypes)
t1 = rd_squalarc(sites)
t1 = rd_squalarc(sites, from_date=from_date, to_date=to_date)

def chunkify(lst,n):
    return([lst[i::n] for i in xrange(n)])

##########################
#### VCSN convert

from xarray import open_dataset
from pandas import concat
from os.path import join

nc_path = r'Y:\VirtualClimate\vcsn_precip_et_2016-06-06.nc'
export_path = r'Y:\VirtualClimate\VCN_precip_ET_2016-06-06_lst'

nc = open_dataset(nc_path)

sites = nc.site.to_pandas()

for i in sites:
    data1 = nc.sel(site=i)
    precip = data1.precip.to_dataframe()
    et = data1.ET.to_pandas()
    et.name = 'et'
    both1 = concat([precip, et], axis=1).reset_index()
    out1 = both1[['site', 'y', 'x', 'time', 'precip', 'et']]
    out1.loc[:, 'time'] = out1.time.dt.strftime('%Y%m%d')
    out1.to_csv(join(export_path, i + '.lst'), index=False, header=False)


##########################
### Coes ford usage

from pandas import read_hdf, read_csv

use_csv = 'C:/ecan/shared/base_data/usage/usage_daily.h5'
waps_csv = r'C:\ecan\local\Projects\requests\coes_ford\2017-03-10\waps.csv'
date = '2017-02'
export_csv = r'C:\ecan\local\Projects\requests\coes_ford\2017-03-10\waps_usage_v02.csv'

use1 = read_hdf(use_csv)
waps = read_csv(waps_csv).wap.values

use2 = use1[use1.wap.isin(waps)].set_index('date')
use3 = use2[date:].pivot(columns='wap', values='usage')[:-1]
use3.to_csv(export_csv)

############################
### Allocation info

from core.ecan_io import rd_sql

dates = rd_sql(code='crc_details_acc')[['crc', 'from_date', 'to_date']]
crc_wap = rd_sql(code='crc_wap_act_acc')
crc_gen = rd_sql(code='crc_gen_acc')
crc_use_type = rd_sql(code='crc_use_type_acc')
crc_acc = rd_sql('SQL2012PROD03', 'DataWarehouse', 'D_ACC_ActivityAttribute_TakeWaterCombined')

crc1 = 'CRC992347.2'

crc_wap[crc_wap.crc == crc1]

a1 = merge(crc_wap, crc_use_type, on=['crc', 'take_type'], how='left')

a2 = a1[a1.use_type.isnull()]
a2.to_csv(r'C:\ecan\local\Projects\requests\matt_smith\missing_use_types.csv', index=False)


crc2 = 'CRC146412'

crc_wap2[crc_wap2.crc == crc2]

m3 = crc_use_type[crc_use_type.duplicated(subset=['crc', 'take_type', 'allo_block', 'use_type'], keep=False)]
m3.sort_values(['crc'])
12096/10.0 * 365

mix = 'CRC135895'
sw1 = 'CRC147355'
sw2 = 'CRC155932'
sw3 = 'CRC155937'
gw1 = 'CRC981180.1'
sw3 = 'CRC160916'
sw4 = 'CRC010728.2'
gw2 = 'CRC982161.2'
sw5 = 'WTK880021'

crc_wap[crc_wap.crc == sw5]
crc_use_type[crc_use_type.crc == sw5]

c1 = crc_wap.groupby(['crc', 'take_type', 'allo_block'])['wap'].count()
b1 = crc_use_type.groupby(['crc', 'take_type', 'allo_block'])['use_type'].count()

a1 = concat([c1, b1], axis=1)
a2 = a1[a1.use_type > 1]
a3 = a2[a2.wap != a2.use_type]


s2 = crc_acc[crc_acc['Operation'] == 'Food / Drink Processing']
s2.to_csv(r'C:\ecan\local\Projects\requests\suz\2017-03-29\food_drink_processing.csv', index=False)



###########################################
#### mssql writing






##########################
### Slatwater Creek usage

from pandas import read_hdf, read_csv

use_csv = 'C:/ecan/shared/base_data/usage/usage_daily.h5'
waps = ['J39/0607', 'J39/0608', 'J39/0609']
date = '2011-01'
export_csv = r'C:\ecan\local\Projects\requests\saltwater_creek\2017-03-21\waps_usage.csv'

use1 = read_hdf(use_csv)

use2 = use1[use1.wap.isin(waps)].set_index('date')
use3 = use2[date:].pivot(columns='wap', values='usage')
use3.to_csv(export_csv)

############################
### Patrick usage

from pandas import read_hdf, read_csv

use_csv = 'C:/ecan/shared/base_data/usage/usage_daily.h5'
waps = ['K36/0911', 'BY20/0089', 'K36/1036', 'K36/1031']
date = '2001-01'
export_csv = r'C:\ecan\local\Projects\requests\patrick\2017-03-22\CRC169512_usage.csv'

use1 = read_hdf(use_csv)

use2 = use1[use1.wap.isin(waps)].set_index('date')
use3 = use2[date:].pivot(columns='wap', values='usage')
use3.to_csv(export_csv)

############################
### Fouad usage

from pandas import read_hdf, read_csv
from core.spatial import sel_sites_poly
from geopandas import read_file

use_csv = 'C:/ecan/shared/base_data/usage/usage_daily.h5'
#waps = ['K36/0911', 'BY20/0089', 'K36/1036', 'K36/1031']
shp = r'C:\ecan\local\Projects\requests\fouad\2017-03-24\New_Shapefile.shp'
export_csv = r'C:\ecan\local\Projects\requests\fouad\2017-03-24\fouad_usage.csv'

def rd_waps_geo():
    from core.ecan_io import rd_sql
    from core.spatial.vector import xy_to_gpd

    site_geo = rd_sql(code='waps_details')
    site_geo2 = xy_to_gpd(site_geo, ['wap'], 'NZTMX', 'NZTMY')
    site_geo2.loc[:, 'wap'] = to_numeric(site_geo2.loc[:, 'wap'], errors='ignore')
    return(site_geo2.set_index('wap'))

use1 = read_hdf(use_csv)
poly = read_file(shp)

waps_loc = rd_waps_geo()

waps = sel_sites_poly(waps_loc, poly)

use2 = use1[use1.wap.isin(waps.index.unique())].set_index('date')
use3 = use2.pivot(columns='wap', values='usage')[:-1]
use3.to_csv(export_csv)

############################
#### Fixing the sql function

from core.ecan_io import rd_sql

server = 'SQL2012PROD05'
database = 'Wells'
table = 'WELL_DETAILS'
col_names = ['WELL_NO', 'WELL_TYPE', 'Well_Status']
where_col = 'WELL_TYPE'
where_val = ['SA', 'BO']
where_col = {'WELL_TYPE': ['SA', 'BO'], 'Well_Status': ['AE']}
where_col = {'WELL_TYPE': ['SA', 'BO']}

t1 = rd_sql(server, database, table, col_names, where_col, where_val)


##########################
#### Low flow restrictions

from core.ecan_io import rd_sql

restr = rd_sql(code='lowflow_restr_day')
crc = rd_sql(code='lowflow_band_crc')
sites = rd_sql(code='lowflow_gauge')
restr2 = rd_sql(code='lowflow_restr')


##########################
#### Fouad request

from core.ecan_io import rd_vcn

### VCSN  data
select = poly_shp = r'C:\ecan\local\Projects\requests\fouad\2017-04-03\vcs_border.shp'
#nc_path = r'Y:\VirtualClimate\vcsn_precip_et_2016-06-06.nc'
export_vcn = r'C:\ecan\local\Projects\requests\fouad\2017-04-03\precip_data.csv'
export_et = r'C:\ecan\local\Projects\requests\fouad\2017-04-03\et_data.csv'

precip = rd_vcn(select, site_col=1, csv_export=True, export_path=export_vcn)
et = rd_vcn(select, site_col=1, data_type='ET', csv_export=True, export_path=export_et)

### Usage
from pandas import read_hdf, read_csv, Timestamp, to_datetime, Grouper
from core.spatial import sel_sites_poly
from geopandas import read_file
from core.ecan_io import rd_sql
from core.allo_use import allo_ts_proc

allo_csv = r'C:\ecan\shared\base_data\usage\allo_gis.csv'
use_csv = 'C:/ecan/shared/base_data/usage/usage_daily.h5'
#waps = ['K36/0911', 'BY20/0089', 'K36/1036', 'K36/1031']
zone = 'Selwyn - Waihora'
export_csv_daily = r'C:\ecan\local\Projects\requests\fouad\2017-04-03\selwyn_usage_daily.csv'
export_csv_yearly = r'C:\ecan\local\Projects\requests\fouad\2017-04-03\selwyn_usage_yearly.csv'
export_csv_daily_allo = r'C:\ecan\local\Projects\requests\fouad\2017-04-03\selwyn_allo_daily.csv'
export_csv_yearly_allo = r'C:\ecan\local\Projects\requests\fouad\2017-04-03\selwyn_allo_yearly.csv'

def rd_waps_geo():
    from core.ecan_io import rd_sql
    from core.spatial.vector import xy_to_gpd
    from pandas import to_numeric

    site_geo = rd_sql(code='waps_details')
    site_geo2 = xy_to_gpd(site_geo, ['wap'], 'NZTMX', 'NZTMY')
    site_geo2.loc[:, 'wap'] = to_numeric(site_geo2.loc[:, 'wap'], errors='ignore')
    return(site_geo2.set_index('wap'))

allo = read_csv(allo_csv)
cwms = rd_sql(code='cwms_gis')
poly = cwms[cwms.cwms == zone]
use1 = read_hdf(use_csv)
#poly = read_file(shp)
waps_loc = rd_waps_geo()

waps = sel_sites_poly(waps_loc, poly)

allo1 = allo[allo.cwms == zone]
allo2 = allo1[allo1.status_details.isin(['Terminated - Replaced', 'Issued - Active', 'Terminated - Surrendered', 'Terminated - Expired', 'Terminated - Annulled', 'Terminated - Lapsed', 'Issued - s124 Continuance', 'Terminated - Cancelled'])]
allo3 = allo2[(allo2.daily_vol.notnull()) & allo2.from_date.notnull() & allo2.to_date.notnull() & allo2.wap.notnull()]
allo3 = allo3[(to_datetime(allo3.to_date) - to_datetime(allo3.from_date)).dt.days > 31]
allo3 = allo3[to_datetime(allo3.from_date) > '2000-01-01']

use2 = use1[use1.wap.isin(allo3.wap.unique())].set_index('date')
use3 = use2.pivot(columns='wap', values='usage')[:-1]
use3.to_csv(export_csv_daily)
use4 = use3.resample('A').sum()
use4.to_csv(export_csv_yearly)

allo3.set_index(['crc', 'take_type', 'allo_block', 'wap'], inplace=True)
allo_day = allo3.apply(lambda x: allo_ts_proc(x, start_date='2000-01-01', end_date='2017-12-31', freq='D'), axis=1)
allo_day.columns.name = 'time'
allo_day.name = 'allo_day'

allo_day1 = allo_day.stack()
allo_day1.name = 'allo_day'
allo_day2 = allo_day1.reset_index()

wap_allo_day = allo_day2.groupby(['wap', 'time']).sum()
wap_allo_day1 = wap_allo_day.unstack('wap')
wap_allo_day1.to_csv(export_csv_daily_allo)


allo_mon = allo3.apply(lambda x: allo_ts_proc(x, start_date='2000-01-01', end_date='2017-12-31', freq='M'), axis=1)
allo_mon.columns.name = 'time'
allo_mon.name = 'allo_mon'

allo_mon1 = allo_mon.stack()
allo_mon1.name = 'allo_mon'
allo_mon2 = allo_mon1.reset_index()

wap_allo_mon = allo_mon2.groupby(['wap', 'time']).sum()

wap_allo_yr = wap_allo_mon.groupby([Grouper(level='wap'), Grouper(level='time', freq='A')]).sum()
wap_allo_yr1 = wap_allo_yr.unstack('wap')
wap_allo_yr1.to_csv(export_csv_yearly_allo)




a1 = ('CRC030633', 'Take Groundwater', 'A', 'M36/4973')
allo3.loc[a1].reset_index().loc[0]


allo2[allo2.wap == 'M37/0540']





####################################
#### SQL import testing

from pandas import read_csv, to_datetime
from core.ecan_io import rd_ts, rd_sql

flow_csv = r'S:\Surface Water\shared\base_data\flow\all_flow_rec_data.csv'
flow_csv_long = r'S:\Surface Water\shared\base_data\flow\all_flow_rec_data_long.csv'
usage_csv = r'S:\Surface Water\shared\base_data\usage\use_daily_all_waps.csv'

flow1 = rd_ts(flow_csv)
use1 = read_csv(usage_csv)

## Fix potential date issue
use1.loc[:, 'time'] = to_datetime(use1.loc[:, 'time'])
use2 = use1[use1.time.notnull()].dropna()
use2.loc[:, 'site'] = use2.site.str.replace('"', '')
use2.loc[:, 'site'] = use2.site.str.replace(',', '')

use2.to_csv(r'S:\Surface Water\shared\base_data\usage\usage_data.csv', index=False)

## Reform
flow2 = flow1.stack().reset_index()

flow2['mtype'] = 'flow'
flow2.columns = ['time', 'site', 'data', 'mtype']

flow3 = flow2[['mtype', 'site', 'time', 'data']].sort_values(['mtype', 'site', 'time'])
flow3.to_csv(flow_csv_long, index=False)

### Read the imported data

server = 'SQL2012DEV01'
database = 'Hydro'
table = 'hydro_data'
mtype = ['flow']
sites = [69607, 70105]
where_dict = {'mtype': mtype, 'site': sites}


t1 = rd_sql(server1, db1, table, where_col=where_dict)


#####################################

from core.classes.hydro import hydro

mtype = 'flow'
site = 69635

h1 = hydro().get_data(mtypes=mtype, sites=site)

h1.malf7d()

########################################
#### Opihi

from pandas import read_csv, to_datetime

allo_use_csv = r'C:\ecan\shared\base_data\usage\sd_est_all_mon_vol.csv'
waps_csv = r'C:\ecan\local\Projects\requests\jen\2017-04-06\crc_opihi.csv'
crc_csv = r'C:\ecan\local\Projects\requests\jen\2017-04-06\crc_opihi1.csv'
export = r'C:\ecan\local\Projects\requests\jen\2017-04-06\allo_use_data.csv'
export_rate = r'C:\ecan\local\Projects\requests\jen\2017-04-06\allo_use_rate_data.csv'

allo_use = read_csv(allo_use_csv)
waps = read_csv(waps_csv)
crcs = read_csv(crc_csv)

allo_use1 = allo_use[allo_use.crc.isin(crcs.crc)]

allo_use1.to_csv(export, index=False)

crcs.crc[~crcs.crc.isin(allo_use.crc.unique())]

allo1 = read_csv(export)
allo1.loc[:, 'dates'] = to_datetime(allo1.loc[:, 'dates'])
allo1.columns = ['crc', 'dates', 'take_type', 'use_type', 'mon_allo_vol', 'mon_restr_allo_vol', 'ann_restr_allo_vol', 'mon_usage_est_vol', 'mon_sd_est_vol']
allo2 = allo1.drop('ann_restr_allo_vol', axis=1)
allo2.set_index(['crc', 'dates', 'take_type', 'use_type'], inplace=True)

days = allo1.dates.dt.day
days.index = allo2.index

allo_rate = allo2.div((days * 24*60*60.0) / 1000, axis=0).round(3)
allo_rate.columns = allo_rate.columns.str.replace('vol', 'rate')

allo_rate.to_csv(export_rate)


################################################
#### Allo testing

from pandas import read_csv
from core.allo_use import allo_proc, allo_ts_proc

allo_export_path = 'S:/Surface Water/shared/base_data/usage/allo.csv'
start = '2010-07-01'
end = '2017-06-30'


allo = read_csv(allo_export_path)

#allo1 = allo_filter(allo, start='2000', end='2016-06-30')
#
#allo2 = allo1.apply(lambda x: allo_ts_proc(x, start_date='2000', end_date='2016-06-30', freq='M'), axis=1)

allo_ts = allo_ts_proc(allo, start, end, freq='M')

##############################################
#### Request from Matt Dodson

from pandas import read_csv, read_hdf, merge, concat
from core.ecan_io import rd_sql

use_h5 = 'E:/ecan/shared/base_data/usage/usage_daily.h5'
allo_csv = r'E:\ecan\shared\base_data\usage\allo_gis.csv'
sd_code = 'sd'
waps_code = 'waps_details'
zone = 'Waimakariri'
start = '2014-07-01'
end = '2015-06-30'
active = ['Issued - Active', 'Issued - s124 Continuance', 'Application in Process',
 'Issued - Inactive']

export_csv = r'E:\ecan\local\Projects\requests\matt_dodson\2017-04-11\waimak_allo_use3.csv'

### Read in data

allo = read_csv(allo_csv)
use1 = read_hdf(use_h5)
sd = rd_sql(code=sd_code)[['wap', 'dist1', 'sd1_7', 'sd1_30', 'sd1_150']]
waps = rd_sql(code=waps_code)[['wap', 'NZTMX', 'NZTMY']]

### Select date ranges and aggregate by year
use2 = use1[(use1.date >= start) & (use1.date <= end) & (use1.usage.notnull())]
use3 = use2.groupby('wap').sum()

### Select active consents in the zone
allo1 = allo[allo.cwms == zone]
allo2 = allo1[allo1.status_details.isin(active)]

### Inner join of consents to sd
allo3 = merge(allo2, sd, on='wap')

### Join the usage data
count1 = allo3.groupby('wap')['crc'].count()
use4 = concat([count1, use3], axis=1, join='inner')
use5 = (use4.usage/use4.crc).round(1).reset_index()
use5.columns = ['wap', 'usage']

allo4 = merge(allo3, use5, on='wap', how='left')
allo4['usage_ratio'] = (allo4.usage/allo4.cav).round(2)

### Join the x and y data
allo5 = merge(allo4, waps, on='wap', how='left')

### Export
allo5.to_csv(export_csv, index=False)


######################################################
#### 69618 Rockwood correlations to upstream Opihi

from core.classes.hydro import hydro

### Parameters

mtypes1 = 'flow'
mtypes2 = 'flow_m'
y = [69607, 69615, 69616]
x = [69618]
all_sites = [69615, 69616, 69618]

### From the MSSQL server (the easy way) - Loads in both the time series data and the geo locations

h1 = hydro().get_data(mtypes=mtypes1, sites=x)
h2 = h1.get_data(mtypes=mtypes2, sites=y)

### Add both upper sites together

q1 = h2.sel_ts(sites=[69616, 69615], pivot=True)
q2 = (q1[69616] + q1[69615]).dropna()
q2.name = 69617
q3 = DataFrame(q2)
h2 = h2.add_data(q3, mtypes='flow_m', dformat='wide')
h2.get_geo_loc()
y.append(69617)

### Filter dates

h3 = h2.sel(end='1998-12-31')
h4 = h2.sel(start='1999-01-01')

### Regressions

new1, reg1 = h2.flow_reg(y, x, y_mtype='flow_m', x_mtype='flow', min_yrs=1)
new2, reg2 = h3.flow_reg(y, x, y_mtype='flow_m', x_mtype='flow', min_yrs=1)
new3, reg3 = h4.flow_reg(y, x, y_mtype='flow_m', x_mtype='flow', min_yrs=1)


#### Flow comparisons

f1 = hydro().get_data(mtypes=mtypes1, sites=all_sites)

f2 = f1.sel_ts(pivot=True).dropna().interpolate()

### Specific date ranges
start1 = '1999-01-01'
end1 = '1999-06-01'
start2 = '2007-06-01'
end2 = '2007-12-01'


f3 = f2[start1:end1]
f3.plot()

f4 = f2[start2:end2]
f4.plot()


##############################################
#### Hydstra export improvement

from core.ecan_io import rd_sql

### Parameters
server = 'SQL2012PROD03'
db = 'Hydstra'
period_tab = 'PERIOD'
var_tab = 'VARIABLE'
site_tab = 'SITE'
qual_tab = 'QUALITY'

period_cols = ['STATION', 'DATASOURCE', 'VARFROM', 'VARIABLE', 'PERSTART', 'PEREND']
period_names = ['site', 'datasource', 'varfrom', 'varto', 'start', 'end']
var_cols = ['VARNUM', 'VARNAM', 'VARUNIT', 'SHORTNAME']
var_names = ['var_num', 'var_name', 'var_unit', 'var_short_name']
site_cols = ['STATION', 'STNAME', 'SHORTNAME']
site_names = ['site', 'site_name', 'site_short_name']
qual_cols = ['QUALITY', 'TEXT']
qual_names = ['qual_code', 'qual_name']

### Import
period1 = rd_sql(server, db, period_tab, period_cols, where_col='DATASOURCE', where_val=['A'])
period1.columns = period_names

var1 = rd_sql(server, db, var_tab, var_cols)
var1.columns = var_names


###################################################
#### Low flows joining

allo_use_export = 'E:/ecan/shared/base_data/usage/allo_use_ts_mon_results.csv'

restr = rd_sql(code='lowflow_restr_day')
restr_cond = rd_sql(code='lowflow_restr')
crc = rd_sql(code='lowflow_band_crc').drop('active', axis=1)
sites = rd_sql(code='lowflow_gauge').drop(['active', 'DB'], axis=1)

allo_use1 = read_csv(allo_use_export)

c1 = crc.drop_duplicates()
c1[c1.duplicated(subset=['lowflow_id', 'crc'], keep=False)]

a1 = allo_use1[['crc', 'take_type', 'allo_block']].drop_duplicates()
a1[a1.crc.duplicated(keep=False)]


restr_cond[restr_cond.lowflow_id == 232]






sites = [65901]
qual_codes = [10, 20, 30]

server = 'SQL2012PROD03'
database = 'DataWarehouse'
table = 'F_HY_Flow_Data'
mtype = 'flow'
time_col = 'DateTime'
site_col = 'SiteNo'
data_col = 'Value'
qual_col = 'QualityCode'
from_date='2000-01-01'
to_date='2010-01-01'

##############################################3
#### Hydro testing

from core.classes.hydro import hydro

sites1 = ['BW22/0008', 'BW22/0009', 'L35/0888', 'L35/0716', 'BW22/0070', 'L35/1048', 'L35/0697', 'L35/0565', 'L35/0951', 'L35/0744', 'BW22/0063', 'L35/0721', 'L35/0783', 'L35/1061']

h1 = hydro().get_data(mtypes='usage', sites=sites1)


##############################################
#### Figure out how to make a netcdf GIS worthy

from xarray import open_dataset, Dataset, DataArray
from core.spatial.vector import convert_crs

ex_nc = r'E:\ecan\shared\base_data\metservice\testing\TotalPrecipCorr_VCSN_BCC-CSM1.1_RCPpast_1971_2005_south-island_p05_daily_ECan.nc'

save1 = r'E:\ecan\shared\base_data\metservice\testing\niwa_test1.nc'

ds1 = open_dataset(ex_nc)
ds1
ds1['rain']
ds1['latitude']
ds1['longitude']
ds1['time']

wgs84 = {'grid_mapping_name': "latitude_longitude", 'longitude_of_prime_meridian': 0, 'semi_major_axis': 6378137, 'inverse_flattening': 298.257223563}

ds2 = ds1.copy()

#time_attrs = ds2.coords['time'].attrs
#time_attrs.update({'units': 'days since 1971-1-1 0:0:0'})
#ds2.coords['time'].attrs = {'units': 'seconds since 1970-1-1 0:0:0', 'long_name': 'time'}
ds3 = ds2.drop(['elevation', 'time_bounds', 'x_index', 'y_index'])
crs4 = {'grid_mapping': 'crs'}
rain_attrs = ds3['rain'].attrs
rain_attrs.update(crs4)
ds3['rain'].attrs = rain_attrs
crs8 = convert_crs(4326, 'netcdf_dict')
ds_crs = DataArray(4326, attrs=crs8, name='crs').to_dataset()

ds5 = ds3.merge(ds_crs)
ds5.to_netcdf(save1)


ds6 = open_dataset(save1)

###############################################
#### Jen usage data request

from core.classes.hydro import hydro

sites4 = ['J38/0774', 'J38/0874', 'J38/0811', 'I39/0033']
mtypes6 = 'usage'
export = r'E:\ecan\local\Projects\requests\jen\2017-05-29\waps_use3.csv'

use1 = hydro().get_data(mtypes=mtypes6, sites=sites4)
use1.to_csv(export, pivot=True)

use2 = use1.sel_ts(sites='I39/0033', pivot=True)
use2.loc['2014-10-30']



##################################################
#### NIWA data extraction and reformating

from xarray import open_dataset, open_mfdataset
from os import path, walk, makedirs
from core.misc import rd_dir
from numpy import in1d
from pandas import read_csv, merge
from core.spatial import xy_to_gpd, sel_sites_poly
from geopandas import read_file
from fnmatch import filter as fil

mtype_name = {'precip': 'TotalPrecipCorr', 'T_max': 'MaxTempCorr', 'T_min': 'MinTempCorr', 'P_atmos': 'MSLP', 'PET': 'PE', 'RH_mean': 'RelHum', 'R_s': 'SurfRad', 'U_z': 'WindSpeed'}
mtype_param = {'precip': 'rain', 'T_max': 'tmax', 'T_min': 'tmin', 'P_atmos': 'mslp', 'PET': 'pe', 'RH_mean': 'rh', 'R_s': 'srad', 'U_z': 'wind'}

vcsn_sites_csv = r'Z:\Data\VirtualClimate\GIS\niwa_vcsn_wgs84.csv'
id_col = 'Network'
x_col = 'deg_x'
y_col = 'deg_y'

param = ['precip', 'PET']
mtype = 'precip'
poly = r'E:\ecan\local\Projects\requests\zeb\2017-06-01\FinalModelDomain.shp'

base_path = r'I:\niwa_data\climate_projections'
out_path = r'Z:\Data\VirtualClimate\NIWA_RCP\climate\waimak'
test_path1 = r'I:\niwa_data\climate_projections\RCP2.6\BCC-CSM1.1\TotalPrecipCorr_VCSN_BCC-CSM1.1_RCP2.6_2006_2120_south-island_p05_daily_ECan.nc'
test_path2 = r'I:\niwa_data\climate_projections\RCP2.6\BCC-CSM1.1\PE_VCSN_BCC-CSM1.1_RCP2.6_2006_2100_south-island_p05_daily_ECan.nc'

### Test with new functions

from core.ecan_io.met import proc_niwa_rcp, export_rcp_lst

mtypes = ['precip', 'PET']
poly = r'E:\ecan\local\Projects\requests\zeb\2017-06-01\FinalModelDomain.shp'
base_path = r'I:\niwa_data\climate_projections'
export_path = r'Z:\Data\VirtualClimate\NIWA_RCP\climate\waimak'

proc_niwa_rcp(base_path, mtypes, poly, output_fun=export_rcp_lst, export_path=export_path)


##########################
#### Squalarc detection limits

from core.ecan_io import rd_squalarc
from pandas import to_numeric
from numpy import nan

sites = 'E:/ecan/local/Projects/otop/GIS/vector/min_flow/catch1.shp'
sites = ['SQ35872', 'J39/0109']
#sites = ['SQ36017']
from_date = '2010-01-05'
to_date = '2015-06-02'
mtypes = ['Water Temperature']

#t1 = rd_squalarc(sites, mtypes)
t1 = rd_squalarc(sites)
#t1 = rd_squalarc(sites, from_date=from_date, to_date=to_date)
t1 = rd_squalarc(sites, from_date=from_date, to_date=to_date, dtl_method='trend')

########################
#### Improving the MALF function

from core.classes.hydro import hydro
from scipy.interpolate import splrep, splev
from pandas import to_datetime

mtypes1 = 'flow'
sites1 = [70105, 69607, 69602, 65101, 69505]
qual_codes = [10, 18, 20, 30]

h1 = hydro().get_data(mtypes=mtypes1, sites=sites1, qual_codes=qual_codes)

df1 = h1.sel_ts(pivot=True)
x = df1.copy()


d1 = df2.loc['1994-07-01':'1995-06-30', 65101]
d2 = df2.loc['1983-07-01':'1984-06-30', 65101]
d3 = df2.loc[:, 65101].values

d4 = df2.interpolate(method='spline', order=2)
d5 = d4.loc['1994-07-01':'1995-06-30', 65101]

dayofyear1 = min_date.apply(lambda x: x.dt.dayofyear)

def day_june(df, dayofyear=182):
    day1 = df.dt.dayofyear
    over1 = day1 >= dayofyear
    under1 = day1 < dayofyear
    day2 = day1.copy()
    day2.loc[over1] = day1.loc[over1] - dayofyear
    day2.loc[under1] = 365 - dayofyear + day1.loc[under1]
    return(day2)

dayofyear1 = min_date.apply(day_june)
dayofyear1.mean()
dayofyear1.std()

series = df2.loc['1994-07-01':'1995-06-30', 65101]

d10 = df2.loc['1975-07-01':'1976-06-30', 65101]
d10 = df2.loc['2008-07-01':'2009-06-30', 65101]
d10 = df2.loc['2001-07-01':'2002-06-30', 70105]
d10 = df2.loc['1986-07-01':'1987-06-30', 70105]

d10.plot()


from core.classes.hydro import hydro

mtypes1 = 'flow'
sites1 = [70105, 69607, 69602, 65101, 69505]
qual_codes = [10, 18, 20, 30]

h1 = hydro().get_data(mtypes=mtypes1, sites=sites1, qual_codes=qual_codes)

malf = h1.malf7d()
malf, alf, alf_mis, alf_min_mis, min_date_alf = h1.malf7d(return_alfs=True)

start = '1986-07-01'
end = '1987-06-30'
flow_sites = 70105

h1.plot_hydrograph(flow_sites=flow_sites, x_period='month', time_format='%d-%m-%Y', start=start, end=end)






###############################################
#### Catchment numbering uniqueness

from geopandas import read_file

shp1 = r'P:\Surface Water Quantity\New surface water catchments\Surface_water_catchments_copy.shp'

g1 = read_file(shp1)

g1.loc[g1['OldHydrolo'].duplicated(), 'OldHydrolo'].sort_values()
g1.loc[g1['OldHydrolo'].isnull(), 'OldHydrolo']


##############################################
#### Graham MALF request

from core.classes.hydro import hydro

mtypes1 = 'flow'
sites3 = [68002, 68001]
qual_codes = [10, 18, 20, 30]

base_path = r'E:\ecan\local\Projects\requests\graham\2017-06-07'
malf_path = '68001_68002_malf.csv'
alf_path = '68001_68002_alf.csv'
alf_mis_path = '68001_68002_alf_missing_days.csv'

h1 = hydro().get_data(mtypes=mtypes1, sites=sites3, qual_codes=qual_codes)
malf, alf, alf_mis, alf_min_mis, min_date_alf = h1.malf7d(return_alfs=True, export_name_malf=malf_path, export_name_alf=alf_path, export_name_mis=alf_mis_path, export_path=base_path, export=True)

d10 = h1.sel_ts(sites=68002, pivot=True, start='1996-07-01', end='1997-06-30')

d10.plot()

x = h1.sel_ts(pivot=True)


###########################################
#### Stony River?

from core.classes.hydro import hydro

mtypes1 = 'flow'
mtypes2 = 'usage'
sites1 = 2280
sites2 = ['I39/0033']

qual_codes = [10, 18, 20, 30]


h1 = hydro().get_data(mtypes=mtypes1, sites=sites1, qual_codes=qual_codes)
h1 = hydro().get_data(mtypes=mtypes2, sites=sites2)


###########################################
#### Marble point

from core.allo_use import restr_days

export1 = r'E:\ecan\local\Projects\requests\suz\2017-06-12\daily_restr_mon.csv'
sites = [64602, 64609, 65104, 65101]

rd1 = restr_days(sites, period='M', export=True, export_path=export1)

##########################################
#### VCSN rearrangement

from xarray import open_dataset, DataArray
from core.spatial.vector import convert_crs

nc = r'Z:\Data\VirtualClimate\vcsn_precip_et_2016-06-06.nc'
x_coord = 'x'
y_coord = 'y'




nc_crs = {'inverse_flattening': 298.25722356299997, 'longitude_of_prime_meridian': 0, 'semi_major_axis': 6378137, 'transform_name': 'latitude_longitude'}

ds_crs = DataArray(4326, attrs=nc_crs, name='crs').to_dataset()

crs4 = {'grid_mapping': 'crs'}
rain_attrs = ds3['rain'].attrs
rain_attrs.update(crs4)

ds1 = open_dataset(nc)

df1 = ds1.to_dataframe()
df2 = df1.reset_index()

sites_df1 = df2[['x', 'y', 'site']].drop_duplicates().sort_values(['x', 'y']).set_index(['x', 'y'])

data_df1 = df2[['x', 'y', 'time', 'precip', 'ET']].sort_values(['x', 'y', 'time'])
data_df2 = data_df1.set_index(['x', 'y', 'time'])

p1 = data_df2.precip

sites_xa = sites_df1.to_xarray()


##################################################
### Bokeh testing

from bokeh.plotting import figure, output_file, show
from os.path import join

base_dir = r'S:\Surface Water\backups\MichaelE\Projects\plotting\bokeh'
base_dir = r'E:\ecan\local\Projects\plotting\bokeh'
html1 = 'lines.html'
html2 = 'log_lines.html'
html3 = 'color_scatter.html'
html4 = "linked_panning.html"
html5 = "linked_brushing.html"
html6 = "stocks.html"
svg6 = "stocks.svg"
png6 = 'stocks.png'

# prepare some data
x = [1, 2, 3, 4, 5]
y = [6, 7, 2, 4, 5]

# output to static HTML file
output_file(join(base_dir, html1))

# create a new plot with a title and axis labels
p = figure(title="simple line example", x_axis_label='x', y_axis_label='y', logo=None)

# add a line renderer with legend and line thickness
p.line(x, y, legend="Temp.", line_width=2)

# show the results
show(p)


# prepare some data
x = [0.1, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
y0 = [i**2 for i in x]
y1 = [10**i for i in x]
y2 = [10**(i**2) for i in x]

# output to static HTML file
output_file(join(base_dir, html2))

# create a new plot
p = figure(
   tools="pan,box_zoom,reset,save",
   y_axis_type="log", y_range=[0.001, 10**11], title="log axis example",
   x_axis_label='sections', y_axis_label='particles'
)

# add some renderers
p.line(x, x, legend="y=x")
p.circle(x, x, legend="y=x", fill_color="white", size=8)
p.line(x, y0, legend="y=x^2", line_width=3)
p.line(x, y1, legend="y=10^x", line_color="red")
p.circle(x, y1, legend="y=10^x", fill_color="red", line_color="red", size=6)
p.line(x, y2, legend="y=10^x^2", line_color="orange", line_dash="4 4")

# show the results
show(p)


import numpy as np
from bokeh.plotting import figure, output_file, show

# prepare some data
N = 4000
x = np.random.random(size=N) * 100
y = np.random.random(size=N) * 100
radii = np.random.random(size=N) * 1.5
colors = ["#%02x%02x%02x" % (int(r), int(g), 150) for r, g in zip(50+2*x, 30+2*y)]

# output to static HTML file (with CDN resources)
output_file(join(base_dir, html3), title="color_scatter.py example", mode="cdn")

TOOLS="resize,crosshair,pan,wheel_zoom,box_zoom,reset,box_select,lasso_select"

# create a new plot with the tools above, and explicit ranges
p = figure(tools=TOOLS, x_range=(0,100), y_range=(0,100))

# add a circle renderer with vectorized colors and sizes
p.circle(x,y, radius=radii, fill_color=colors, fill_alpha=0.6, line_color=None)

# show the results
show(p)


########
import numpy as np

from bokeh.layouts import gridplot
from bokeh.plotting import figure, output_file, show

# prepare some data
N = 100
x = np.linspace(0, 4*np.pi, N)
y0 = np.sin(x)
y1 = np.cos(x)
y2 = np.sin(x) + np.cos(x)

# output to static HTML file
output_file(join(base_dir, html4))

# create a new plot
s1 = figure(width=250, plot_height=250, title=None)
s1.circle(x, y0, size=10, color="navy", alpha=0.5)

# NEW: create a new plot and share both ranges
s2 = figure(width=250, height=250, x_range=s1.x_range, y_range=s1.y_range, title=None)
s2.triangle(x, y1, size=10, color="firebrick", alpha=0.5)

# NEW: create a new plot and share only one range
s3 = figure(width=250, height=250, x_range=s1.x_range, title=None)
s3.square(x, y2, size=10, color="olive", alpha=0.5)

# NEW: put the subplots in a gridplot
p = gridplot([[s1, s2, s3]], toolbar_location=None)

# show the results
show(p)


##############
import numpy as np
from bokeh.plotting import *
from bokeh.models import ColumnDataSource

# prepare some date
N = 300
x = np.linspace(0, 4*np.pi, N)
y0 = np.sin(x)
y1 = np.cos(x)

# output to static HTML file
output_file(join(base_dir, html5))

# NEW: create a column data source for the plots to share
source = ColumnDataSource(data=dict(x=x, y0=y0, y1=y1))

TOOLS = "pan,wheel_zoom,box_zoom,reset,save,box_select,lasso_select"

# create a new plot and add a renderer
left = figure(tools=TOOLS, width=350, height=350, title=None)
left.circle('x', 'y0', source=source)

# create another new plot and add a renderer
right = figure(tools=TOOLS, width=350, height=350, title=None)
right.circle('x', 'y1', source=source)

# put the subplots in a gridplot
p = gridplot([[left, right]])

# show the results
show(p)


###################
import numpy as np

from bokeh.plotting import figure, output_file, show
from bokeh.sampledata.stocks import AAPL
from scipy.signal import fftconvolve
from bokeh.io import export_svgs, export_png

# prepare some data
aapl = np.array(AAPL['adj_close'])
aapl_dates = np.array(AAPL['date'], dtype=np.datetime64)

window_size = 10
window = np.ones(window_size)/float(window_size)
aapl_avg = fftconvolve(aapl, window, 'same')

# output to static HTML file
output_file(join(base_dir, html6), title="stocks.py example")

# create a new plot with a a datetime axis type
p = figure(width=1800, height=800, x_axis_type="datetime")

# add renderers
p.circle(aapl_dates, aapl, size=4, color='darkgrey', alpha=0.2, legend='close')
p.line(aapl_dates, aapl_avg, color='navy', legend='avg')

# NEW: customize by setting attributes
p.title.text = "AAPL One-Month Average"
p.legend.location = "top_left"
p.grid.grid_line_alpha=0
p.xaxis.axis_label = 'Date'
p.yaxis.axis_label = 'Price'
p.ygrid.band_fill_color="olive"
p.ygrid.band_fill_alpha = 0.1

# show the results
show(p)
p.output_backend = 'svg'
export_svgs(p, filename=join(base_dir, svg6))
export_png(p, filename=join(base_dir, png6))


###################################################
#### ftplib testing

from ftplib import FTP
from os import path

### Parameters
dir1 = r'C:\ecan\ftp\metservice\netcdf_combined'

ftp_site = 'ftp1.met.co.nz'
username = 'wrf_rainfall_time_series'
password = 'WrfR@1nF@ll'

### Read local files


### Read ftp files
ftp1 = FTP(ftp_site)
ftp1.login(user=username, passwd=password)
ftp1.cwd('/netcdf_combined')
files1 = ftp1.nlst()

####################################################
### Export rainfall sites for metservice

from core.ecan_io import rd_sql

server = 'SQL2012PROD03'
db = 'MetConnect'
table = 'RainFallPredictionSites'

out_csv = r'E:\ecan\shared\base_data\metservice\metservice_sites.csv'

rsites = rd_sql(server, db, table)
rsites.to_csv(out_csv, index=False)


#################################################
#### Interpolation testing

from core.ts.met.metservice import proc_metservice_nc, MetS_nc_to_df
from core.ts.met.interp import poly_interp_agg
from geopandas import read_file
from pandas import to_datetime, concat
import numpy as np
from scipy.interpolate import griddata
from core.spatial import sel_sites_poly, point_interp_ts


### Parameters

nc = r'E:\ecan\shared\base_data\metservice\testing\nz8kmN-NCEP_2_2017050918.00.nc'
point_shp = r'E:\ecan\shared\base_data\metservice\catch_del\MetConnect_rf_sites.shp'
poly = r'E:\ecan\shared\base_data\metservice\testing\study_area.shp'

time_col = 'time'
y_col = 'y'
x_col = 'x'
data_col = 'precip'
digits = 2
from_crs=None
to_crs=None
interp_fun='cubic'
agg_ts_fun=None
period=None
point_site_col = 'SITENUMBER'
grid_res = 1000
to_crs=2193

test1 = np.array([156914, 5210595])

## preprocess the nc file to save it as a proper nc file
new_nc = proc_metservice_nc(nc)

## extract the data from the new nc file
precip, sites, start_date = MetS_nc_to_df(new_nc)

df = precip.copy()

from_crs = sites.crs

poly1 = read_file(poly).to_crs(from_crs)

sites2 = sel_sites_poly(sites, poly1, 10000)

df = precip.loc[precip.site.isin(sites2.site)]

t = df.groupby('time')['precip'].sum().idxmax()

output1 = grid_interp_ts(df, time_col, x_col, y_col, data_col, grid_res, from_crs, to_crs, interp_fun='cubic', agg_ts_fun=None, period=None, digits=digits)

t1 = output1.groupby('time')['precip'].sum().idxmax()


new_point_df = point_interp_ts(df, time_col, x_col, y_col, data_col, point_shp, point_site_col, from_crs, to_crs, interp_fun, agg_ts_fun=None, period=None, digits=2)



def func(x, y):
    return x*(1-x)*np.cos(4*np.pi*x) * np.sin(4*np.pi*y**2)**2


grid_x, grid_y = np.mgrid[0:1:100j, 0:1:200j]

points = np.random.rand(1000, 2)
values = func(points[:,0], points[:,1])

grid_z2 = griddata(points1, values, (grid_x, grid_y), method='cubic')

import matplotlib.pyplot as plt
plt.imshow(grid_z2.T, extent=(0,1,0,1), origin='lower')
plt.title('Cubic')
plt.gcf().set_size_inches(6, 6)
plt.show()

x1 = np.array([0.2, 0.4, 0.5, 0.7, 0.7, 0.8])
y1 = np.array([0.3, 0.4, 0.35, 0.35, 0.7, 0.8])

grid_z3 = griddata(points, values, (x1, y1), method='cubic')


xy = np.column_stack((x, y))
z = set1.values

x_new = points.geometry.apply(lambda p: p.x).round(digits).values
y_new = points.geometry.apply(lambda p: p.y).round(digits).values
xy_new = np.column_stack((x_new, y_new))

grid_z4 = griddata(xy, z, xy_new, method='cubic').round(2)


set2 = df2.loc[df2[time_col] == t, ['x', 'y', 'precip']]

gpd5 = xy_to_gpd('precip', 'x', 'y', set2, from_crs)

gpd5.to_file(r'E:\ecan\shared\base_data\metservice\catch_del\test1.shp')

save_geotiff(set2, 'precip', from_crs, x_col='x', y_col='y', time_col=None, export_path=r'E:\ecan\shared\base_data\metservice\catch_del\test2.tif', grid_res=8000)

from time import time

interp1 = Rbf(x, y, z)
z_int = interp1(x_int, y_int)

s1 = time()
z6 = grid_resample(x, y, z, x_int, y_int, digits=2, method='cubic')
e1 = time()
rdf_t = e1 - s1

s1 = time()
z7 = griddata(xy, z, xy_int, method='cubic').round(2)
e1 = time()
griddata_t = e1 - s1

rdf_t/griddata_t



########
### More testing

import numpy as np
from numpy import pi
import matplotlib.pyplot as plt
import matplotlib as mpl
%matplotlib inline
from scipy.interpolate import (
    LinearNDInterpolator, RectBivariateSpline,
    RegularGridInterpolator, CloughTocher2DInterpolator, SmoothBivariateSpline)
from scipy.ndimage import map_coordinates
#from dolointerpolation import MultilinearInterpolator
# Tweak how images are plotted with imshow
mpl.rcParams['image.interpolation'] = 'none' # no interpolation
mpl.rcParams['image.origin'] = 'lower' # origin at lower left corner
mpl.rcParams['image.cmap'] = 'RdBu_r'

def f_2d(x,y):
    '''a function with 2D input to interpolate on [0,1]'''
    twopi = 2*pi
    return np.exp(-x)*np.cos(x*2*pi)*np.sin(y*2*pi)

def f_3d(x,y,z):
    '''a function with 3D input to interpolate on [0,1]'''
    twopi = 2*pi
    return np.sin(x*2*pi)*np.sin(y*2*pi)*np.sin(z*2*pi)


Ndata = 50
xgrid = np.linspace(0,1, Ndata)
ygrid = np.linspace(0,1, Ndata+1) # use a slighly different size to check differences
zgrid = np.linspace(0,1, Ndata+2)

f_2d_grid = f_2d(xgrid.reshape(-1,1), ygrid)

plt.imshow(f_2d_grid.T)
plt.title(u'image of a 2D function ({}² pts)'.format(Ndata));

f_2d_grid.shape

f_3d_grid = f_3d(xgrid.reshape(-1,1,1), ygrid.reshape(1,-1,1), zgrid)
f_3d_grid.shape

# Define the grid to interpolate on :
Ninterp = 1000
xinterp = np.linspace(0,1, Ninterp)
yinterp = np.linspace(0,1, Ninterp+1) # use a slighly different size to check differences
zinterp = np.linspace(0,1, 5) # small dimension to avoid size explosion


#### Test 1

# Build data for the interpolator
points_x, points_y = np.broadcast_arrays(xgrid.reshape(-1,1), ygrid)
points = np.vstack((points_x.flatten(), points_y.flatten())).T
values = f_2d_grid.flatten()

x_int1, y_int1 = np.meshgrid(xinterp, yinterp)

%timeit f_2d_interp = LinearNDInterpolator(points, values)
f_2d_interp = LinearNDInterpolator(points, values)

%timeit f_2d_interp = CloughTocher2DInterpolator(points, values)
f_2d_interp = CloughTocher2DInterpolator(points, values)

# Evaluate
%timeit f_2d_interp(xinterp.reshape(-1,1), yinterp)

xy_int = np.column_stack((x_int1.flatten(), y_int1.flatten()))




#### Test 2

# Prepare the coordinates to evaluate the array on :
points_x, points_y = np.broadcast_arrays(xinterp.reshape(-1,1), yinterp)
coord = np.vstack((points_x.flatten()*(len(xgrid)-1) , # a weird formula !
                   points_y.flatten()*(len(ygrid)-1)))
coord.shape

f_2d_interp = map_coordinates(f_2d_grid, coord, order=1)


a = np.arange(20.).reshape((5, 4))
a[4][3] = np.nan

map_coordinates(a, [[0.5, 3], [2, 1.99]], order=1, mode='nearest')


#### Test 4
points_x, points_y = np.broadcast_arrays(xgrid.reshape(-1,1), ygrid)
points = np.vstack((points_x.flatten(), points_y.flatten())).T
values = f_2d_grid.flatten()

x_int1, y_int1 = np.meshgrid(xinterp, yinterp)

%timeit spline1 = SmoothBivariateSpline(points_x.flatten(), points_y.flatten(), values, kx=2, ky=2)
spline1 = SmoothBivariateSpline(points_x.flatten(), points_y.flatten(), values, kx=2, ky=2)

spline1([0.5, 1], [0.5, 1])

spline1(x_int1.flatten()[:1000], y_int1.flatten()[:1000])

###########################
### More interp stuff

import numpy as np




#######################################################
#### precip stats testing

from core.classes.hydro import hydro
from pandas import read_csv
from core.ts.met.met_stats import precip_stats

base_dir = r'\\fs02\ManagedShares2\Data\Surface Water\shared\base_data\precip\precip_data.csv'

data = read_csv(base_dir)
data1 = data.set_index(['site', 'time'])

data = precip.sel_ts(mtypes=['precip'])


#################################################
#### Hydstra fixes

from core.ecan_io import rd_sql, rd_hydstra_by_var, write_sql, rd_hydstra_db
from pandas import concat
from os.path import join
from datetime import date, timedelta

sites = [70105, 62103, 62105, 65104, 66429, 66450]
bad_site = [166602, 366425]

for i in sites:
    print(i)
    t1 = rd_hydstra_db(sites=[i])


###############################################
#### Hydrotel mtypes

from core.ecan_io import rd_sql
from pandas import to_datetime, merge, to_numeric, Grouper
from numpy import ndarray, in1d, where
from core.ts.ts import res
from core.misc.misc import time_switch, select_sites

#### Database parameters
server = 'SQL2012PROD05'
database = 'Hydrotel'

objects_tab = 'Hydrotel.dbo.Objects'
objects_col = ['Object', 'Site', 'Name']

objects1 = rd_sql(server, database, objects_tab, objects_col)

t1 = objects1.Name.unique()
t1.sort()
t1.tolist()

c1 = objects1.groupby('Name')['Site'].count()
c1.sort_values()

###############################################
#### Hydrotel all precip sites test

from core.ecan_io import rd_hydrotel

mtype = 'Rainfall'
from_date = '2017-07-21'
to_date = '2017-07-23'

precip1 = rd_hydrotel(mtype=mtype, from_date=from_date, to_date=to_date)

##############################################
#### Relative paths

import patoolib, fnmatch, os


base_dir = r'E:\ecan\git'
test_dir = r'E:\ecan\git\Ecan.Science.Python.Base\core\ts\met\interp.py'

d1 = os.path.relpath(test_dir, base_dir)
path1, file1 = os.path.split(d1)
count1 = len(path1.split(os.path.sep))

# Read in the file
with open(test_dir, 'r') as file :
  filedata = file.read()

# Replace the target string
filedata = filedata.replace('core.', ('.' * count1) + 'core.')

# Write the file out again
with open(test_dir, 'w') as file:
  file.write(filedata)



for root, dirs, files in os.walk(folder):
    for filename in fnmatch.filter(files, '*.' + ext):
        print(os.path.join(root, filename))


##############################################
#### Browns rock

from core.classes.hydro import hydro

mtype = 'flow_tel'
site = '66450'

export1 = r'E:\ecan\local\Projects\requests\browns_rock_export\browns_rock_2017-08-03.csv'

b1 = hydro().get_data(mtype, site)
b1.to_csv(export1, pivot=True)

##############################################
#### 2016-2017 usage/allocation ratios

from pandas import read_hdf

allo_use_hdf = 'E:/ecan/shared/base_data/usage/allo_use_ts_mon_results.h5'
allo_use_2017_export = r'E:\ecan\local\Projects\requests\Ilja\2017-08-04\allo_use_2106-2017.csv'


allo_use1 = read_hdf(allo_use_hdf)
allo_use2 = allo_use1[(allo_use1.date >= '2016-07-01') & (allo_use1.date < '2017-07-01')]
allo_use3 = allo_use2[allo_use2.usage.notnull()]

use1 = allo_use3.groupby(['crc', 'take_type', 'allo_block', 'wap']).sum()
use1['usage-allo_ratio'] = (use1.usage/use1.allo).round(2)
use1.to_csv(allo_use_2017_export)


##############################################
#### REC network

from geopandas import read_file, GeoDataFrame
import fiona
from core.ecan_io import rd_sql
from time import time
from os.path import join
from shapely.ops import nearest_points
from pandas import concat
from core.spatial.network import find_upstream_rec, extract_rec_catch, agg_rec_catch
from core.spatial.vector import closest_line_to_pts

catch_shp = r'E:\ecan\shared\GIS_base\vector\catchments\river-environment-classification-watershed-canterbury-2010.shp'
streams_shp = r'E:\ecan\shared\GIS_base\vector\streams\rec-canterbury-2010.shp'

server = 'SQL2012PROD05'
db = 'GIS'
table = 'MFE_NZTM_RECWATERSHEDCANTERBURY'
cols = ['NZREACH']

base_dir = r'P:\cant_catch_delin\set2'
sites_shp = 'sites.shp'
sites_col = 'site'
catch_out = 'catch1.shp'

streams1 = read_file(streams_shp)
catch1 = read_file(catch_shp)

pts1 = read_file(join(base_dir, sites_shp))

pts_seg = closest_line_to_pts(pts1, streams1, line_site_col='NZREACH', dis=1000)
nzreach = pts_seg.copy().NZREACH.unique()

reaches = find_upstream_rec(nzreach)

rec_catch = extract_rec_catch(reaches)

rec_shed = agg_rec_catch(rec_catch)
rec_shed.columns = ['NZREACH', 'geometry']
rec_shed1 = rec_shed.merge(pts_seg[['site', 'NZREACH']], on='NZREACH')

rec_shed1.to_file(join(base_dir, catch_out))

t1 = rec_shed.loc[[13059458]]






catch2 = catch1[catch1.NZREACH.isin(sites)].dissolve('NZREACH')[['geometry']]








