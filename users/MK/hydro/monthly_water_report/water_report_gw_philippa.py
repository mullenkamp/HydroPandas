# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 16:27:09 2017

@author: MichaelEK
"""

import sys
sys.path.append(r'C:\git\Ecan.Science.Python.Base')

from geopandas import read_file, sjoin
from pandas import DateOffset, to_datetime, concat, merge, cut, DataFrame, MultiIndex, Series, read_csv
from os.path import join
from core.spatial.vector import multipoly_to_poly
from core.ts import grp_ts_agg
from datetime import date
from scipy.stats import percentileofscore
from numpy import nan
from core.classes.hydro import hydro
from core.ts import tsreg
from warnings import filterwarnings

from bokeh.plotting import figure, show, output_file
from bokeh.models import ColumnDataSource, HoverTool, CategoricalColorMapper, CustomJS, renderers, annotations
from bokeh.palettes import brewer
from bokeh.models.widgets import Select
from bokeh.layouts import column

from Tkinter import Tk
from tkFileDialog import askdirectory

date1 = date.today()
date_str = str(date1)
filterwarnings('ignore')
Tk().withdraw()

###################################################
#### Parameters

base_dir = r'\\gisdata\projects\SCI\Surface Water Quantity\Projects\Freshwater Report\GW'
gw_poly_shp = 'cwms_zones_simple.shp'
gw_sites_shp = 'gw_sites.shp'
well_depth_csv = 'well_depths.csv'

interp = True

n_previous_months = 6

### Output

output_dir = askdirectory(initialdir=base_dir, title='Select the output directory', mustexist=True)
gw_sites_ts_shp = 'gw_sites_perc_' + date_str + '.shp'

## plots
test2_html = 'fresh_gw_map_' + date_str + '.html'

##################################################
#### Read in data

print('Reading in the data')

### gw
gw_sites = read_file(join(base_dir, gw_sites_shp))
gw_zones = read_file(join(base_dir, gw_poly_shp))[['ZONE_NAME', 'geometry']]

gw_zones = gw_zones.rename(columns={'ZONE_NAME': 'zone'})
#gw_zones['mtype'] = 'gw'

well_depths = read_csv(join(base_dir, well_depth_csv)).set_index('site')

### Combine the sites with the polygons
gw_site_zone0 = sjoin(gw_sites, gw_zones).drop(['index_right'], axis=1)
gw_site_zone = gw_site_zone0.drop(['geometry'], axis=1)

#################################################
#### Select sites

### GW
gw1 = hydro().get_data(mtypes='gwl_m', sites=gw_sites.site.unique())

#################################################
#### Run monthly summary stats

print('Processing past data')

### gw
gw2 = gw1.sel_ts(mtypes='gwl_m')
gw2.index = gw2.index.droplevel('mtype')
gw3 = gw2.reset_index()

if interp:
    ## Estimate monthly means through interpolation
    day1 = grp_ts_agg(gw3, 'site', 'time', 'D').mean().unstack('site')
    day2 = tsreg(day1, 'D', False)
    day3 = day2.interpolate(method='time', limit=40, limit_direction='both')
    mon_gw1 = day3.resample('M').median().stack().reset_index()
else:
    mon_gw1 = grp_ts_agg(gw3, 'site', 'time', 'M').mean().reset_index()

## Resample to month
mon_gw1['mon'] = mon_gw1.time.dt.month
mon_gw1['mtype'] = 'gw'

###############################################
#### Pull out recent monthly data

now1 = to_datetime(date1)
start_date = now1 - DateOffset(months=n_previous_months) - DateOffset(days=now1.day - 1)
end_date = now1 - DateOffset(days=now1.day - 1)

### GW
hy_gw0 = mon_gw1[(mon_gw1.time >= start_date) & (mon_gw1.time < end_date)]


##############################################
#### Run the monthly stats comparisons

print('Calculating the percentiles')

def row_perc(x, mon_summ):
    mon1 = x.time.month
    mon_val = mon_summ[(mon_summ.site == x.site) & (mon_summ.mon == mon1) & (mon_summ.mtype == x.mtype)].data.values
    perc1 = percentileofscore(mon_val, x.data)
    return(perc1)

hy_gw = hy_gw0.copy()
hy_gw['perc'] = hy_gw.apply(row_perc, mon_summ=mon_gw1, axis=1)
hy_gw.loc[:, 'time'] = hy_gw.loc[:, 'time'].dt.strftime('%Y-%m')

##############################################
#### Calc zone stats and apply categories

perc_site_zone = merge(hy_gw, gw_site_zone, on='site')
perc_zone = perc_site_zone.groupby(['zone', 'time'])['perc'].mean()

prod1 = [gw_zones.zone.unique(), perc_zone.reset_index().time.unique()]
mindex = MultiIndex.from_product(prod1, names=['zone', 'time'])
blank1 = Series(nan, index=mindex, name='temp')
zone_stats2 = concat([perc_zone, blank1], axis=1).perc
zone_stats2[zone_stats2.isnull()] = -1

cat_val_lst = [-10, -0.5, 10, 25, 75, 90, 100]
cat_name_lst = ['No data', 'Very low', 'Below average', 'Average', 'Above average', 'Very high']

cat1 = cut(zone_stats2, cat_val_lst, labels=cat_name_lst).astype('str')
cat1.name = 'category'
cat2 = concat([zone_stats2, cat1], axis=1)
cat3 = cat2.sort_values('perc', ascending=False).category

################################################
#### Output stats

print('Exporting results to csv')

ts_out1 = hy_gw.loc[:, ['site', 'time', 'perc']].copy()
ts_out2 = ts_out1.pivot_table('perc', 'site', 'time').round(2)

stats1 = mon_gw1.groupby('site')['data'].describe().round(2)
ts_out3 = concat([ts_out2, stats1], axis=1, join='inner')
well_depths1 = well_depths.loc[ts_out3.index]
ts_out4 = concat([ts_out3, well_depths1], axis=1).reset_index()

gw_sites_ts = gw_site_zone0.merge(ts_out4, on='site')
gw_sites_ts.crs = gw_sites.crs
gw_sites_ts.to_file(join(output_dir, gw_sites_ts_shp))

#################################################
#### Plotting

print('Creating the plot')

### Extract x and y data for plotting


def getPolyCoords(row, coord_type, geom='geometry'):
    """Returns the coordinates ('x' or 'y') of edges of a Polygon exterior"""

    # Parse the exterior of the coordinate
    exterior = row[geom].exterior

    if coord_type == 'x':
        # Get the x coordinates of the exterior
        return list(exterior.coords.xy[0])
    elif coord_type == 'y':
        # Get the y coordinates of the exterior
        return list(exterior.coords.xy[1])

zones1 = multipoly_to_poly(gw_zones)

zones1['x'] = zones1.apply(getPolyCoords, coord_type='x', axis=1)
zones1['y'] = zones1.apply(getPolyCoords, coord_type='y', axis=1)

zones2 = zones1.drop('geometry', axis=1)

### Combine with time series data
data1 = merge(cat1.unstack('time').reset_index(), zones2, on=['zone'])
time_index = hy_gw.time.unique().tolist()
data1['cat'] = data1[time_index[-1]]

### Extract the mtype dataframes
gw_b = data1.copy()

gw_source = ColumnDataSource(gw_b)
time_source = ColumnDataSource(DataFrame({'index': time_index}))

### Set up plotting parameters
c1 = brewer['RdBu'][5]
grey1 = brewer['Greys'][7][5]

factors = cat_name_lst[::-1]
color_map = CategoricalColorMapper(factors=factors, palette=[c1[0], c1[1], c1[2], c1[3], c1[4], grey1])

### Set up dummy source for the legend
dummy_b = gw_b[['zone', 'cat', 'x', 'y']].sort_values('zone')
dummy_b.loc[:, 'cat'].iloc[0:len(factors)] = factors
dummy_source = ColumnDataSource(dummy_b)

TOOLS = "pan,wheel_zoom,reset,hover,save"

w = 700
h = w

output_file(join(output_dir, test2_html))

## dummy figure - for legend consistency
p0 = figure(title='dummy Index', tools=[], logo=None, height=h, width=w)
p0.patches('x', 'y', source=dummy_source, fill_color={'field': 'cat', 'transform': color_map}, line_color="black", line_width=1, legend='cat')
p0.renderers = [i for i in p0.renderers if (type(i) == renderers.GlyphRenderer) | (type(i) == annotations.Legend)]
p0.renderers[1].visible = False

## Figure 3 - GW
p3 = figure(title='Groundwater Level Index', tools=TOOLS, logo=None, active_scroll='wheel_zoom', plot_height=h, plot_width=w)
p3.patches('x', 'y', source=gw_source, fill_color={'field': 'cat', 'transform': color_map}, line_color="black", line_width=1, legend='cat')
p3.renderers.extend(p0.renderers)
p3.legend.location = 'top_left'

hover3 = p3.select_one(HoverTool)
hover3.point_policy = "follow_mouse"
hover3.tooltips = [("Category", "@cat"), ("Zone", "@zone")]

def callback1(source=gw_source, window=None):
    data = source.data
    f = cb_obj.value
    source.data.cat = data[f]
    source.change.emit()

select3 = Select(title='Month', value=time_index[-1], options=time_index)
select3.js_on_change('value', CustomJS.from_py_func(callback1))

layout3 = column(p3, select3)

show(layout3)

#############################################
#### Print where results are saved

print('########################')

print('Results were saved here: ' + join(output_dir, gw_sites_ts_shp))
print('The plot was saved here: ' + join(output_dir, test2_html))

