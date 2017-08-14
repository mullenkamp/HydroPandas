# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 16:27:09 2017

@author: MichaelEK
"""

from geopandas import read_file, overlay, sjoin, GeoDataFrame
from core.classes.hydro import hydro, all_mtypes
from datetime import datetime
from pandas import DateOffset, to_datetime, date_range, read_csv, concat, merge, cut, DataFrame
from os.path import join
from core.spatial.vector import spatial_overlays, multipoly_to_poly
from core.ts import grp_ts_agg, w_resample
from datetime import date
from scipy.stats import percentileofscore
from numpy import in1d, round
from bokeh.plotting import figure, save, show, output_file
from bokeh.models import ColumnDataSource, HoverTool, LogColorMapper
from bokeh.palettes import RdYlBu11 as palette
from bokeh.palettes import brewer
from bokeh.models.widgets import Panel, Tabs

###################################################
#### Parameters

base_dir = r'P:\Surface Water Quantity\Projects\Freshwater Report'
sw_poly_shp = 'sw_boundary_v01.shp'
precip_poly_shp = 'precip_boundary_v01.shp'
rec_catch_shp = r'E:\ecan\shared\GIS_base\vector\catchments\catch_delin_recorders.shp'
streams_shp = r'E:\ecan\shared\GIS_base\vector\streams\river-environment-classification-canterbury-2010.shp'
rec_sites_shp = r'E:\ecan\shared\GIS_base\vector\catchments\recorder_sites_REC.shp'
rec_sites_details_shp = r'E:\ecan\shared\GIS_base\vector\catchments\recorder_sites_REC_details.shp'

qual_codes = [10, 18, 20, 30, 50, 11, 21, 40]

std_cat = [0.1, 1]

pot_sw_site_list_csv = 'potential_sw_site_list.csv'

### Output
catch_shp = 'recorder_catch.shp'
pot_sites_shp = 'potential_sites.shp'
pot_catch_sites_shp = 'potential_catch_sites.shp'
sw_zone_stats_shp = 'sw_zone_stats.shp'
precip_site_shp = 'precip_sites.shp'
precip_zone_stats_shp = 'precip_zone_stats.shp'

## plots
test1_html = r'E:\ecan\git\ecan_python_courses\docs\test1.html'

##################################################
#### Read in data

### SW
sw_poly = read_file(join(base_dir, sw_poly_shp))[['lat_zone', 'lon_zone', 'geometry']]
rec_catch = read_file(rec_catch_shp)
streams = read_file(streams_shp)
rec_sites = read_file(rec_sites_shp)
site_list = read_csv(join(base_dir, pot_sw_site_list_csv))

### precip
precip_sites = read_file(join(base_dir, precip_site_shp))
precip_zones = read_file(join(base_dir, precip_poly_shp))

#################################################
#### Select sites

### SW
sites1 = site_list[site_list.Notes.isnull()]

flow1 = hydro().get_data('flow', sites1.site, qual_codes)
stats_flow = flow1.stats('flow')

### precip
precip1 = hydro().get_data(mtypes='precip', sites=precip_sites.site, qual_codes=qual_codes)


#################################################
#### Estimate the catchment area weights

### SW
site_catch1 = rec_catch[rec_catch.site.isin(sites1.site)]

overlay1 = spatial_overlays(site_catch1, sw_poly, how='intersection')

overlay2 = overlay1.merge(sites1, on=['site', 'lat_zone', 'lon_zone'])
overlay2['area'] = overlay2.area

zone_sum1 = overlay2.groupby(['lat_zone', 'lon_zone']).area.transform('sum')
overlay2['agg_area'] = zone_sum1

overlay3 = overlay2.set_index('site').drop('geometry', axis=1)
site_area_weight = (overlay3.area / overlay3.agg_area)

### precip
precip_site_zone = sjoin(precip_sites, precip_zones)

#################################################
#### Run monthly summary stats

### SW
flow2 = flow1.sel_ts(mtypes='flow')
flow2.index = flow2.index.droplevel('mtype')
flow3 = flow2.reset_index()

mon_flow1 = grp_ts_agg(flow3, 'site', 'time', 'M', 'median')
mon_flow1['mon'] = mon_flow1.time.dt.month

### precip
precip2 = precip1.sel_ts(mtypes='precip')
precip2.index = precip2.index.droplevel('mtype')
precip3 = precip2.reset_index()

mon_precip1 = grp_ts_agg(precip3, 'site', 'time', 'M', 'sum')
mon_precip1['mon'] = mon_precip1.time.dt.month

###############################################
#### Pull out recent monthly data from hydrotel

now1 = to_datetime(date.today())
start_date = now1 - DateOffset(months=1) - DateOffset(days=now1.day - 1)
end_date = now1 - DateOffset(days=now1.day - 1)

### SW
sites2 = sites1.copy()
sites2.loc[sites2.site.isin([64610, 65104, 68526]), 'site'] = [164610, 165104, 168526]

hy1 = hydro().get_data(mtypes='flow_tel', sites=sites2.site, from_date=start_date.strftime('%Y-%m-%d'), to_date=end_date.strftime('%Y-%m-%d'))
if len(hy1.sites) != len(sites2):
    raise ValueError("Didn't get all sites")
hy2 = hy1.data.reset_index().drop('mtype', axis=1)
hy2 = hy2[hy2.time != end_date]
hy3 = hy2.groupby('site')['data'].median().reset_index()
hy3.columns = ['site', 'mon_median_flow']

hy3.loc[hy3.site.isin([164610, 165104, 168526]), 'site'] = [64610, 65104, 68526]
hy_flow = hy3.copy()

### precip
hy1 = hydro().get_data(mtypes='precip_tel', sites=mon_precip1.site.unique(), from_date=start_date.strftime('%Y-%m-%d'), to_date=end_date.strftime('%Y-%m-%d'))
if len(hy1.sites) != len(mon_precip1.site.unique()):
    raise ValueError("Didn't get all sites")
hy2 = hy1.data.reset_index().drop('mtype', axis=1)
hy2 = hy2[hy2.time != end_date]
hy3 = hy2.groupby('site')['data'].sum().reset_index()
hy3.columns = ['site', 'mon_sum_precip']
hy_precip = hy3.copy()


##############################################
#### Run the monthly stats comparisons

### SW
mon1 = start_date.month
mon_stats = mon_flow1[mon_flow1.mon == mon1]
mon_val = hy_flow.set_index('site')

mon_stats_grp = mon_stats.groupby('site')['data']

mon_site_prec = mon_val['mon_median_flow'].copy()
mon_site_prec.name = 'mon_flow_perc'
t1_sw =
for name, grp in mon_stats_grp:
    val = mon_val.loc[name][0]
    arr1 = grp.values
    perc1 = percentileofscore(arr1, val)
    mon_site_prec.loc[name] = perc1

mon_flow_prec = mon_site_prec.copy()

### precip
mon1 = start_date.month
mon_stats = mon_precip1[mon_precip1.mon == mon1]
mon_val = hy_precip.set_index('site')

mon_stats_grp = mon_stats.groupby('site')['data']

mon_site_prec = mon_val['mon_sum_precip'].copy()
mon_site_prec.name = 'mon_precip_perc'
for name, grp in mon_stats_grp:
    val = mon_val.loc[name][0]
    arr1 = grp.values
    perc1 = percentileofscore(arr1, val)
    mon_site_prec.loc[name] = perc1

mon_precip_prec = mon_site_prec.copy()

##############################################
#### Calc zone stats and apply categories

### SW
mon_site = mon_flow_prec * site_area_weight
mon_site.name = 'mon_flow_perc'

mon_site_area = concat([overlay3, mon_site], axis=1)

zone_stats1 = mon_site_area.groupby(['lat_zone', 'lon_zone'])['mon_flow_perc'].sum().round(1)

cat_val_lst = [0, 10, 40, 60, 90, 100]
cat_name_lst = ['very low', 'below average', 'average', 'above average', 'very high']

cat1 = cut(zone_stats1, cat_val_lst, labels=cat_name_lst).astype('str')
cat1.name = 'flow_category'

cat2 = concat([zone_stats1, cat1], axis=1)

sw_poly2 = sw_poly.merge(cat2.reset_index(), on=['lat_zone', 'lon_zone'])

sw_poly2.to_file(join(base_dir, sw_zone_stats_shp))

### precip
precip_site_zone1 = precip_site_zone.drop(['geometry', 'index_right'], axis=1).set_index('site')
mon_site_area = concat([precip_site_zone1, mon_precip_prec], axis=1)

zone_stats1 = mon_site_area.groupby(['lat_zone', 'lon_zone'])['mon_precip_perc'].mean().round(1)

cat_val_lst = [0, 10, 40, 60, 90, 100]
cat_name_lst = ['very low', 'below average', 'average', 'above average', 'very high']

cat1 = cut(zone_stats1, cat_val_lst, labels=cat_name_lst).astype('str')
cat1.name = 'precip_category'

cat2 = concat([zone_stats1, cat1], axis=1)

precip_poly2 = precip_zones.merge(cat2.reset_index(), on=['lat_zone', 'lon_zone'])

precip_poly2.to_file(join(base_dir, precip_zone_stats_shp))


#################################################
#### Plotting



precip_poly2 = read_file(join(base_dir, precip_zone_stats_shp))
sw_poly2 = read_file(join(base_dir, sw_zone_stats_shp))

sw_precip_poly = sw_poly2.merge(precip_poly2.drop('geometry', axis=1), on=['lat_zone', 'lon_zone'])

sw_precip_poly2 = multipoly_to_poly(sw_precip_poly)


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



sw_precip_poly2['x'] = sw_precip_poly2.apply(getPolyCoords, coord_type='x', axis=1)
sw_precip_poly2['y'] = sw_precip_poly2.apply(getPolyCoords, coord_type='y', axis=1)

g_df = sw_precip_poly2.drop('geometry', axis=1).copy()
c1 = brewer['RdBu'][5]
color_dict = {'very high': c1[0], 'above average': c1[1], 'average': c1[2], 'below average': c1[3], 'very low': c1[4]}
g_df['precip_color'] = g_df['precip_cat'].replace(color_dict)
g_df['flow_color'] = g_df['flow_categ'].replace(color_dict)

gsource = ColumnDataSource(g_df)
color_map = LogColorMapper(palette=palette)



TOOLS = "pan,wheel_zoom,reset,hover,save"

output_file(test1_html)

p1 = figure(title='Test map', tools=TOOLS, logo=None)
p1.patches('x', 'y', source=gsource, fill_color={'field': 'precip_color'}, line_color="black", line_width=0.5, legend='precip_cat')
p1.legend.location = 'top_left'
hover1 = p1.select_one(HoverTool)
hover1.point_policy = "follow_mouse"
hover1.tooltips = [("Category", "@precip_cat"), ("Percentile", "@mon_precip{1.1}" + "%"), ]
tab1 = Panel(child=p1, title='Precip')

p2 = figure(title='Test map', tools=TOOLS, logo=None)
p2.patches('x', 'y', source=gsource, fill_color={'field': 'flow_color'}, line_color="black", line_width=0.5, legend='flow_categ')
p2.legend.location = 'top_left'
hover2 = p2.select_one(HoverTool)
hover2.point_policy = "follow_mouse"
hover2.tooltips = [("Category", "@flow_categ"), ("Percentile", "@mon_flow_p{1.1}" + "%"), ]
tab2 = Panel(child=p2, title='Flow')

tabs = Tabs(tabs=[tab1, tab2])

show(tabs)
































##################################################
#### Testing

precip = hydro().get_data(mtypes='precip', sites=join(base_dir, precip_poly_shp), qual_codes=qual_codes)


mis_sites = mon_precip1.site.unique()[~in1d(mon_precip1.site.unique(), hy1.sites)]




t1 = grp.copy()
min1 = t1.min()
max1 = t1.max()

f1 = round((t1.values - min1)/(max1 - min1) *100, 1)

f3 = []
for i in t1.values:
    f2 = percentileofscore(t1.values, i)
    f3.append(f2)


df1 = DataFrame([t1.values, f1, f3]).T
df1.columns = ['flow_data', 'fouad', 'percentile']

df1.to_csv(join(base_dir, 'test_output_71195.csv'), index=False)

























