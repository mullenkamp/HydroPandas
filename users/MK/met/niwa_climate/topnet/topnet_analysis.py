# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 09:23:46 2017

@author: MichaelEK
"""

from xarray import open_dataset, open_mfdataset
from os import path
from core.misc import rd_dir, unarchive_dir, save_df, get_subdir
from geopandas import read_file, sjoin
from core.ecan_io.met import nc_add_gis
from pandas import to_datetime, DataFrame, concat, read_hdf, merge
from core.ts.met.topnet import proc_topnet_nc
from core.spatial.vector import closest_line_to_pts, multipoly_to_poly, spatial_overlays
from core.ts import grp_ts_agg

from bokeh.layouts import widgetbox, column, row
from bokeh.plotting import figure, save, show, output_file, curdoc
from bokeh.models import ColumnDataSource, HoverTool, LogColorMapper, Legend, CategoricalColorMapper, CustomJS, renderers, annotations, Circle
from bokeh.palettes import RdYlBu11 as palette
from bokeh.palettes import brewer
from bokeh.models.widgets import Panel, Tabs, Slider, Select
from bokeh.models.tools import WheelZoomTool, TapTool
from core.plotting.bokeh.tools import getPolyCoords, getLineCoords, getPointCoords, getCoords

#############################################
#### Parameters

base_path = r'E:\ecan\shared\projects\climate_change\waimak_gw_model'

rcppast_hdf = 'rcppast.h5'
rcpproj_hdf = 'rcpproj.h5'
sites_rec_shp = 'sites_rec.shp'
bound_shp = 'catch_grps.shp'

### Plot output
map1_html = r'E:\ecan\git\ecan_python_courses\docs\map_climate1.html'


#############################################
#### Read in data

past1 = read_hdf(path.join(base_path, rcppast_hdf))
proj1 = read_hdf(path.join(base_path, rcpproj_hdf))
sites = read_file(path.join(base_path, sites_rec_shp))
sites.rename(columns={'NZREACH': 'nrch'}, inplace=True)
site_dict = dict(zip(sites.nrch, sites.site))
bound = read_file(path.join(base_path, bound_shp))[['Catchmen_1', 'geometry']]
bound.columns = ['catch', 'geometry']

#############################################
#### Resample data

past2 = concat([past1 for i in range(4)], axis=1)
past2.columns = proj1.columns
combo1 = concat([past2.reset_index(), proj1.reset_index()])

combo2 = combo1[combo1.time >= '1975-01-01']
combo3 = combo2.replace({'nrch': site_dict})

grp1 = grp_ts_agg(combo3, 'nrch', 'time', '10A-JUN')
combo_mean = grp1.mean()
combo_count = grp1.count()
combo_mean1 = combo_mean[combo_count > 3600].dropna().reset_index()
#
#past2 = past1.loc['1975-01-01':]
#grp2 = grp_ts_agg(past2.reset_index(), 'nrch', 'time', '10A-JUN')
#past_mean = grp2.mean()
#past_count = grp2.count()
#past_mean1 = past_mean[past_count > 3650].dropna()
#past_mean2 = concat([past_mean1 for i in range(4)], axis=1)
#past_mean2.columns = proj_mean1.columns
#
#combo_mean1 = concat([past_mean2.reset_index(), proj_mean1.reset_index()])

### Normalise to first decade

time1 = combo_mean1.time.unique()[0]
combo_first = combo_mean1[combo_mean1.time == time1].drop('time', axis=1)

#col_names = ['RCP2.6_norm', 'RCP4.5_norm', 'RCP6.0_norm', 'RCP8.5_norm', 'nrch']
#proj_first.columns = proj_first.columns.set_levels(col_names, 0)

combo_first2 = merge(combo_mean1[['nrch', 'time']], combo_first, on='nrch').set_index(['nrch', 'time'])
combo_norm1 = combo_mean1.set_index(['nrch', 'time']) / combo_first2 * 100

############################################
#### Stats

#combo_norm1.std()
#
#combo_norm1.std(axis=1)
#
#combo_norm1.describe().T
#combo_norm1.mean(axis=1)
#combo_norm1.mean(axis=1)


############################################
#### Plot

mean1 = combo_norm1.mean(axis=1, level=0)
mean1.name = 'mean'
mean2 = mean1.unstack(0)
mean3 = mean2.reorder_levels([1,0], axis=1)
mean4 = mean3.sort_index(axis=1)

i1 = mean4.columns.get_level_values(0).astype(str)
i2 = mean4.columns.get_level_values(1).astype(str)

mean4.columns = i1 + ' ' + i2

#mean2.plot()
#
#median1 = combo_norm1.median(axis=1)
#median1.name = 'median'
#median2 = median1.unstack(0)
#median2.plot()
#
#
#lst1 = [0,3,6,8,5,0]
#
#t1 = mean1.reset_index()
#s1 = ColumnDataSource(t1)
#data = s1.data

def sel_site(lst, site):
    return([i for i in range(len(lst)) if lst[i] == site])

#sel1 = sel_site(data['nrch'], 66402)
#
#data['RCP2.6'][sel1]


### Bokeh

sites_catch = sjoin(sites, bound).drop('index_right', axis=1)

bound.loc[:, 'geometry'] = bound.simplify(30)

bound1 = multipoly_to_poly(bound)

bound2 = getCoords(bound1).drop('geometry', axis=1)
bound2.rename(columns={'catch': 'name'}, inplace=True)

sites_catch1 = getCoords(sites_catch).drop('geometry', axis=1)

name1 = sites_catch1['name'] + ' on ' + sites_catch1['river']
sites_catch1.loc[:, 'name'] = name1
sites_catch1.loc[:, 'test'] = 'test'

### Combine with time series data
#data1 = merge(cat1.unstack('time').reset_index(), zones2, on=['zone'])
#time_index = hy_gw.time.unique().tolist()
#data1['cat'] = data1[time_index[-1]]

### Extract the mtype dataframes
#gw_b = data1.copy()

cols1 = mean4.columns
rcps = ['RCP2.6', 'RCP4.5', 'RCP6.0', 'RCP8.5']
mean4[rcps] = mean4.iloc[:, range(len(rcps))]


data_s = ColumnDataSource(mean4.reset_index())
#ts_view_s = ColumnDataSource(data[data.nrch == sites_catch1.site.iloc[0]])

bound_s = ColumnDataSource(bound2)
site_s = ColumnDataSource(sites_catch1)

### Set up plotting parameters
c1 = brewer['RdBu'][5]
grey1 = brewer['Greys'][7][5]

#factors = cat_name_lst[::-1]
#color_map = CategoricalColorMapper(factors=factors, palette=[c1[0], c1[1], c1[2], c1[3], c1[4], grey1])


w = 700
h = w

output_file(map1_html)

TOOLS = "pan,wheel_zoom,reset,save"

p1 = figure(title='Sites Map', tools=TOOLS, logo=None, active_scroll='wheel_zoom', plot_height=h, plot_width=w)
p1.patches('x', 'y', source=bound_s, fill_color=grey1, line_color="black", line_width=1)
renderer = p1.circle('x', 'y', source=site_s, size=10)

selected_circle = Circle(fill_alpha=1, fill_color="firebrick", line_color=None)
nonselected_circle = Circle(fill_alpha=0.2, fill_color="blue", line_color="firebrick")

renderer.selection_glyph = selected_circle
renderer.nonselection_glyph = nonselected_circle

p1.add_tools(HoverTool())
hover1 = p1.select_one(HoverTool)
hover1.point_policy = "follow_mouse"
hover1.tooltips = "@name"

p2 = figure(title='Site data', tools=TOOLS, logo=None, active_scroll='wheel_zoom', plot_height=h, plot_width=w, x_axis_type="datetime")
for i in rcps:
    p2.line(x='time', y=i, source=data_s)

## Tap/selection


def callback1(data=data_s, pts=site_s):
    data1 = data.data
    pt_data = pts.data
    data_cols = ['RCP2.6', 'RCP4.5', 'RCP6.0', 'RCP8.5']
    ind1 = pts.selected['1d']['indices'][0]
    site = pt_data['site'][ind1]
    ind2 = [str(site) + ' ' + i for i in data_cols]
    for d in range(len(data_cols)):
        new_data = data1[ind2[d]]
        data1[data_cols[d]] = new_data

    data.change.emit()
#
#
#callback1 = CustomJS(args=dict(source=ts_view_s), code="""
#        var view = source.data;
#        var new_data = data['RCP8.5'];
#        view['RCP2.6'].push(new_data);
#        source.change.emit();
#    """)

tt = TapTool(renderers = [renderer])
p1.add_tools(tt)
sel1 = p1.select_one(TapTool)
sel1.callback = CustomJS.from_py_func(callback1)
#site_s.callback = CustomJS.from_py_func(callback1)
#p2.js_on_change('data', CustomJS.from_py_func(callback1))

show(row(p1, p2))
#curdoc().add_root(row(p1, p2))

#
#
#show(p1)
#
#
# active_tap='tap',
#
#
#
#
#
#
#def callback1(source=ts_view_s, window=None):
#    data = source.data
#    f = cb_obj.value
#    source.data.cat = data[f]
#    source.change.emit()
#
#
#
#site_col='nrch', data_cols=cols1


#
#def callback1(source=ts_view_s, data=data_s, pts=site_s):
#    data1 = data.data
#    site_col = 'nrch'
#    data_cols = ['RCP2.6', 'RCP4.5', 'RCP6.0', 'RCP8.5']
#    ind1 = cb_obj.selected['1d'].indices
#    f = pts.data['site'][ind1]
#    site_index = [i for i in range(len(data1[site_col])) if data1[site_col][i] == f]
#    for d in data_cols:
#        data_col = data1[d][site_index]
#        source.data[d] = data_col
#    source.change.emit()



#
#
#def callback1(source=ts_view_s, data=data_s, pts=site_s):
#    data1 = data.data
#    view = source.data
#    site_col = 'nrch'
#    data_cols = ['RCP2.6', 'RCP4.5', 'RCP6.0', 'RCP8.5']
#    ind1 = cb_obj.selected['1d'].indices
#    sites = pts.data['site'][ind1]
#    site_index = [i for i in range(len(data1[site_col])) if data1[site_col][i] == sites]
#    for d in data_cols:
#        data_col = data1[d][site_index]
#        view[d].push(data_col)
#    source.change.emit()
#
#
#
#
#
#
#callback1 = CustomJS(args=dict(source=ts_view_s, data=data_s), code="""
#        var inds = cb_obj.selected['1d'].indices;
#        var data1 = data.data;
#        var view = source.data;
#        new_data = data1['RCP2.6']
#        view['RCP2.6'].push(new_data)
#        source.change.emit();
#    """)
#
#
#
#from bokeh.plotting import figure, curdoc
#from bokeh.layouts import column
#from bokeh.models import ColumnDataSource
#
#TOOLS = "tap"
#p = figure(title="Some Figure", tools=TOOLS)
#
#source = ColumnDataSource(dict(x=[[1, 3, 2], [3, 4, 6, 6]],
#                y=[[2, 1, 4], [4, 7, 8, 5]], name=['A', 'B']))
#
#pglyph = p.patches('x', 'y', source=source, color=["firebrick", "navy"],
#                                alpha=[0.8, 0.3], line_width=2)
#
#def callback(attr, old, new):
#    # The index of the selected glyph is : new['1d']['indices'][0]
#    patch_name =  source.data['name'][new['1d']['indices'][0]]
#    print("TapTool callback executed on Patch {}".format(patch_name))
#
#pglyph.data_source.on_change('selected',callback)
#
##curdoc().add_root(column(p))
#
#show(p)



#ind1 = cb_obj.selected['1d'].indices[0]




