# -*- coding: utf-8 -*-
"""
Functions to index and select data within the hydro class.
"""
from pandas import DataFrame, Series, DatetimeIndex, to_datetime, MultiIndex, concat, Grouper
from numpy import array, ndarray, in1d, unique, append, nan, argmax
from geopandas import GeoDataFrame, read_file
from core.classes.hydro.base import resample_fun

#########################################################
### Selecting/indexing the time series data and returing a hydro class object


def sel(self, mtypes=None, sites=None, require=None, start=None, end=None):
    sel_out = self.sel_ts(mtypes=mtypes, sites=sites, require=require, start=start, end=end)
    sel_out1 = sel_out.reset_index()
    new1 = self.add_data(sel_out1, 'time', 'site', 'mtype', 'data', dformat='long', add=False)
    return(new1)


def __getitem__(self, key):
    n3 = self.data.loc(axis=0)[key]
    return(n3)


def sel_by_geo_attr(self, attr_dict, mtypes=None):
    """
    Function to select sites based on the geo_attributes and return a hydro class object.
    """

    if not isinstance(attr_dict, dict):
        raise TypeError('attr_dict must be a dictionary.')
    attr1 = self.site_geo_attr.copy()
    for i in attr_dict:
        attr1 = attr1[in1d(attr1[i].values, attr_dict[i])]

    sites = attr1.index.tolist()

    new2 = self.sel(mtypes=mtypes, sites=sites)
    return(new2)

##########################################################
### Selecting/Indexing the time series data and returning a Pandas object


#def _pivot(df, mtypes):
#    if len(mtypes) == 1:
#        out1.index = out1.index.droplevel('mtype')
#        out2 = out1.unstack('site')
#    else:
#        out2 = out1.unstack(['mtype', 'site'])
#
#    return(out2)


def sel_ts(self, mtypes=None, sites=None, require=None, pivot=False, resample=None, start=None, end=None):
    if mtypes is None:
        mtypes = slice(None)
    if sites is None:
        sites = slice(None)
    sel_out1 = self.data.loc(axis=0)[mtypes, sites, start:end]
    if require is not None:
        mtypes2 = sel_out1.index.get_level_values('mtype').unique()
        mtypes3 = [i for i in mtypes2 if all([j in list(self.mtypes_sites[i]) for j in require])]
        sel_out1 = sel_out1.loc(axis=0)[mtypes3, :, :]
    if sel_out1.empty:
        return(sel_out1)
    if resample is not None:
        levels1 = sel_out1.index.get_level_values(0).unique()
        agg_funs = {i: resample_fun[i] for i in levels1}
        sel_out2 = DataFrame()
        if 'mean' in agg_funs.values():
            mtypes_mean = [i for i in agg_funs if agg_funs[i] == 'mean']
            df_mean = sel_out1.loc[mtypes_mean, :, :]
            mean1 = df_mean.groupby([Grouper(level='mtype'), Grouper(level='site'), Grouper(level='time', freq=resample)]).mean()
            sel_out2 = mean1.copy()
        if 'sum' in agg_funs.values():
            mtypes_sum = [i for i in agg_funs if agg_funs[i] == 'sum']
            df_sum = sel_out1.loc[mtypes_sum, :, :]
            sum1 = df_sum.groupby([Grouper(level='mtype'), Grouper(level='site'), Grouper(level='time', freq=resample)]).sum()
            sel_out2 = concat([sel_out2, sum1])
    else:
        sel_out2 = sel_out1
    if pivot:
        levels1 = sel_out2.index.get_level_values(0).unique()
        if len(levels1) == 1:
            sel_out2.index = sel_out2.index.droplevel('mtype')
            sel_out2 = sel_out2.unstack('site')
        else:
            sel_out2 = sel_out2.unstack(['mtype', 'site'])
    return(sel_out2)


##############################################################
### Selecting/indexing the geo data

#def sel_sites(self, mtypes=None, sites=None):
#    """
#    Function to select sites based on certain criteria.
#    Output is a list of site names.
#    """
#    if (sites is None) & (mtypes is None):
#        raise ValueError('Must input at least one of sites and/or mtypes.')
#    else:
#        site_lst = []
#        if sites is not None:
#            if isinstance(sites, (int, str)):
#                site_lst.extend([sites])
#            elif isinstance(sites, list):
#                site_lst.extend(sites)
#        if mtypes is not None:
#            if isinstance(mtypes, (int, str)):
#                site_lst.extend(self.mtypes_sites[mtypes])
#            if isinstance(mtypes, list):
#                for i in mtypes:
#                    site_lst.extend(self.mtypes_sites[i])
#        site_lst2 = Series(site_lst).unique().tolist()
#        return(site_lst2)


def sel_sites_by_poly(self, poly, buffer_dis=0):
    from core.spatial.vector import sel_sites_poly

    pts = self.geo_loc
    sites_sel = sel_sites_poly(pts, poly, buffer_dis).index.tolist()
    return(sites_sel)


def sel_ts_by_poly(self, poly, buffer_dis=0, mtypes=None, require=None, pivot=False, resample=None):
    sites_sel = self.sel_sites_by_poly(poly, buffer_dis)
    ts_out1 = self.sel_ts(sites=sites_sel, mtypes=mtypes, require=require, pivot=pivot, resample=resample)
    return(ts_out1)


def sel_by_poly(self, poly, buffer_dis=0, mtypes=None, require=None):
    sites_sel = self.sel_sites_by_poly(poly, buffer_dis)
    out1 = self.sel(sites=sites_sel, mtypes=mtypes, require=require)
    return(out1)


def _comp_by_buffer(self, buffer_dis=None):
    """
    Create a site comparison dictionary using a buffer distance around the site locations.
    """
    if not isinstance(buffer_dis, int):
        raise ValueError('buffer_dis must be an integer.')
    if not hasattr(self, 'geo_loc'):
        raise ValueError('Add the geo locations of the sites.')
    pts = self.geo_loc
    pts_buff = pts.buffer(buffer_dis)
    dict1 = {i: tuple(j for j in pts[pts.within(pts_buff.loc[i])].index.tolist()  if j != i) for i in pts_buff.index}
    setattr(self, 'comp_dict', dict1)
    return(self)


def _comp_by_catch(self):
    """
    Create a site comparison dictionary using the catchments polygons.
    """
    if not hasattr(self, 'geo_catch'):
        raise ValueError('Add in catchments to hydro object.')
    if not hasattr(self, 'geo_loc'):
        raise ValueError('Add the geo locations of the sites.')
    catch = self.geo_catch
    pts = self.geo_loc
    dict1 = {i: tuple(j for j in pts[pts.within(catch.loc[i, 'geometry'])].index.tolist()  if j != i) for i in catch.index}
    setattr(self, 'comp_catch_dict', dict1)
    return(self)






