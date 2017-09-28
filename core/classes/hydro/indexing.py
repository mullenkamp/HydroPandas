# -*- coding: utf-8 -*-
"""
Functions to index and select data within the hydro class.
"""
from pandas import DataFrame, Series, DatetimeIndex, to_datetime, MultiIndex, concat, Grouper
from numpy import array, ndarray, in1d, unique, append, nan, argmax
from geopandas import GeoDataFrame, read_file

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


#def _time_freq(x):
#    freq1 = x.index.get_level_values('time')[:3].inferred_freq
#    secs = int((x.index.get_level_values('time')[1] - x.index.get_level_values('time')[0]).total_seconds())
#    out1 = Series([freq1, secs])
#    return(out1)


def _pivot(df, resample, mtype=None):
    from core.classes.hydro.base import resample_fun

    if resample:
        if len(df.index.names) == 2:
            grp1 = df.groupby(level='site')
        else:
            grp1 = df.groupby(level=['mtype', 'site'])
        freq1 = grp1.apply(lambda x: x.index.get_level_values('time')[2:5].inferred_freq)
        if not all(freq1.isin([None])):
            if all(freq1.isin([freq1.iloc[0]])):
                if len(df.index.names) == 2:
                    out2 = df.unstack('site').resample(freq1.iloc[0]).mean()
                else:
                    out2 = df.unstack(['mtype', 'site']).resample(freq1.iloc[0]).mean()
                return(out2)
            secs1 = grp1.apply(lambda x: int((x.index.get_level_values('time')[3] - x.index.get_level_values('time')[2]).total_seconds()))
    #        set1 = grp1.apply(_time_freq)
            freq2 = concat([freq1, secs1], axis=1)
            freq2.columns = ['freq', 'secs']
            check1 = freq2[freq2.freq.notnull()]
            res1 = check1.loc(axis=0)[check1.secs.idxmax()]
            res2 = res1.freq
            if len(df.index.names) == 2:
                res_mtype = mtype
                agg_fun = resample_fun[res_mtype]
                grp2 = df.groupby([Grouper(level='site'), Grouper(level='time', freq=res2)])
                if agg_fun == 'sum':
                    out1 = grp2.sum()
                elif agg_fun == 'mean':
                    out1 = grp2.mean()
                out2 = out1.unstack('site')
            else:
                res_mtype = res1.name[0]
                agg_fun = resample_fun[res_mtype]
                grp2 = df.groupby([Grouper(level='mtype'), Grouper(level='site'), Grouper(level='time', freq=res2)])
                if agg_fun == 'sum':
                    out1 = grp2.sum()
                elif agg_fun == 'mean':
                    out1 = grp2.mean()
                out2 = out1.unstack(['mtype', 'site'])

#            out2 = out1.unstack(['mtype', 'site'])
        else:
            if len(df.index.names) == 2:
                out2 = df.unstack(['site'])
            else:
                out2 = df.unstack(['mtype', 'site'])
    else:
        if len(df.index.names) == 2:
            out2 = df.unstack(['site'])
        else:
            out2 = df.unstack(['mtype', 'site'])
    return(out2)


def sel_ts(self, mtypes=None, sites=None, require=None, pivot=False, resample=True, start=None, end=None):
    if mtypes is None:
        mtypes = slice(None)
    if sites is None:
        sites = slice(None)
    sel_out = self.data.loc(axis=0)[mtypes, sites, :]
    if require is not None:
        sites2 = sel_out.index.get_level_values('site').unique()
        sites3 = [i for i in sites2 if all(in1d(require, list(self.sites_mtypes[i])))]
        sel_out1 = sel_out.loc(axis=0)[:, sites3, start:end]
    else:
        sel_out1 = sel_out.loc(axis=0)[:, :, start:end]
    if pivot:
        levels1 = sel_out1.index.get_level_values(0).unique()
        sel_out1 = _pivot(sel_out1, resample, levels1[0])
        if len(levels1) == 1:
            sel_out1 = sel_out1.loc[:, levels1[0]]
    return(sel_out1)


##############################################################
### Selecting/indexing the geo data

def sel_sites(self, mtypes=None, sites=None):
    """
    Function to select sites based on certain criteria.
    Output is a list of site names.
    """
    if (sites is None) & (mtypes is None):
        raise ValueError('Must input at least one of sites and/or mtypes.')
    else:
        site_lst = []
        if sites is not None:
            if isinstance(sites, (int, str)):
                site_lst.extend([sites])
            elif isinstance(sites, list):
                site_lst.extend(sites)
        if mtypes is not None:
            if isinstance(mtypes, (int, str)):
                site_lst.extend(self.mtypes_sites[mtypes])
            if isinstance(mtypes, list):
                for i in mtypes:
                    site_lst.extend(self.mtypes_sites[i])
        site_lst2 = Series(site_lst).unique().tolist()
        return(site_lst2)


def sel_sites_by_poly(self, poly, buffer_dis=0):
    from core.spatial.vector import sel_sites_poly

    pts = self.geo_loc
    sites_sel = sel_sites_poly(pts, poly, buffer_dis).index.tolist()
    return(sites_sel)


def sel_ts_by_poly(self, poly, buffer_dis=0, mtypes=None, require=None, pivot=False, resample=True):
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






