# -*- coding: utf-8 -*-
"""
Functions to index and select data within the hydro class.
"""
import numpy as np
# import pandas as pd
# from hydropandas.core.base import resample_fun

#########################################################
### Selecting/indexing the time series data and returing a hydro class object


def sel(self, hydro_id=None, sites=None, require=None, start=None, end=None, to_units=None, return_qual_code=False):
    """
    Function to select data based on various input parameters and output a Hydro object.

    Parameters
    ----------
    mtypes : str or a list or ndarray of str
        The mtypes that should be returned.
    sites : int, str, list, or ndarray
        The sites that should be returned.
    require : list or ndarray
        A list of sites that must be in all of the mtypes that should be returned.
    start : str
        The start date in the format '2001-01-01'.
    end : str
        The end date in the above format.
    to_units: str or dict
        The new units to convert the data to.
    return_qual_code: bool
        Should the quality codes be returned?

    Return
    ------
    Hydro
    """

    sel_out = self.sel_ts(hydro_id=hydro_id, sites=sites, require=require, start=start, end=end, to_units=to_units, return_qual_code=return_qual_code)
    sel_out1 = sel_out.reset_index()
    if return_qual_code:
        qual_codes = 'qual_codes'
    else:
        qual_codes = False
    new1 = self.copy()
    delattr(new1, 'tsdata')
    new2 = new1.add_tsdata(sel_out1, dformat='long', hydro_id='hydro_id', freq_type=new1.mfreq, times='time', sites='site', values='value', units=new1.units, qual_codes=qual_codes)
    return new2


def __getitem__(self, key):
    n3 = self.tsdata.loc(axis=0)[key]
    return n3


def sel_by_geo_attr(self, attr_dict, mtypes=None):
    """
    Function to select sites based on the geo_attributes and return a hydro class object.
    """

    if not isinstance(attr_dict, dict):
        raise TypeError('attr_dict must be a dictionary.')
    attr1 = self.site_geo_attr.copy()
    for i in attr_dict:
        attr1 = attr1[np.in1d(attr1[i].values, attr_dict[i])]

    sites = attr1.index.tolist()

    new2 = self.sel(mtypes=mtypes, sites=sites)
    return new2

##########################################################
### Selecting/Indexing the time series data and returning a Pandas object


def sel_ts(self, hydro_id=None, sites=None, require=None, pivot=False, start=None, end=None, to_units=None, return_qual_code=False):
    """
    Function to select data based on various input parameters and output a Pandas Series.

    Parameters
    ----------
    hydro_id : str or a list or ndarray of str
        The hydro_ids that should be returned. Can also be a str or list of str of one of the four key hydro_id attributes (i.e. hydro feature, hydro_id, freq type, qual state). For example, inputting 'river_flow' will get you 'river_flow_cont_raw', 'river_flow_cont_qc', 'river_flow_disc_raw', and 'river_flow_disc_qc' if these exist in the hydro object.
    sites : int, str, list, or ndarray
        The sites that should be returned.
    require : list or ndarray
        A list of sites that must be in all of the mtypes that should be returned.
    pivot : bool
        Should the data be pivotted to wide format?
    resample : str or None
        Either None or the Pandas resampling code to resample the data (e.g. 'D' for daily, 'W' for weekly, 'M' for monthly). See http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases for more details.
    start : str
        The start date in the format '2001-01-01'.
    end : str
        The end date in the above format.
    to_units: str or dict
        The new units to convert the data to.
    return_qual_code: bool
        Should the quality codes be returned?

    Return
    ------
    Series
        With a MultiIndex of mtype, site, and time
    """
    h1 = self.copy()
    if isinstance(hydro_id, str):
        hydro_id = [hydro_id]
    if hydro_id is None:
        hydro_id = slice(None)
    elif isinstance(hydro_id, (list, np.ndarray)):
        hydro_id = [i for i in h1.hydro_id if any([j in i for j in hydro_id])]
    else:
        raise TypeError('hydro_id must be a str, list, or ndarray')
    if sites is None:
        sites = slice(None)
    if isinstance(to_units, dict):
        self.to_units(h1, to_units, inplace=True)
    if return_qual_code:
        sel_out1 = h1.tsdata.loc(axis=0)[hydro_id, sites, start:end]
    else:
        sel_out1 = h1.tsdata.loc(axis=0)[hydro_id, sites, start:end]['value']
    if require is not None:
        hydro_id2 = sel_out1.index.get_level_values('hydro_id').unique()
        hydro_id3 = [i for i in hydro_id2 if all([j in list(h1.hydroid_sites[i]) for j in require])]
        sel_out1 = sel_out1.loc(axis=0)[hydro_id3, :, :]
    if sel_out1.empty:
        return sel_out1
    sel_out1.index = sel_out1.index.remove_unused_levels()
    if pivot:
        levels1 = sel_out1.index.get_level_values(0).unique()
        if len(levels1) == 1:
            sel_out1.index = sel_out1.index.droplevel('hydro_id')
            sel_out1 = sel_out1.unstack('site')
        else:
            sel_out1 = sel_out1.unstack(['hydro_id', 'site'])
    return sel_out1


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

    pts = self.geo_point
    sites_sel = sel_sites_poly(pts, poly, buffer_dis).index.tolist()
    return sites_sel


def sel_ts_by_poly(self, poly, buffer_dis=0, **kwargs):
    """
    Function to filter the sites by a polygon and return a Pandas Series.

    Parameters
    ----------
    poly : GeoDataFrame or str as shapefile path
        This input can be either a str path to a polygon shapefile or a GeoDataFrame of polygons.
    buffer_dis : int
        The additional buffer distance surrounding the polygons to include.
    **kwargs
        Keyword arguments from sel_ts excluding the sites.

    Returns
    -------
    Series
        With a MultiIndex of mtype, site, and time
    """
    sites_sel = self.sel_sites_by_poly(poly, buffer_dis)
    ts_out1 = self.sel_ts(sites=sites_sel, **kwargs)
    return ts_out1


def sel_by_poly(self, poly, buffer_dis=0, **kwargs):
    """
    Function to filter the sites by a polygon and return a Hydro object.

    Parameters
    ----------
    poly : GeoDataFrame or str as shapefile path
        This input can be either a str path to a polygon shapefile or a GeoDataFrame of polygons.
    buffer_dis : int
        The additional buffer distance surrounding the polygons to include.
    **kwargs
        Keyword arguments from sel_ts excluding the sites.

    Returns
    -------
    Hydro
    """
    sites_sel = self.sel_sites_by_poly(poly, buffer_dis)
    out1 = self.sel(sites=sites_sel, **kwargs)
    return out1


def _comp_by_buffer(self, buffer_dis=None):
    """
    Create a site comparison dictionary using a buffer distance around the site locations.
    """
    if not isinstance(buffer_dis, int):
        raise ValueError('buffer_dis must be an integer.')
    if not hasattr(self, 'geo_loc'):
        raise ValueError('Add the geo locations of the sites.')
    pts = self.geo_point
    pts_buff = pts.buffer(buffer_dis)
    dict1 = {i: tuple(j for j in pts[pts.within(pts_buff.loc[i])].index.tolist()  if j != i) for i in pts_buff.index}
    setattr(self, 'comp_dict', dict1)
    return self


def _comp_by_catch(self):
    """
    Create a site comparison dictionary using the catchments polygons.
    """
    if not hasattr(self, 'geo_catch'):
        raise ValueError('Add in catchments to hydro object.')
    if not hasattr(self, 'geo_loc'):
        raise ValueError('Add the geo locations of the sites.')
    catch = self.geo_catch
    pts = self.geo_point
    dict1 = {i: tuple(j for j in pts[pts.within(catch.loc[i, 'geometry'])].index.tolist()  if j != i) for i in catch.index}
    setattr(self, 'comp_catch_dict', dict1)
    return self






