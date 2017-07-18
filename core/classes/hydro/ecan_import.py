# -*- coding: utf-8 -*-
"""
Input and output functions specific to ECan systems.
"""

from pandas import DataFrame, Series, DatetimeIndex, to_datetime, MultiIndex, concat, to_numeric
from numpy import array, ndarray, in1d, unique, append, nan, argmax, where, dtype
from geopandas import GeoDataFrame, GeoSeries, read_file
from core.ecan_io import rd_sql
from core.spatial.vector import xy_to_gpd, sel_sites_poly

############################################
#### Database parameters

prod_server03 = 'SQL2012PROD03'
prod_server05 = 'SQL2012PROD05'
dw_db = 'DataWarehouse'

flow_dict = {'server': prod_server03, 'db': dw_db, 'table': 'F_HY_Flow_Data', 'site_col': 'SiteNo', 'time_col': 'DateTime', 'data_col': 'Value', 'qual_col': 'QualityCode'}

precip_dict = {'server': prod_server03, 'db': dw_db, 'table': 'F_HY_Precip_data', 'site_col': 'site', 'time_col': 'time', 'data_col': 'data', 'qual_col': 'qual_code'}

swl_dict = {'server': prod_server03, 'db': dw_db, 'table': 'F_HY_SWL_data', 'site_col': 'site', 'time_col': 'time', 'data_col': 'data', 'qual_col': 'qual_code'}

gwl_dict = {'server': prod_server03, 'db': dw_db, 'table': 'F_HY_GWL_data', 'site_col': 'site', 'time_col': 'time', 'data_col': 'data', 'qual_col': 'qual_code'}

gwl_m_dict = {'server': prod_server05, 'db': 'Wells', 'table': 'DTW_READINGS', 'site_col': 'WELL_NO', 'time_col': 'DATE_READ', 'data_col': 'DEPTH_TO_WATER', 'qual_col': None}

usage_dict = {'server': prod_server03, 'db': dw_db, 'table': 'F_HY_Usage_data', 'site_col': 'site', 'time_col': 'time', 'data_col': 'data', 'qual_col': None}

wus_usage_dict = {'server': prod_server03, 'db': 'WUS', 'table': 'vw_WUS_Fact_DailyUsageByUsageSite', 'site_col': 'UsageSite', 'time_col': 'Day', 'data_col': 'Usage', 'qual_col': None}

#usage_server = 'SQL2012DEV01'
#usage_db = 'Hydro'
#usage_tab = 'usage_data'
#
#geo_attr_server = 'SQL2012DEV01'
#geo_attr_db = 'Hydro'
#geo_attr_tab = 'site_geo_attr'


############################################
### Populate site attributes

#def get_site_attr(self):
#    from core.ecan_io import rd_sql
#
#    def rd_sw_attr(sites):
#
#        site_attr = rd_sql('SQL2012PROD05', 'GIS', 'vGAUGING_NZTM', col_names=['SiteNumber', 'RIVER', 'SITENAME'], where_col='SiteNumber', where_val=sites, geo_col=False)
#        site_attr.columns = ['site', 'waterbody', 'site_name']
#        site_attr.loc[:, 'waterbody'] = site_attr.waterbody.str.title()
#        site_attr.loc[:, 'site_name'] = site_attr.site_name.str.title()
#        site_attr.loc[:, 'site_name'] = site_attr.site_name.str.replace(' \(Recorder\)', '')
#        site_attr.loc[:, 'site_name'] = site_attr.site_name.str.replace('Sh', 'SH')
#        site_attr.loc[:, 'site_name'] = site_attr.site_name.str.replace('Ecs', 'ECS')
#        site_attr.loc[:, 'site'] = to_numeric(site_attr.loc[:, 'site'], errors='ignore')
#        return(site_attr.set_index('site'))
#
#    def rd_ecan_attr(sites):
#
#        site_attr = rd_sql('SQL2012PROD05', 'Bgauging', 'RSITES', col_names=['SiteNumber', 'River', 'SiteName'], where_col='SiteNumber', where_val=sites)
#        site_attr.columns = ['site', 'waterbody', 'site_name']
#        site_attr['waterbody'] = site_attr.waterbody.str.title()
#        site_attr['site_name'] = site_attr.site_name.str.title()
#        site_attr['site_name'] = site_attr.site_name.str.replace(' \(Recorder\)', '')
#        site_attr['site_name'] = site_attr.site_name.str.replace('Sh', 'SH')
#        site_attr['site_name'] = site_attr.site_name.str.replace('Ecs', 'ECS')
#        site_attr.loc[:, 'site'] = to_numeric(site_attr.loc[:, 'site'], errors='ignore')
#
#        return(site_attr.set_index('site'))
#
#    ### Run through all sites
#    site_attr = self.site_attr
#    site_bool = site_attr[site_attr_lst[0]].isnull()
##    if any(site_bool):
##        mis_sites = site_bool.index[site_bool].tolist()
##        site_attr_new = rd_sw_attr(mis_sites)
##        site_attr = site_attr_new.combine_first(site_attr)
##        site_bool = site_attr[site_attr_lst[0]].isnull()
#    if any(site_bool):
#        mis_sites = site_bool.index[site_bool].tolist()
#        site_attr_new = rd_ecan_attr(mis_sites)
#        site_attr = site_attr_new.combine_first(site_attr)
#        site_bool = site_attr[site_attr_lst[0]].isnull()
#
#    setattr(self, 'site_attr', site_attr)
#
#    if any(site_bool):
#        mis_sites = site_bool.index[site_bool].tolist()
#        print('Missing ' + str(mis_sites) + ' attributes')
#    else:
#        print('Found all of the sites!')

def rd_site_geo_attr(sites):
    geo_attr = rd_sql('SQL2012DEV01', 'Hydro', 'site_geo_attr', where_col='site', where_val=sites)
    return(geo_attr)


def get_site_geo_attr(self):

    sites = Series(self.sites)

    ### get existing site attributes
    if hasattr(self, 'site_geo_attr'):
        old_attr1 = self.site_geo_attr
        old_sites = old_attr1.index
        old_attr = old_attr1[old_attr1.index.isin(sites)]
        sites = sites[~sites.isin(old_sites)]

    ## get new site attr
    if len(sites) > 0:

        sites_str = map(str, sites)
        site_attr = rd_site_geo_attr(sites_str)
        site_attr = site_attr.apply(lambda x: to_numeric(x, errors='ignore'), axis=0)
        site_attr.set_index('site', inplace=True)

        if hasattr(self, 'site_geo_attr'):
            site_attr = concat([old_attr, site_attr])

        setattr(self, 'site_geo_attr', site_attr)

        mis_sites = Series(self.sites)[~site_attr.index.isin(self.sites)].values.tolist()

        if len(mis_sites) > 0:
            print('Missing ' + str(mis_sites) + ' geo attributes')
    else:
        setattr(self, 'site_geo_attr', old_attr)


####################################################
### Geo data import

## SQL database connections


def rd_sw_rain_geo(sites=None):
    if sites is not None:
        site_geo = rd_sql('SQL2012PROD05', 'Bgauging', 'RSITES', col_names=['SiteNumber', 'NZTMX', 'NZTMY'], where_col='SiteNumber', where_val=sites)
    else:
        site_geo = rd_sql('SQL2012PROD05', 'Bgauging', 'RSITES', col_names=['SiteNumber', 'NZTMX', 'NZTMY'])

    site_geo.columns = ['site', 'NZTMX', 'NZTMY']
    site_geo.loc[:, 'site'] = to_numeric(site_geo.loc[:, 'site'], errors='ignore')

    site_geo2 = xy_to_gpd(df=site_geo, id_col='site', x_col='NZTMX', y_col='NZTMY')
    site_geo3 = site_geo2.loc[site_geo2.site > 0, :]
    site_geo3.loc[:, 'site'] = site_geo3.loc[:, 'site'].astype('int32')
    return(site_geo3.set_index('site'))


def rd_waps_geo(sites=None):
    if sites is not None:
        site_geo = rd_sql('SQL2012PROD05', 'Wells', 'WELL_DETAILS', ['WELL_NO',  'NZTMX', 'NZTMY'], where_col='WELL_NO', where_val=sites)
    else:
        site_geo = rd_sql('SQL2012PROD05', 'Wells', 'WELL_DETAILS', ['WELL_NO',  'NZTMX', 'NZTMY'])

    site_geo.rename(columns={'WELL_NO': 'site'}, inplace=True)
    index1 = (site_geo.NZTMX > 1300000) & (site_geo.NZTMX < 1700000) & (site_geo.NZTMY > 5000000) & (site_geo.NZTMY < 5400000)
    site_geo0 = site_geo[index1]
    site_geo2 = xy_to_gpd(df=site_geo0, id_col='site', x_col='NZTMX', y_col='NZTMY')
#    site_geo2.loc[:, 'site'] = site_geo2.loc[:, 'site'].str.upper().str.replace(' ', '')
#    site_geo2 = site_geo2.drop_duplicates()
    site_geo2.loc[:, 'site'] = to_numeric(site_geo2.loc[:, 'site'], errors='ignore')

    return(site_geo2.set_index('site'))


def rd_niwa_geo():
    site_geo = rd_sql('SQL2012PROD05', 'GIS', 'NIWA_NZTM_NIWA_STATIONS', col_names=['gml_id'], geo_col=True)
    site_geo.loc[:, 'gml_id'] = site_geo.loc[:, 'gml_id'].str.replace('stations.', '')
    site_geo.loc[:, 'gml_id'] = to_numeric(site_geo.loc[:, 'gml_id'], errors='coerse')
    site_geo.columns = ['site', 'geometry']

    return(site_geo.set_index('site'))


def get_geo_loc(self):
    for i in self.mtypes_sites:
        if hasattr(self, 'geo_loc'):
            geo_sites = self.geo_loc.index.tolist()
        else:
            geo_sites = []
        sites = list(self.mtypes_sites[i])
        geo_sites_check = ~Series(sites).isin(geo_sites)
        if any(geo_sites_check):
            sites_sel = Series(sites)[geo_sites_check].values
            if sites_sel.dtype == dtype('int64'):
                sites_sel = sites_sel.astype(int).tolist()
            elif sites_sel.dtype == dtype('O'):
                sites_sel = sites_sel.astype(str).tolist()
            else:
                sites_sel = sites_sel.tolist()
            if i in ['flow', 'swl', 'swl_m', 'flow_m', 'precip']:
                geo1 = rd_sw_rain_geo(sites_sel)
                if hasattr(self, 'geo_loc'):
                    self.geo_loc = concat([self.geo_loc, geo1])
                else:
                    setattr(self, 'geo_loc', geo1)
            elif i in ['gwl', 'gwl_m', 'usage']:
                geo1 = rd_waps_geo(sites_sel)
                if hasattr(self, 'geo_loc'):
                    self.geo_loc = concat([self.geo_loc, geo1])
                else:
                    self.geo_loc = geo1

    sites_mis_bool = ~Series(self.sites).isin(self.geo_loc.index)
    if any(sites_mis_bool):
        sites_mis = Series(self.sites)[sites_mis_bool].tolist()
        print('Missing ' + str(sites_mis) + ' site(s)')
    else:
        print('Found all of the sites!')



#####################################################
#### Import time series data


def _rd_hydstra(self, sites, start_time=0, end_time=0, datasource='A', data_type='mean', varfrom=100, varto=140, interval='day', multiplier=1, min_qual=30):
    from core.ecan_io import rd_hydstra_db
    from core.classes.hydro import hydro

    ### Create dict to map the mtype to a hydstra variable
    mtypes_dict = {140: 'flow', 100: 'swl'}
    mtype = mtypes_dict[varto]

    ### Pull data from hydstra and format
    df1 = rd_hydstra_db(sites, start_time, end_time, datasource, data_type, varfrom, varto, interval, multiplier, min_qual)
    df2 = df1.stack().reset_index()
    df2.columns = ['time', 'site', 'data']
    df2['mtype'] = mtype

    ### Create new hydro class
    self.add_data(df2, 'time', 'site', 'mtype', 'data', 'long')
    return(self)


def _rd_hydrotel(self, sites, input_type='name', mtype='Flow', resample=False, period='day', n_periods=1, fun='mean'):
    """
    Need to update!!! The mtypes in hydrotel are inconsistent!!!
    """
    from core.ecan_io.flow import rd_hydrotel

    ### Mtype conversion
    mtype_dict1 = {'flow_tel': 'Flow'}

    ### Load in hydrotel data
    data = rd_hydrotel(select=sites, input_type=input_type, mtype=mtype, resample=resample, period=period, n_periods=n_periods, fun=fun)
    mtype1 = mtype.lower()
    data2 = data.reset_index()
    data2['mtype'] = mtype1

    ### Load into hydro class
    self.add_data(data2, 'time', 'site', 'mtype', 'value', 'long')
    return(self)


def _rd_henry(self, sites, from_date=None, to_date=None, agg_day=True, min_filter=5):
    from core.ecan_io.flow import rd_henry

    if from_date is None:
        from_date = '1900-01-01'
    if to_date is None:
        to_date = '2100-01-01'

    ### Load in gaugings data
    data = rd_henry(sites, start=from_date, end=to_date, agg_day=agg_day, min_filter=min_filter)
    data['mtype'] = 'flow_m'

    ### Load into hydro class
    self.add_data(data, 'date', 'site', 'mtype', 'flow', 'long')
    return(self)


##########################################
#### The big GET!

#mtypes_sql_dict = {'flow': flow_dict, 'flow_tel': _rd_hydrotel, 'flow_m': _rd_henry, 'swl_tel': _rd_hydrotel, 'swl': swl_dict, 'gwl_tel': _rd_hydrotel, 'gwl': gwl_dict, 'gwl_m': gwl_m_dict, 'usage': (wus_usage_dict, usage_dict)}
mtypes_sql_dict = {'flow': flow_dict, 'flow_m': _rd_henry, 'swl': swl_dict, 'gwl': gwl_dict, 'gwl_m': gwl_m_dict, 'usage': (wus_usage_dict, usage_dict), 'precip': precip_dict}

geo_loc_dict = {'flow': rd_sw_rain_geo, 'flow_m': rd_sw_rain_geo, 'swl': rd_sw_rain_geo, 'gwl': rd_waps_geo, 'gwl_m': rd_waps_geo, 'usage': rd_waps_geo, 'precip': rd_sw_rain_geo}


def get_data(self, mtypes, sites=None, qual_codes=None, from_date=None, to_date=None, buffer_dis=0):
    """
    Primary function to import ecan data into a hydro class.

    mtypes -- str or list of mtypes to be extracted.\n
    sites -- str/list/array of sites to be extracted or a shapefile polygon path to extract all sites within a polygon.\n
    qual_codes -- list of quality codes of the data that should be extracted.\n
    from_date -- str of start date. Examples: '2000-01-01' or '2000-01-01 12:30'.\n
    to_date -- str of end date.\n
    buffer_dis -- If sites is a shapefile str, then a buffer distance in meters (str) can be passed.\n
    min_filter -- If mtypes includes 'flow_m', then extract sites with at least the number of gauges (int).
    """
    from pandas import Series
    from numpy import ndarray

    if not isinstance(mtypes, list):
        if isinstance(mtypes, str):
            mtypes = [str(mtypes)]
        else:
            raise ValueError('mtypes must be a list or a string!')

    if isinstance(sites, str):
        if sites.endswith('.shp'):
            sites = read_file(sites)
    elif isinstance(sites, list):
        sites = [str(i) for i in sites]
    elif isinstance(sites, (Series, ndarray)):
        sites = sites.astype(str)
    else:
        raise ValueError('Must pass a str, list, Series, or array to sites.')

    ## Get the time series data
    h1 = self.copy()
    for i in mtypes:
        if i in mtypes_sql_dict:
            h1 = h1._proc_hydro_sql(geo_loc_dict[i], mtypes_sql_dict, i, sites=sites, from_date=from_date, to_date=to_date, qual_codes=qual_codes, buffer_dis=buffer_dis)

    ## Find all of the locations
    h1.get_geo_loc()

    ## Find all of the site_geo_attr
#    if any(in1d(['flow', 'flow_m', 'precip', 'gwl_m', 'usage'], mtypes)):
#        h1.get_site_geo_attr() ## Need to update!

    ## Return
    return(h1)















