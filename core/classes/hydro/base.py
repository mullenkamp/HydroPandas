# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 15:41:08 2017

@author: michaelek
"""

#mtypes_units = {'flow': 'm3/s', 'flow_m': 'm3/s', 'swl': 'masl', 'swl_m': 'masl', 'gwl': 'masl', 'gwl_m': 'masl', 'precip': 'mm', 'R_n': 'MJ/m2', 'R_s': 'MJ/m2', 'G': 'MJ/m2', 'T_min': 'deg C', 'T_max': 'deg C', 'T_mean': 'deg C', 'T_dew': 'deg C', 'RH_min': '%', 'RH_max': '%', 'RH_mean': '%', 'n_sun': 'hours/day', 'U_z': 'm/s', 'P_atmos': 'kPa', 'nitrate': 'mg/l', 'ammonium': 'mg/l', 'chloride': 'mg/l', 'usage': 'm3', 'lakel': 'masl'}
resample_fun = {'flow': 'mean', 'flow_m': 'mean', 'swl': 'mean', 'swl_m': 'mean', 'gwl': 'mean', 'gwl_m': 'mean', 'precip': 'sum', 'R_n': 'sum', 'R_s': 'sum', 'G': 'sum', 'T_min': 'mean', 'T_max': 'mean', 'T_mean': 'mean', 'T_dew': 'mean', 'RH_min': 'mean', 'RH_max': 'mean', 'RH_mean': 'mean', 'n_sun': 'sum', 'U_z': 'mean', 'P_atmos': 'mean', 'nitrate': 'mean', 'ammonium': 'mean', 'chloride': 'mean', 'usage': 'sum', 'lakel': 'mean'}

flow_dict = {'units': 'm3/s', 'resample': 'mean', 'long_name': 'recorder flow', 'description': 'flow rate derrived from corrected surface water level recorders'}
flow_m_dict = {'units': 'm3/s', 'resample': 'mean', 'long_name': 'manually gauged flow', 'description': 'flow rate manually measured during gaugings'}
flow_tel_dict = {'units': 'm3/s', 'resample': 'mean', 'long_name': 'Telemetered flow', 'description': 'flow rate derrived from telemetered surface water level'}
swl_dict = {'units': 'masl', 'resample': 'mean', 'long_name': 'recorder surface water level', 'description': 'surface water level measured via a recording device'}
swl_m_dict = {'units': 'masl', 'resample': 'mean', 'long_name': 'manually gauged surface water level', 'description': 'surface water level manually measured during gaugings'}
gwl_dict = {'units': 'masl', 'resample': 'mean', 'long_name': 'recorder groundwater level', 'description': 'groundwater level measured via a recording device'}
gwl_m_dict = {'units': 'masl', 'resample': 'mean', 'long_name': 'manually gauged groundwater level', 'description': 'groundwater level manually measured during gaugings'}
precip_dict = {'units': 'mm', 'resample': 'sum', 'long_name': 'ECan precipitation', 'description': 'precipitation data from ECan stations'}
precip_tel_dict = {'units': 'mm', 'resample': 'sum', 'long_name': 'ECan precipitation', 'description': 'telemetered precipitation data from ECan stations'}
lakel_dict = {'units': 'masl', 'resample': 'mean', 'long_name': 'recorder lake level', 'description': 'lake level measured via a recording device'}
usage_dict = {'units': 'm', 'resample': 'mean', 'long_name': 'water meter usage', 'description': 'water meter usage data from consent holders'}

all_mtypes = {'flow': flow_dict, 'flow_m': flow_m_dict, 'swl': swl_dict, 'swl_m': swl_m_dict, 'gwl': gwl_dict, 'gwl_m': gwl_m_dict, 'precip': precip_dict, 'usage': usage_dict, 'lakel': lakel_dict, 'flow_tel': flow_tel_dict, 'precip_tel': precip_tel_dict}

class hydro(object):
    """
    A class to handle environmental time series data where a site has a measurement type, a time series, and a location.
    """
    from core.classes.hydro.import_fun import add_geo_loc, missing_geo_loc_sites, add_data, _import_attr, add_geo_catch, _add_geo_data, rd_csv, rd_netcdf, _rd_hydro_mssql, combine, _rd_hydro_geo_mssql, _proc_hydro_sql
    from core.classes.hydro.indexing import sel_ts, sel_sites_by_poly, sel_ts_by_poly, sel, sel_by_poly, __getitem__, _comp_by_buffer, _comp_by_catch, sel_by_geo_attr
    from core.classes.hydro.misc import _check_mtypes_sites, _base_stats_fun, _mtype_check
    from core.classes.hydro.export_fun import to_csv, to_netcdf, to_shp
    from core.classes.hydro.ecan_import import get_geo_loc, _rd_hydstra, _rd_hydrotel, _rd_henry, get_data, get_site_geo_attr
    from copy import copy
    from core.classes.hydro.tools.sw import malf7d, flow_reg
    from core.classes.hydro.tools.general import resample, stats
    from core.classes.hydro.tools.plot import plot_hydrograph, plot_reg
    from core.classes.hydro.tools.gw import gwl_reg

#    @property
#    def _constructor(self):
#        return(hydro)
    ### General attributes

    ### Initial import and assignment function
    def __init__(self, data=None, time=None, sites=None, mtypes=None, values=None, dformat=None):
        if data is None:
            pass
        else:
            ## Read in data
            self.add_data(data=data, time=time, sites=sites, mtypes=mtypes, values=values, dformat=dformat)

    ### Call
#    def __call__(self, data=None, time=None, sites=None, mtypes=None, values=None, dformat=None):
#        self.add_data(data=data, time=time, sites=sites, mtypes=mtypes, values=values, dformat=dformat)

    ### What to return when the oject is called alone
    def __repr__(self):
        if hasattr(self, 'data'):
            if not hasattr(self, '_base_stats'):
                self._base_stats_fun()
            return(repr(self._base_stats))
        else:
            print("There's no data here. Add some in.")





























