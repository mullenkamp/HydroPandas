# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 15:41:08 2017

@author: michaelek
"""
from collections import OrderedDict
from itertools import product, chain
import pandas as pd
from hydropandas.io.tools.mssql import to_mssql


#######################################
### Lists and dictionaries for the master tables and associated data
### Need to be kept up-to-date compared to the SQL server


feature_list = [['River', 'river', 'A flowing freshwater surface water body'],
                ['Aquifer', 'aq', 'An underground layer of water-bearing permeable rock, rock fractures or unconsolidated materials'],
                ['Atmosphere', 'atmos', 'The shpere of vapour surrounding our panet'],
                ['Soil', 'soil', 'The unsaturated zone of unconsolidated sediment directly below our feet'],
                ['Lake', 'lake', 'A freshwater body with little flowing movement']]

mtype_list = [['Water Level', 'wl', 'Quantity', 'meter', 'The water level above an arbitrary datum'],
              ['Flow', 'flow', 'Quantity', 'm**3/s', 'Water flow'],
              ['Temperature', 'T', 'Quality', 'degC', 'Instantaneous temperature'],
              ['Temperature Max', 'T_max', 'Quality', 'degC', 'Max temperature'],
              ['Temperature Min', 'T_min', 'Quality', 'degC', 'Min temperature'],
              ['Temperature Mean', 'T_mean', 'Quality', 'degC', 'Mean temperature'],
              ['Temperature dew', 'T_dew', 'Quality', 'degC', 'Dew point temperature'],
              ['Precipitation', 'precip', 'Quantity', 'mm', 'depth/height of water'],
              ['Abstraction', 'abstr', 'Quantity', 'l/s', 'Volume of water abstracted'],
              ['Net Radiation', 'R_n', 'Quantity', 'MJ/m**2', 'Net Radiation'],
              ['Shortwave Radiation', 'R_s', 'Quantity', 'MJ/m**2', 'Shortwave Radiation'],
              ['Soil heat flux', 'G', 'Quantity', 'MJ/m**2', 'Net soil heat flux'],
              ['Relative Humidity Min', 'RH_min', 'Quality', '', 'Min relative humidity'],
              ['Relative Humidity Max', 'RH_max', 'Quality', '', 'Max relative humidity'],
              ['Relative Humidity Mean', 'RH_mean', 'Quality', '', 'Mean relative humidity'],
              ['Wind Speed', 'U_2', 'Quantity', 'm/s', 'Wind speed at 2m in length per unit time'],
              ['Barometric Pressure', 'P_baro', 'Quantity', 'kPa', 'Barometric Pressure'],
              ['n sun hours', 'n_sun', 'Quantity', 'hour', 'Number of sunshine hours per day'],
              ['ET Actual', 'ET', 'Quantity', 'mm', 'Actual Evapotranspiration'],
              ['ET Reference', 'ETo', 'Quantity', 'mm', 'Reference Evapotranspiration'],
              ['Temperature at 9', 'T_9', 'Quality', 'degC', 'Temperature at 9am'],
              ['Penman ET', 'PenmanET', 'Quantity', 'mm', 'ET estimated via the NIWA Penman method'],
              ['Vapour Pressure', 'P_vap', 'Quantity', 'hectopascals', 'Vapour pressure as calculated from dewpoint Temperature'],
              ['Wind run', 'U_run', 'Quantity', 'km', 'The distance travelled by the wind over the day'],
              ['Wind speed at 9', 'U_2_9', 'Quantity', 'm/s', 'Wind speed at 2m at 9am']]

msource_list = [['Recorder', 'rec', 'Data collected and measured via an automatic recording device'],
                ['Manual Field', 'mfield', 'Data collected and measured manually'],
                ['Manual Lab', 'mlab', 'Data collected in the field, but measured in the lab']]

mtype_sec_list = [['RAW', 'raw', 'Unaltered data'],
                   ['Primary', 'prime', 'Primary data/quality controlled'],
                   ['Synthetic', 'synth', 'Synthetic data']]

feature_mtype_dict = OrderedDict((('river', ('wl', 'flow', 'abstr', 'T')),
                                  ('aq', ('wl', 'abstr', 'T')),
                                  ('atmos', ('precip', 'R_n', 'R_s', 'T_min', 'T_max', 'RH_min', 'RH_max', 'RH_mean', 'U_z', 'P', 'T', 'T_mean', 'T_dew', 'n_sun')),
                                  ('soil', ('T', 'ETo', 'ET', 'G')),
                                  ('lake', ('wl', 'T', 'abstr'))))

resample_mtype_fun = {'flow': 'mean', 'wl': 'mean', 'precip': 'sum', 'R_n': 'sum', 'R_s': 'sum', 'T_min': 'mean', 'T_max': 'mean', 'T': 'mean', 'RH_min': 'mean', 'RH_max': 'mean', 'U_z': 'mean', 'P': 'mean', 'abstr': 'sum', 'ET': 'sum', 'ETo': 'sum', 'G': 'sum', 'T_mean': 'mean', 'T_dew': 'mean', 'RH_mean': 'mean', 'n_sun': 'sum'}

#####################################
### Create master tables

feature_df = pd.DataFrame(feature_list, columns=['FeatureLongName', 'FeatureShortName', 'Description'])
feature_df.set_index('FeatureShortName', inplace=True)

mtype_df = pd.DataFrame(mtype_list, columns=['MTypeLongName', 'MTypeShortName', 'MTypeGroup', 'Units', 'Description'])
mtype_df.set_index('MTypeShortName', inplace=True)

msource_df = pd.DataFrame(msource_list, columns=['MSourceLongName', 'MSourceShortName', 'Description'])
msource_df.set_index('MSourceShortName', inplace=True)

mtype_sec_df = pd.DataFrame(mtype_sec_list, columns=['MtypeSecLongName', 'MtypeSecShortName', 'Description'])
mtype_sec_df.set_index('MtypeSecShortName', inplace=True)

## Save to SQL server
#server = 'SQL2012DEV01'
#database = 'Hydro'
#
#to_mssql(feature_df, server, database, 'FeatureMaster', True)
#to_mssql(mtype_df, server, database, 'MtypeMaster', True)
#to_mssql(msource_df, server, database, 'MSourceMaster', True)
#to_mssql(qual_state_df, server, database, 'QualityStateMaster', True)


#######################################
### Hydro IDs table and the resampling function dict

fields_list1 = [list(product([i], feature_mtype_dict[i], msource_df.index.tolist(), mtype_sec_df.index.tolist())) for i in feature_mtype_dict]

fields_list = list(chain.from_iterable(fields_list1))

hydro_ids = OrderedDict((' / '.join(i), i) for i in fields_list)

resample_fun = OrderedDict((' / '.join(i), resample_mtype_fun[i[1]]) for i in fields_list)

all_hydro_ids = pd.DataFrame(fields_list, index=hydro_ids.keys(), dtype='category')
all_hydro_ids.columns = ['feature', 'mtype', 'msource', 'mtype_sec']
all_hydro_ids.index.name = 'hydro_id'


######################################
### The main class


class Hydro(object):
    """
    A class to handle environmental time series data where a site has a measurement type, a time series, and a location.
    """
#    from core.classes.hydro.import_fun import add_geo_loc, missing_geo_loc_sites, add_data, _import_attr, add_geo_catch, _add_geo_data, rd_csv, rd_netcdf, _rd_hydro_mssql, combine, _rd_hydro_geo_mssql, _proc_hydro_sql
#    from core.classes.hydro.indexing import sel_ts, sel_sites_by_poly, sel_ts_by_poly, sel, sel_by_poly, __getitem__, _comp_by_buffer, _comp_by_catch, sel_by_geo_attr
#    from core.classes.hydro.misc import _check_mtypes_sites, _mtype_check
#    from core.classes.hydro.export_fun import to_csv, to_netcdf, to_shp
#    from core.classes.hydro.ecan_import import get_geo_loc, _rd_hydstra, _rd_hydrotel, _rd_henry, get_data, get_site_geo_attr
    from copy import copy
#    from core.classes.hydro.tools.sw import malf7d, flow_reg
#    from core.classes.hydro.tools.general import resample, stats
#    from core.classes.hydro.tools.plot import plot_hydrograph, plot_reg
#    from core.classes.hydro.tools.gw import gwl_reg
    from hydropandas.io.import_base import add_tsdata, rd_csv, combine, add_geo_point, add_geo_catch, _add_geo_data, _check_geo_sites, _check_crs, missing_geo_point_sites, rd_hdf
    from hydropandas.core.indexing import sel_ts, sel, sel_sites_by_poly, sel_ts_by_poly, sel_by_poly, __getitem__, _comp_by_buffer, _comp_by_catch
    from hydropandas.io.export_base import to_csv, to_hdf, to_shp
    from hydropandas.util.unit_conversion import to_units

#    @property
#    def _constructor(self):
#        return(hydro)
    ### General attributes

    ### Initial import and assignment function
    def __init__(self, data=None):
        if data is None:
            pass


    ### Call
#    def __call__(self, data=None, time=None, sites=None, mtypes=None, values=None, dformat=None):
#        self.add_data(data=data, time=time, sites=sites, mtypes=mtypes, values=values, dformat=dformat)

    ### What to return when the oject is called alone
    def __repr__(self):
        if hasattr(self, 'tsdata'):
            if not hasattr(self, '_base_stats'):
                self._base_stats_fun()
            return(repr(self._base_stats))
        else:
            print("There's no data here. Add some in.")


    ### Base stats for the default view of the class (once data has been loaded)

    def _base_stats_fun(self):
        grp1 = self.tsdata['value'].groupby(level=['hydro_id', 'site'])
        start = grp1.apply(lambda x: x.first_valid_index()[2])
        start.name = 'start_time'
        end = grp1.apply(lambda x: x.last_valid_index()[2])
        end.name = 'end_time'
        stats1 = grp1.describe()[['min', '25%', '50%', '75%', 'mean', 'max', 'count']].round(2)
        out1 = pd.concat([stats1, start, end], axis=1)
        setattr(self, '_base_stats', out1)

    ### Add in additional attributes
#    setattr(self, 'all_hydro_ids', all_hydro_ids)
#    setattr(self, 'mtypes', mtype_df)
    hydro_ids = all_hydro_ids
    mtypes = mtype_df


