"""
Functions to read in Hydstra data. Requires a 32bit python environment.
"""

import ctypes
import os
import contextlib
# from core.ecan_io.mssql import rd_sql
from numpy import array_split, ceil
from pandas import to_numeric, to_datetime, concat, DataFrame, Timestamp
from core.misc.misc import select_sites, save_df


# def rd_hydstra_by_var(varto, start=0, end=0, data_type='mean', interval='day', multiplier=1, min_qual=None,
#                       sites_chunk=20, return_qual=True, print_sites=False, export=False, export_path='flow_data.csv'):
#     """
#     Function to read in data from Hydstra's database using HYDLLP. This function extracts all sites with a specific variable code (varto).
#
#     Parameters
#     ----------
#     start : str or int of 0
#         The start time in the format of either '2001-01-01' or 0 (for all data).
#     end : str or int of 0
#         Same formatting as start.
#     datasource : str
#         Hydstra datasource code (usually 'A').
#     data_type : str
#         mean, maxmin, max, min, start, end, first, last, tot, point, partialtot, or cum.
#     varfrom : int or float
#         The hydstra source data variable (100.00 is water level).
#     varto : int or float
#         The hydstra conversion data variable (140.00 is flow).
#     interval : str
#         The frequency of the output data (year, month, day, hour, minute, second, period). If data_type is 'point', then interval cannot be 'period' (use anything else, it doesn't matter).
#     multiplier : int
#         interval frequency.
#     min_qual : int or None
#         The minimum quality code or None (and there is no screening by quality, suggest exporting qual).
#     return_qual : bool
#         If true returns series, qual_series.
#     sites_chunk : int
#         Number of sites to request to hydllp at one time. Do not change unless you understand what it does.
#
#     Return
#     ------
#     Series
#         In long format with site and time as a MultiIndex.
#     """
#
#     ### Parameters
#     #    numeric_varto = [100, 140, 143, 10, 130]
#
#     server = 'SQL2012PROD03'
#     db = 'Hydstra'
#     period_tab = 'PERIOD'
#     #    var_tab = 'VARIABLE'
#     #    site_tab = 'SITE'
#     #    qual_tab = 'QUALITY'
#
#     period_cols = ['STATION', 'VARFROM', 'VARIABLE', 'PERSTART', 'PEREND', 'NUMPOINTS']
#     period_names = ['site', 'varfrom', 'varto', 'start', 'end', 'num_points']
#     #    var_cols = ['VARNUM', 'VARNAM', 'VARUNIT', 'SHORTNAME']
#     #    var_names = ['var_num', 'var_name', 'var_unit', 'var_short_name']
#     #    site_cols = ['STATION', 'STNAME', 'SHORTNAME']
#     #    site_names = ['site', 'site_name', 'site_short_name']
#     #    qual_cols = ['QUALITY', 'TEXT']
#     #    qual_names = ['qual_code', 'qual_name']
#
#     ## Removals
#     rem_dict = {'165131': [140, 140], '69302': [140, 140], '71106': [140, 140]}
#
#     ### Import
#     period1 = rd_sql(server, db, period_tab, period_cols, where_col='DATASOURCE', where_val=['A'])
#     period1.columns = period_names
#     period1.loc[:, 'site'] = period1.site.str.strip()
#
#     #    var1 = rd_sql(server, db, var_tab, var_cols)
#     #    var1.columns = var_names
#
#     #    site1 = rd_sql(server, db, site_tab, site_cols)
#     #    site1.columns = site_names
#
#     #    qual1 = rd_sql(server, db, qual_tab, qual_cols)
#     #    qual1.columns = qual_names
#
#     ### Determine the variables to extract
#     period2 = period1[period1.varto.isin(period1.varto.round())].sort_values('site')
#     period2 = period2[period2.varto != 101]
#     for i in rem_dict:
#         period2 = period2[
#             ~((period2.site == i) & (period2.varfrom == rem_dict[i][0]) & (period2.varto == rem_dict[i][1]))]
#     #    data_vars1 = period2.varto.sort_values().unique()
#     #    var2 = var1[var1.var_num.isin(data_vars1)]
#
#     ### Extract the sites for the specific varto
#     period3 = period2[period2.varto == varto]
#     varfrom1 = period3.varfrom.unique()
#
#     data = DataFrame()
#     for j in varfrom1:
#         #        if varto in numeric_varto:
#         #            sites1 = to_numeric(period3[period3.varfrom == j].site, 'coerce', 'integer').dropna().values
#         #        else:
#         sites1 = period3[period3.varfrom == j].site.values
#         df = rd_hydstra_db(sites1, data_type=data_type, start=start, end=end, varfrom=j, varto=varto, interval=interval,
#                            multiplier=multiplier, min_qual=min_qual, return_qual=return_qual, sites_chunk=sites_chunk,
#                            print_sites=print_sites)
#         data = concat([data, df])
#
#     ### Make sure the data types are correct
#     data.loc[:, 'qual_code'] = data.qual_code.astype('int32')
#
#     ### Export data
#     if export:
#         data.to_csv(export_path)
#     return (data)


def rd_hydstra_db(sites, start=0, end=0, datasource='A', data_type='mean', varfrom=100, varto=140, interval='day',
                  multiplier=1, qual_codes=[30, 20, 10, 11, 21, 18], report_time=None, sites_chunk=20,
                  print_sites=False, export_path=None):
    """
    Wrapper function over hydllp to read in data from Hydstra's database. Must be run in a 32bit python. If either start_time or end_time is not 0, then they both need a date.

    Parameters
    ----------
    sites : list, array, one column csv file, or dataframe
        Site numbers.
    start : str or int of 0
        The start time in the format of either '2001-01-01' or 0 (for all data).
    end : str or int of 0
        Same formatting as start.
    datasource : str
        Hydstra datasource code (usually 'A').
    data_type : str
        mean, maxmin, max, min, start, end, first, last, tot, point, partialtot, or cum.
    varfrom : int or float
        The hydstra source data variable (100.00 is water level).
    varto : int or float
        The hydstra conversion data variable (140.00 is flow).
    interval : str
        The frequency of the output data (year, month, day, hour, minute, second, period). If data_type is 'point', then interval cannot be 'period' (use anything else, it doesn't matter).
    multiplier : int
        interval frequency.
    qual_codes : list of int
        The quality codes in Hydstra for filtering the data.
    sites_chunk : int
        Number of sites to request to hydllp at one time. Do not change unless you understand what it does.

    Return
    ------
    Series
        In long format with site and time as a MultiIndex.
    """

    ### Process sites into workable chunks
    sites1 = select_sites(sites)
    n_chunks = ceil(len(sites1) / float(sites_chunk))
    sites2 = array_split(sites1, n_chunks)

    ### Run instance of hydllp
    data = DataFrame()
    for i in sites2:
        if print_sites:
            print(i)
        ### Open connection
        hyd = openHyDb()
        with hyd as h:
            df = h.get_ts_traces(i, start=start, end=end, datasource=datasource, data_type=data_type, varfrom=varfrom,
                                 varto=varto, interval=interval, multiplier=multiplier, qual_codes=qual_codes,
                                 report_time=report_time)
        data = concat([data, df])

    if isinstance(export_path, str):
        save_df(data, export_path)

    return (data)


# Define a context manager generator
# that creates and releases the connection to the hydstra server
@contextlib.contextmanager
def openHyDb(ini_path='Y:/Hydstra/prod/hyd/', dll_path='Y:/Hydstra/prod/hyd/sys/run/', username='', password=''):
    hyd = Hydllp(dll_path=dll_path,
                 ini_path=ini_path)
    try:
        hyd.login(username, password)
        yield hyd
    finally:
        hyd.logout()


# Exception for hydstra related errors
class HydstraError(Exception):
    pass


class HydstraErrorUnknown(HydstraError):
    pass


class Hydllp(object):
    def __init__(self,
                 dll_path='Y:/Hydstra/prod/hyd/sys/run/',
                 ini_path='Y:/Hydstra/prod/hyd/',
                 hydllp_filename='hydllp.dll',
                 hyaccess_filename='Hyaccess.ini',
                 hyconfig_filename='HYCONFIG.INI'):

        self._dll_path = dll_path
        self._ini_path = ini_path

        self._dll_filename = os.path.join(self._dll_path, hydllp_filename)
        self._hyaccess_filename = os.path.join(self._ini_path, hyaccess_filename)
        self._hyconfig_filename = os.path.join(self._ini_path, hyconfig_filename)

        # See Hydstra Help file
        # According to the HYDLLP doc, the hydll.dll needs to run "in situ" since
        # it needs to reference other files in that directory.
        os.chdir(self._dll_path)

        # According to the HYDLLP doc, the stdcall calling convention is used.
        self._dll = ctypes.WinDLL(self._dll_filename)

        # Hydstra server handle. Unique to each instance.
        self._handle = ctypes.c_int()

        self._logged_in = False

        # ********************************************************************************

    # Start - Define HYDLLP Wrappers
    # ********************************************************************************

    def _decode_error(self, error_code):
        """
        HYDLLP.dll "DecodeError" function.

        Parameters
        ----------
            -error_code, int
                The error code returned by startup_ex
        """

        # Reference the DecodeError dll function
        decode_error_lib = self._dll['DecodeError']
        decode_error_lib.restype = ctypes.c_int

        # string c_type to store the error message
        error_str = ""
        c_error_str = ctypes.c_char_p(error_str)

        # Allocate memory for the return string
        return_str = ctypes.create_string_buffer(" ", 1400)

        # Call "DecodeError"
        err = decode_error_lib(ctypes.c_int(error_code),
                               c_error_str,
                               ctypes.c_int(1023))
        return return_str.value

    def _start_up_ex(self, user, password, hyaccess, hyconfig):
        """
        HYDLLP.dll "StartUpEx" function

        Parameters
        ----------
            -user, str
                Hydstra username

            -password, str
                Hydstra password

            -hyaccess, str
                Fullpath to HYACCESS.INI

            -hyconfig
                Fullpath to HYCONFIG.INI
        """

        startUpEx_lib = self._dll['StartUpEx']
        startUpEx_lib.restype = ctypes.c_int

        # Call the dll function "StartUpEx"
        err = startUpEx_lib(ctypes.c_char_p(user),
                            ctypes.c_char_p(password),
                            ctypes.c_char_p(hyaccess),
                            ctypes.c_char_p(hyconfig),
                            ctypes.byref(self._handle))
        return err

    def _shutdown(self):
        """
        HYDLLP.dll "ShutDown" function

        Parameters
        ----------
            None
        """

        shutdown_lib = self._dll['ShutDown']
        shutdown_lib.restype = ctypes.c_int

        error_code = shutdown_lib(self._handle)

        # Values other than 0 means that an error occured
        if error_code != 0:
            error_msg = self._decode_error(error_code)
            raise HydstraError(error_msg)

    def _json_call(self, request_str, return_str_len):
        """
        HYDLLP.dll "JsonCall" function
        """

        jsonCall_lib = self._dll['JSonCall']
        jsonCall_lib.restype = ctypes.c_int

        # Allocate memory for the return string
        return_str = ctypes.create_string_buffer(" ", return_str_len)

        # c_return_str = ctypes.c_char_p(return_str)

        # Call the dll function "JsonCall"
        err = jsonCall_lib(self._handle,
                           ctypes.c_char_p(request_str),
                           return_str,
                           return_str_len)

        result = return_str.value
        return (result)

        # ********************************************************************************

    # End - Define HYDLLP Wrappers
    # ********************************************************************************

    def login(self, username, password):
        """
        Logs into hydstra using StartUpEx

        Parameters:
        -----------
            -username, str
                Hydstra username

            -passwords, str
                Hydstra password

        """
        error_code = self._start_up_ex(username,
                                       password,
                                       self._hyaccess_filename,
                                       self._hyconfig_filename)

        # Values other than 0 means that an error occured
        if error_code != 0:
            error_msg = self._decode_error(error_code)
            raise HydstraError(error_msg)

        self._logged_in = True

    def logout(self):
        """
        Log out of hydstra

        Parameters:
        ----------
            None
        """
        if self._logged_in:
            self._shutdown()

    def query_by_dict(self, request_dict):
        """
        Sends and receives request to the hydstra server using hydllp.dll.
        """
        import json

        # initial buffer length
        # If it is too small, we can resize, see below
        buffer_len = 1400

        # convert request dict to a json string
        request_json = json.dumps(request_dict)

        # call json_call and convert result to python dictionary
        result_json = self._json_call(request_json, buffer_len)
        result_dict = json.loads(result_json)

        # If the initial buffer is too small, then re-call json_call
        # with the actual buffer length given by the error response
        if result_dict["error_num"] == 200:
            buffer_len = result_dict["buff_required"]
            result_json = self._json_call(request_json, buffer_len)
            result_dict = json.loads(result_json)

        # If error_num is not 0, then an error occured
        if result_dict["error_num"] != 0:
            error_msg = "Error num:{}, {}".format(result_dict['error_num'],
                                                  result_dict['error_msg'])
            raise HydstraError(error_msg)

        # Just in case the result doesn't have a 'return'
        elif 'return' not in result_dict:
            error_msg = "Error code = 0, however no 'return' was found"
            raise HydstraError(error_msg)

        return (result_dict)

    def get_site_list(self, site_list_exp):
        # Generate a request of all the sites
        site_list_req_dict = {"function": "get_site_list",
                              "version": 1,
                              "params": {"site_list": site_list_exp}}

        site_list_result = self.query_by_dict(site_list_req_dict)

        return (site_list_result["return"]["sites"])

    def get_variable_list(self, site_list, data_source):

        # Convert the site list to a comma delimited string of sites
        site_list_str = ",".join([str(site) for site in site_list])

        var_list_request = {"function": "get_variable_list",
                            "version": 1,
                            "params": {"site_list": site_list_str,
                                       "datasource": data_source}}

        var_list_result = self.query_by_dict(var_list_request)

        return (var_list_result["return"]["sites"])

    def get_db_areas(self, area_classes_list):

        db_areas_request = {"function": "get_db_areas",
                            "version": 1,
                            "params": {"area_classes": area_classes_list}}

        db_area_result = self.query_by_dict(db_areas_request)

        return (db_area_result["return"]["sites"])

    def get_ts_blockinfo(self, site_list, datasources='A', variables=['100', '10', '110', '140', '130', '143', '450'],
                         starttime=0, endtime=0, start_modified=0, end_modified=0, fill_gaps=0, auditinfo=0,
                         report_time='end', offset=0):
        """

        """

        # Convert the site list to a comma delimited string of sites
        sites = select_sites(site_list).astype(str)
        site_list_str = ','.join([str(site) for site in sites])

        get_ts_blockinfo_request = {"function": "get_ts_blockinfo",
                                    "version": 2,
                                    "params": {'site_list': site_list,
                                               'datasources': datasources,
                                               'variables': variables,
                                               'starttime': starttime,
                                               'endtime': endtime,
                                               'start_modified': start_modified,
                                               'end_modified': end_modified,
                                               'fill_gaps': fill_gaps,
                                               'auditinfo': auditinfo
                                               }}

        ts_blockinfo_result = self.query_by_dict(get_ts_blockinfo_request)

        return (ts_blockinfo_result["return"]["sites"])

    def get_ts_traces(self, site_list, start=0, end=0, varfrom=100, varto=140, interval='day', multiplier=1,
                      datasource='A', data_type='mean', qual_codes=[30, 20, 10, 11, 21, 18], report_time=None):
        """

        :param site_list:
        :param start:
        :param end:
        :param varfrom:
        :param varto:
        :param interval:
        :param multiplier:
        :param datasource:
        :param data_type:
        :param qual_codes:
        :param report_time:
        :return:
        """

        # Convert the site list to a comma delimited string of sites
        sites = select_sites(site_list).astype(str)
        site_list_str = ','.join([str(site) for site in sites])

        ### Datetime conversion
        if start != 0:
            start = Timestamp(start).strftime('%Y%m%d%H%M%S')
        if end != 0:
            end = Timestamp(end).strftime('%Y%m%d%H%M%S')

        ts_traces_request = {'function': 'get_ts_traces',
                             'version': 2,
                             'params': {'site_list': site_list_str,
                                        'start_time': start,
                                        'end_time': end,
                                        'varfrom': varfrom,
                                        'varto': varto,
                                        'interval': interval,
                                        'datasource': datasource,
                                        'data_type': data_type,
                                        'multiplier': multiplier,
                                        'report_time': report_time}}

        ts_traces_request = self.query_by_dict(ts_traces_request)
        j1 = ts_traces_request['return']['traces']

        ### Convert json to a dataframe
        sites = [str(f['site']) for f in j1]

        out1 = DataFrame()
        for i in range(len(j1)):
            df1 = DataFrame(j1[i]['trace'])
            if not df1.empty:
                df1.rename(columns={'v': 'data', 't': 'time', 'q': 'qual_code'}, inplace=True)
                df1['data'] = to_numeric(df1['data'], errors='coerce')
                df1['time'] = to_datetime(df1['time'], format='%Y%m%d%H%M%S')
                df1['qual_code'] = to_numeric(df1['qual_code'], errors='coerce', downcast='integer')
                df1['site'] = sites[i]
                df2 = df1[df1.qual_code.isin(qual_codes)]
                out1 = concat([out1, df2])

        out2 = out1.set_index(['site', 'time'])[['data', 'qual_code']]

        return (out2)

# End Class
