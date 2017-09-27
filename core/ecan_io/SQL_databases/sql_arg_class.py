# -*- coding: utf-8 -*-
"""
Created on Mon Sep 25 11:10:04 2017

@author: MichaelEK
"""

#################################################################
### Helper functions


def _get_vars(dict1):
    v1 = [i for i in dict1 if '__' not in i]
    return(v1)

##################################################################
### Class dictionary containers

class consents(object):
    """
    Consents data.
    """

    ## Arg dictionaries

    crc_dates = {'server': 'SQL2012PROD03', 'database': 'DataWarehouse', 'table': 'F_ACC_Permit', 'col_names': ['B1_ALT_ID', 'fmDate', 'toDate', 'Longitude', 'Latitude', 'StatusType', 'B1_APPL_STATUS'], 'rename_cols': ['crc', 'from_date', 'to_date', 'wgs84_x', 'wgs84_y', 'status', 'status_details']}
    crc_relation = {'server': 'SQL2012PROD03', 'database': 'DataWarehouse', 'table': 'D_ACC_Relationships', 'col_names': ['ParentRecordNo', 'ChildRecordNo'], 'rename_cols': ['crc', 'crc_child']}
    crc_wap_allo = {'server': 'SQL2012PROD03', 'database': 'DataWarehouse', 'table': 'D_ACC_Act_Water_TakeWaterWAPAlloc', 'col_names': ['RecordNo', 'Activity', 'Allocation Block', 'ROW_INDEX', 'WAP', 'Max Rate for WAP (l/s)', 'Max Rate Pro Rata (l/s)', 'Max Vol Pro Rata (m3)', 'Consecutive Day Period', 'Include in SW Allocation?', 'First Stream Depletion Rate', 'From Month'], 'rename_cols': ['crc', 'take_type', 'allo_block', 'row_index', 'wap', 'max_rate_wap', 'max_rate', 'max_vol', 'return_period', 'in_sw_allo', 'sd', 'from_month']}
    crc_cav = {'server': 'SQL2012PROD03', 'database': 'DataWarehouse', 'table': 'D_ACC_Act_Water_TakeWaterConsent', 'col_names': ['RecordNo', 'Activity', 'Consented Annual Volume (m3/year)', 'Has a low flow restriction condition?'], 'rename_cols': ['crc', 'take_type', 'cav', 'min_flow']}
    crc_use_type = {'server': 'SQL2012PROD03', 'database': 'DataWarehouse', 'table': 'D_ACC_Act_Water_TakeWaterAllocData', 'col_names': ['RecordNo', 'Activity', 'Allocation Block', 'ROW_INDEX', 'Include in GW Allocation?', 'Water Use', 'Irrigation Area (ha)', 'Full Effective Annual Volume (m3/year)'], 'rename_cols': ['crc', 'take_type', 'allo_block', 'row_index', 'in_gw_allo', 'use_type', 'irr_area', 'feav']}


class water_use(object):
    """
    Water usage data.
    """

    wus_mon = {'server': 'SQL2012PROD03', 'database': 'WUS', 'table': 'vw_WUS_Fact_MonthlyUsageByUsageSite_MsAccess', 'col_names': ['UsageSite', 'Year', 'Month', 'Usage'], 'rename_cols': ['wap', 'year', 'month', 'usage']}
    wus_day = {'server': 'SQL2012PROD03', 'database': 'WUS', 'table': 'vw_WUS_Fact_DailyUsageByUsageSite', 'col_names': ['UsageSite', 'Day', 'Usage'], 'rename_cols': ['wap', 'date', 'usage']}




##################################################################
### Main class call

class sql_arg(object):
    """
    Class container to retreive dictionaries to pass to rd_sql.
    """
    from core.ecan_io.SQL_databases.sql_arg_class import _get_vars, consents, water_use

    _code_dict = {'consents': consents, 'water_use': water_use}

    def __init__(self):
        classes = {i: _get_vars(self._code_dict[i].__dict__) for i in self._code_dict}
        codes = []
        [codes.extend(classes[i]) for i in classes]
        self.classes = classes
        self.codes = codes

    def get_dict(self, code=None, fields=None, minimum=False):
        """
        Main function to retreive dictionaries to pass to rd_sql.

        Arguments:
        code -- Special dictionary code to retreive the dictionary parameters for SQL (str).\n
        fields -- The dictionary keys that should be retreived (str or list). None returns all fields.\n
        minimum -- Should only the server, database, and table values be returned?
        """
        if isinstance(code, str):
            if code in self.codes:
                class1 = [i for i in self.classes if code in self.classes[i]][0]
                class2 = getattr(self, class1)
                dict1 = getattr(class2, code)
                if fields is not None:
                    if isinstance(fields, str):
                        fields = [fields]
                    if isinstance(fields, list):
                        dict1 = {i: dict1[i] for i in fields if i in dict1.keys()}
                if minimum:
                    fields = ['server', 'database', 'table']
                    dict1 = {i: dict1[i] for i in fields if i in dict1.keys()}

                return(dict1)

    def __repr__(self):
        return(repr(self.classes))



































