# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 14:40:56 2017

@author: MichaelEK
"""

from core.ecan_io import proc_niwa_rcp, export_rcp_nc


##############################################
#### Parameters

#base_path = r'I:\niwa_data\climate_projections'
base_path = r'I:\niwa_data\climate_projections\RCP8.5'
poly = r'E:\ecan\local\Projects\requests\tim_kerr\2017-06-19\study_area.shp'

mtype_name = {'precip': 'TotalPrecipCorr', 'T_max': 'MaxTempCorr', 'T_min': 'MinTempCorr', 'P_atmos': 'MSLP', 'PET': 'PE', 'RH_mean': 'RelHum', 'R_s': 'SurfRad', 'U_z': 'WindSpeed'}

mtypes = mtype_name.keys()

export_path = r'E:\ecan\local\Projects\requests\tim_kerr\2017-06-19\data\RCP8.5'

###########################################
#### Run selection

proc_niwa_rcp(base_path, mtypes, poly, output_fun=export_rcp_nc, export_path=export_path)









##########################################
#### Testing

base_path = r'I:\niwa_data\climate_projections\RCP2.6\BCC-CSM1.1'
base_path = r'I:\niwa_data\climate_projections\RCP6.0\GFDL-CM3'


    ### Prepare the mtypes dict
    p_col = {mtype_param[i]: i for i in mtypes}

    ### Extract the data based on criteria from earlier
    ds3 = ds2.sel(latitude=bool_lat, longitude=bool_lon)
    da1 = ds3[p_col.keys()].round(2)
    df1 = da1.to_dataframe().reset_index()
    df1.rename(columns=rename_dict, inplace=True)
    df1.loc[:, 'x'] = df1.loc[:, 'x'].round(3)
    df1.loc[:, 'y'] = df1.loc[:, 'y'].round(3)

    ## Merge the data with the site id's
    df2 = merge(df1, site_loc, on=['x', 'y'])




t2 = to_datetime(da1.time.data)
t2[t2.duplicated(keep=False)]

t3 = to_datetime(ds10.time.data)
t3[t3.duplicated(keep=False)]





















