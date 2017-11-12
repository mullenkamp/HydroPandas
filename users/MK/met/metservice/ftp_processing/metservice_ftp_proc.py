# -*- coding: utf-8 -*-
"""
Created on Fri Jul 07 09:09:11 2017

@author: MichaelEK

Script to process the metservice netcdf files.
"""

from core.ts.met.metservice import proc_metservice_nc, MetS_nc_to_df, metconnect_id_loc
from core.spatial import sel_sites_poly, point_interp_ts
from pandas import to_datetime
from configparser import ConfigParser
from os import path, getcwd
from core.misc import rd_dir, logging
from core.ecan_io import write_sql, rd_sql

##########################################

#py_dir = r'E:\ecan\git\Ecan.Science.Python.Base\users\MK\met\metservice\ftp_processing'
#file1 = 'metservice_ftp_proc.py'

### Load in ini parameters

py_dir = path.realpath(path.join(getcwd(), path.dirname(__file__)))

ini1 = ConfigParser()
ini1.read([path.join(py_dir, path.splitext(__file__)[0] + '.ini')])
#ini1.read([path.join(py_dir, path.splitext(file1)[0] + '.ini')])

nc_dir = ini1.get('Input', 'nc_dir')
server = ini1.get('Output', 'server')
database = ini1.get('Output', 'database')
data_table = ini1.get('Output', 'sites_table')
nc_out_dir = ini1.get('Output', 'nc_output_dir')
log_path = ini1.get('Output', 'log_path')

## Processing parameters

point_site_col = 'MetConnectID'
time_col = 'time'
y_col = 'y'
x_col = 'x'
data_col = 'precip'
buffer_dis = 16000
digits = 2

## Output parameters

rename_dict = {'site': 'MetConnectID', 'model_date': 'PredictionDateTime', 'time': 'ReadingDateTime', 'precip': 'HourlyRainfall'}

dtype_dict = {'MetConnectID': 'INT', 'PredictionDateTime': 'smalldatetime', 'ReadingDateTime': 'smalldatetime', 'HourlyRainfall': 'decimal(18,2)'}


#######################################
### Read in site locations
print('Read in site locations')

points = metconnect_id_loc()

########################################
### Check the model of the day! And see if we already have a record.
print('Check the model of the day! And see if we already have a record.')

txt = rd_dir(nc_dir, 'txt')[-1]
model1 = open(path.join(nc_dir, txt), 'r').readline()

nc1 = rd_dir(nc_dir, 'nc')

nc_model = [i for i in nc1 if model1 in i][-1]
model_date = to_datetime(nc_model[-16:-6], format='%Y%m%d%H')

last_predict_date = rd_sql(server, database, stmt = str("select max(PredictionDateTime) from " + data_table)).loc[0][0]

if model_date == last_predict_date:
    print('Already have the data for this forecast...closing...')
    logging(log_path, str(last_predict_date) + ' was the last model date in the database table. It is the lastest date on the ftp site. Nothing was updated.')
    raise SystemExit
else:
    print("We don't have the newest data in the database table...processing data now...")

#########################################
### Process data

## preprocess the nc file to save it as a proper nc file
new_nc = proc_metservice_nc(path.join(nc_dir, nc_model), export_dir=nc_out_dir)

## Extract the data from the new nc file
precip, sites, start_date = MetS_nc_to_df(new_nc)

## Select the precip data within the buffer area of the points shp
#points = read_file(point_shp)
points_poly1 = points.to_crs(sites.crs).buffer(buffer_dis)
sites2 = sel_sites_poly(sites, points_poly1)

precip2 = precip[precip.site.isin(sites2.site)]

## Interpolate the data at the shp points
data = point_interp_ts(precip2, time_col, x_col, y_col, data_col, points, point_site_col, sites.crs, to_crs=None, interp_fun='cubic', agg_ts_fun=None, period=None, digits=2)

########################################
### Output data
print('Saving data')

## Reformat
data.loc[:, 'model_date'] = start_date
data1 = data[['site', 'model_date', 'time', 'precip']].copy()
data2 = data1.rename(columns=rename_dict)

##Output
write_sql(server, database, data_table, data2, dtype_dict, False, False)

print('Success!')
logging(log_path, str(last_predict_date) + ' was the last model date in the database table. ' + str(model_date) + ' is the new model date. ' + str(len(points)) + ' sites were updated.')


