"""
Author: matth
Date Created: 22/06/2017 3:53 PM
"""

from __future__ import division
from core import env
import numpy as np
import os
from warnings import warn

#todo set up exceptions is required varibles are needed

class ModelTools(object):
    ulx = None
    uly = None
    grid_space = None
    layers = None
    rows = None
    cols = None
    sdp = None
    no_flow = None
    temp_file_dir = None
    elv_path = None
    model_version_name = None
    base_mod_path = None

    def __init__(self, model_version_name, sdp=None, ulx=None, uly=None, layers=None, rows=None, cols=None,
                 grid_space=None, no_flow=None, temp_file_dir=None, elv_path=None, base_mod_path=None):
        self.ulx = ulx
        self.uly = uly
        self.grid_space = grid_space
        self.layers = layers
        self.rows = rows
        self.cols = cols
        self.sdp = sdp
        if not os.path.exists(sdp):
            os.makedirs(sdp)
        self.no_flow = no_flow
        self.temp_file_dir = temp_file_dir
        self.elv_path = elv_path
        self.model_version_name = model_version_name
        self.pickle_dir = '{}/pickled_files'
        if not os.path.exists(self.pickle_dir):
            os.makedirs(self.pickle_dir)
        self.base_mod_path = base_mod_path

    def get_model_x_y(self):
        pixelWidth = pixelHeight = 200

        x_min, y_min = self.ulx, self.uly - self.grid_space * self.rows

        # mid points of the array
        x = np.linspace(x_min + 100, (x_min + 100 + pixelWidth * (self.cols - 1)), self.cols)
        y = np.linspace((y_min + 100 + pixelHeight * (self.rows - 1)), y_min + 100, self.rows)

        model_xs, model_ys = np.meshgrid(x, y)
        return model_xs, model_ys

    def get_empty_model_grid(self, _3d=False):
        if _3d:
            return np.zeros(self.layers,self.rows,self.cols)
        else:
            return np.zeros(self.layers,self.rows,self.cols)

    def model_where(self,condition):
        """
        quick function to return list of model indexes from a np.where style condition assumes that the array in the
        condition has the shape (i,j,k) or (j,k)
        :param condition:
        :return:
        """
        idx = np.where(condition)
        idx2 = [i for i in zip(*idx)]
        return idx2

    def df_to_array(self, dataframe, value_to_map, _3d=False):
        """
        maps a dataframe to a model array
        :param dataframe: dataframe with at least keys (k),i,j and value
        :param value_to_map: key to the value to map onto the array
        :param _3d: boolean if true map to a 3d array with kIJ
        :return: array
        """
        if _3d:
            if not np.in1d(['k', 'i', 'j', value_to_map], dataframe.keys()).all():
                raise ValueError('dataframe missing keys')
            outdata = np.zeros((self.layers, self.rows, self.cols))
            for i in dataframe.index:
                layer, row, col = dataframe.loc[i, ['k', 'i', 'j']]
                outdata[layer, row, col] = dataframe.loc[i, value_to_map]
        else:
            if not np.in1d(['i', 'j', value_to_map], dataframe.keys()).all():
                raise ValueError('dataframe missing keys')
            outdata = np.zeros((self.rows, self.cols))
            for i in dataframe.index:
                row, col = dataframe.loc[i, ['i', 'j']]
                outdata[row, col] = dataframe.loc[i, value_to_map]

        return outdata

    def convert_matrix_to_coords(self, row, col, layer=None, elv_db=None):
        """
        convert from matix indexing to real world coordinates
        :param row:
        :param col:
        :param layer:
        :param elv_db: 3d numpy array of bottom elevation the 0th index is the top layer of the surface of the model
        :return: lon, lat if layer is not specified or lon, lat, elv
        """
        if elv_db is None and layer is not None:
            # below is a pickled nparray of bottoms (with layer 1 top on the top of the stack
            elv_db = self.calc_elv_db()

        col = int(col)
        row = int(row)
        model_xs, model_ys = self.get_model_x_y()
        lon = model_xs[0, col]
        lat = model_ys[row, 0]

        if layer is None:
            return lon, lat
        else:
            elv = elv_db[layer:layer + 2, row, col].mean()
            return lon, lat, elv

    def convert_coords_to_matix(self, lon, lat, elv=None, elv_db=None,
                                return_AE=False):
        """
        convert from real world coordinates to model indexes by comparing value to center point of array.  Where the value
        is on an edge defaults to top left corner (e.g. row 1, col 1, layer 1)
        :param lon: longitude NZTM
        :param lat: latitude NZTM
        :param elv: elevation Lyttleton 43 MSL #not sure
        :param elv_db: 3d numpy array of bottom elevation the 0th index is the top layer of the surface of the model
        :param return_AE: if true return (layer,z0), (row, y0), (col, x0) where x0 ect are the preportion along the cell
                          for use with modpath particles
        :return: (row, col) if elv is not presented or (layer, row, col)
        """
        model_xs, model_ys = self.get_model_x_y()
        if lon > model_xs[0, :].max() or lon < model_xs[0, :].min():
            raise ValueError('coords not in model domain')

        if lat > model_ys[:, 0].max() or lat < model_ys[:, :].min():
            raise ValueError('coords not in model domain')

        layer, row, col = None, None, None
        if elv_db is None and elv is not None:
            # below is a pickled np.array of bottoms (with layer 1 top on the top of the stack)
            elv_db = self.calc_elv_db()
        # find the index of the closest middle point
        # convert lon to col
        col = np.abs(model_xs[0, :] - lon).argmin()

        # convert lat to row
        row = np.abs(model_ys[:, 0] - lat).argmin()

        if return_AE:
            x0 = (100 - (model_xs[0, :] - lon)[col]) / 200
            y0 = (100 - (model_ys[:, 0] - lat)[row]) / 200

        if elv is None:
            if return_AE:
                return (row, y0), (col, x0)
            else:
                return row, col
        else:
            # convert elv to layer
            top = elv_db[:-1, row, col]
            bottom = elv_db[1:, row, col]
            if elv > top.max():
                warn('above top of database setting layer to zero')
                layer = 0
                z0 = 0
            else:
                layer = np.where((top > elv) & (bottom <= elv))
                if len(layer[0]) != 1:
                    raise ValueError(
                        'returns multiple indexes for layer')
                layer = layer[0][0]
                if return_AE:
                    z0 = (top[layer] - elv) / (top[layer] - bottom[layer])
            if return_AE:
                return (layer, z0), (row, y0), (col, x0)
            else:
                return layer, row, col

    def get_all_adjacent_cells(self, cell_locs):

        cell_locs = np.atleast_2d(cell_locs)

        out_locs = []
        for loc in cell_locs:
            out_locs.append(loc)

            k = loc[0]
            i = loc[1]
            j = loc[2]

            if k != 17:
                out_locs.append((k + 1, i, j))
            if k != 0:
                out_locs.append((k - 1, i, j))
            if i != 190:
                out_locs.append((k, i + 1, j))
            if i != 0:
                out_locs.append((k, i - 1, j))
            if j != 365:
                out_locs.append((k, i, j + 1))
            if j != 0:
                out_locs.append((k, i, j - 1))

        return out_locs

    def array_to_raster(self, path, array, only_active=True):
        from osgeo import osr, gdal

        if only_active:
            array[self.no_flow] = -99
        output_raster = gdal.GetDriverByName('GTiff').Create(path, array.shape[1], array.shape[0], 1,
                                                             gdal.GDT_Float32)  # Open the file
        geotransform = (self.ulx, self.grid_space, 0, self.uly, 0, -self.grid_space)
        output_raster.SetGeoTransform(geotransform)  # Specify its coordinates
        srs = osr.SpatialReference()  # Establish its coordinate encoding
        srs.ImportFromEPSG(2193)  # This one specifies NZTM.
        output_raster.SetProjection(srs.ExportToWkt())  # Exports the coordinate system
        # to the file
        band = output_raster.GetRasterBand(1)
        band.WriteArray(array)  # Writes my array to the raster
        band.FlushCache()
        band.SetNoDataValue(-99)

    def plt_matrix(self, array, vmin=None, vmax=None, **kwargs):
        import matplotlib.pyplot as plt
        if vmax is None:
            vmax = np.nanmax(array)
        if vmin is None:
            vmin = np.nanmin(array)

        fig, (ax) = plt.subplots(figsize=(18.5, 9.5))
        ax.set_aspect('equal')
        model_xs, model_ys = self.get_model_x_y()
        pcm = ax.pcolormesh(model_xs, model_ys, array,
                            cmap='plasma', vmin=vmin, vmax=vmax, **kwargs)
        fig.colorbar(pcm, ax=ax, extend='max')
        if self.no_flow is not None:
            ax.contour(model_xs, model_ys, self.no_flow)

        return fig, ax


    def geodb_to_model_array(self, path, shape_name, attribute, alltouched=False):
        """
        create model array from a shapefile in a geodatabase
        :param path: path to geodatabase
        :param shape_name: name of the shape file
        :param attribute: name of the attribute to convert to matrix
        :return: np array of the matrix
        """
        from osgeo import ogr
        driver = ogr.GetDriverByName("OpenFileGDB")
        ds = driver.Open(path, 0)
        source_layer = ds.GetLayer(shape_name)

        outdata = self._layer_to_model_array(source_layer, attribute, alltouched=alltouched)
        return outdata

    def shape_file_to_model_array(self, path, attribute, alltouched=False):
        """
        shape file to vistas array
        :param path: path to shapefile
        :param attribute: attribute name to convert
        :return:  np.array of the data in model format
        """
        # open shape file
        from osgeo import ogr
        source_ds = ogr.Open(path)
        source_layer = source_ds.GetLayer()

        outdata = self._layer_to_model_array(source_layer, attribute, alltouched=alltouched)
        return outdata

    def _layer_to_model_array(self, source_layer, attribute, alltouched=False):
        """
        hidden function to convert a source layer to a rasterized np array
        :param source_layer: from either function above
        :param attribute: attribute to convert.
        :return:  np array of rasterized data
        """
        from osgeo import gdal, osr
        if not os.path.exists(self.temp_file_dir):
            os.makedirs(self. temp_file_dir)

        pixelWidth = pixelHeight = 200  # depending how fine you want your raster

        x_min, y_min = self.ulx, self.uly - self.grid_space * self.rows

        target_ds = gdal.GetDriverByName('GTiff').Create('{}/temp.tif'.format(self.temp_file_dir), self.cols, self.rows, 1,
                                                         gdal.GDT_Float64)
        target_ds.SetGeoTransform((x_min, pixelWidth, 0, y_min, 0, pixelHeight))
        band = target_ds.GetRasterBand(1)
        NoData_value = -999999
        band.SetNoDataValue(NoData_value)
        band.FlushCache()
        if alltouched:
            opt = ["ALL_TOUCHED=TRUE", "ATTRIBUTE={}".format(attribute)]
        else:
            opt = ["ATTRIBUTE={}".format(attribute)]
        gdal.RasterizeLayer(target_ds, [1], source_layer, options=opt)

        target_dsSRS = osr.SpatialReference()
        target_dsSRS.ImportFromEPSG(2193)
        target_ds.SetProjection(target_dsSRS.ExportToWkt())
        target_ds = None

        outdata = gdal.Open('{}/temp.tif'.format(self.temp_file_dir)).ReadAsArray()
        outdata = np.flipud(outdata)
        outdata[np.isclose(outdata, -999999)] = np.nan
        return outdata

    def get_well_postions(self, well_nums, screen_handling='middle', raise_exct=True, error_log_path=None,
                          one_val_per_well=False):
        """
        gets model indexs from a well position
        :param well_nums: value or iterable of well numbers
        :param screen_handling: how to calculate elevation from wells with screen details
                                middle: returns all unique layers for the set
                                all: returns all unique layers intersected by screens(e.g. from top and bottom elevation)
        :param raise_exct: raise exceptions or continue, see error_log_path for handeling if False
        :param error_log_path: path to error log where values are written if raise_exct is True.  if None, exceptions passed
                                as warnings
        :param one_val_per_well: if True, lists of layer values will be condensed and the most frequent layer value will be
                                 returned ensuring there is only one elevation per well. Where there is a tie the lowest
                                 index layer (e.g. the smallest number) of the tied values will be returned
        :return:
        """
        from core.ecan_io import rd_sql, sql_db
        import pandas as pd

        if error_log_path is not None:
            f = open(error_log_path, 'w')

        # check inputs
        if screen_handling not in ['middle', 'all']:
            raise ValueError('screen_handling not appropriately specified')

        if isinstance(well_nums, str):
            return_value = True
        else:
            return_value = False

        # load supporting information
        # below is a pickled nparray of bottoms (with layer 1 top on the top of the stack
        elv_db = self.calc_elv_db()
        well_details = rd_sql(**sql_db.wells_db.well_details)
        well_details = well_details.set_index('WELL_NO')
        screen_details = rd_sql(**sql_db.wells_db.screen_details)

        well_nums = np.atleast_1d(well_nums)

        if well_nums.ndim != 1:
            raise ValueError('unexpected input dimensions')

        # initalize output arrays
        layer = np.zeros(well_nums.shape, dtype=object) * np.nan
        row = np.zeros(well_nums.shape, dtype=object) * np.nan
        col = np.zeros(well_nums.shape, dtype=object) * np.nan
        for i, well in enumerate(well_nums):
            try:
                # first get row and column
                ref_level = well_details.loc[well, 'REFERENCE_RL']
                lat = well_details.loc[well, 'NZTMY']
                lon = well_details.loc[well, 'NZTMX']
                ground_level = well_details.loc[well, 'GROUND_RL'] + ref_level
                if pd.isnull(ground_level):
                    rt, ct = self.convert_coords_to_matix(lon, lat)
                    ground_level = elv_db[0, rt, ct]  # take as top of model

                screen_num = well_details.loc[well, 'Screens']
                depth = ground_level - well_details.loc[well, 'DEPTH']  # this is from ground level

                # set elevation
                elv = None
                if screen_num == 0:
                    elv = depth  # also from ground level
                    layer_temp, row_temp, col_temp = self.convert_coords_to_matix(lon, lat, elv, elv_db)
                elif screen_num == 1:
                    top = ground_level - screen_details['TOP_SCREEN'][screen_details['WELL_NO'] == well]
                    bottom = ground_level - screen_details['BOTTOM_SCREEN'][screen_details['WELL_NO'] == well]
                    if len(top) != 1 or len(bottom) != 1:
                        raise ValueError('well: {} incorrect number of screen values'.format(well))
                    if screen_handling == 'middle':
                        elv = ground_level - (top.iloc[0] + bottom.iloc[0]) / 2  # also from ground level
                        layer_temp, row_temp, col_temp = self.convert_coords_to_matix(lon, lat, elv, elv_db)
                    elif screen_handling == 'all':
                        layer_temp = []
                        lt1, row_temp, col_temp = self.convert_coords_to_matix(lon, lat, top, elv_db)
                        lt2, row_temp, col_temp = self.convert_coords_to_matix(lon, lat, bottom, elv_db)
                        layer_temp.extend(range(lt1, lt2 + 1))  # assume the screen is continous
                else:
                    layer_temp = []
                    for j in range(1, screen_num + 1):
                        top = ground_level - screen_details['TOP_SCREEN'][(screen_details['WELL_NO'] == well) &
                                                                          (screen_details['SCREEN_NO'] == j)]
                        bottom = ground_level - screen_details['BOTTOM_SCREEN'][(screen_details['WELL_NO'] == well) &
                                                                                (screen_details['SCREEN_NO'] == j)]
                        if len(top) != 1 or len(bottom) != 1:
                            raise ValueError('well: {} incorrect number of screen values for screen {}'.format(well, j))
                        if screen_handling == 'middle':
                            elv = ground_level - (top.iloc[0] + bottom.iloc[0]) / 2  # also from ground level
                            lt, row_temp, col_temp = self.convert_coords_to_matix(lon, lat, elv, elv_db)
                            layer_temp.append(lt)
                        elif screen_handling == 'all':
                            lt1, row_temp, col_temp = self.convert_coords_to_matix(lon, lat, top, elv_db)
                            lt2, row_temp, col_temp = self.convert_coords_to_matix(lon, lat, bottom, elv_db)
                            layer_temp.extend(range(lt1, lt2 + 1))  # assume the screen is continous

                if pd.isnull(np.atleast_1d(layer_temp)).sum() != 0:
                    raise ValueError('well: {} does not have screen, reference level, '
                                     'ground level, or depth data'.format(well))

                if isinstance(layer_temp, list):
                    if one_val_per_well:
                        layer_temp = max(set(layer_temp), key=layer_temp.count)
                    else:
                        layer_temp = list(set(layer_temp))
                layer[i] = layer_temp
                row[i] = row_temp
                col[i] = col_temp
            except Exception as val:
                if raise_exct:
                    raise Exception('{} {}'.format(well, val))
                elif error_log_path is None:
                    warn('{} {}'.format(well, val))
                else:
                    f.write('{} {}\n'.format(well, val))

        if raise_exct:
            # check for more missing data
            if pd.isnull(layer).sum() != 0:
                raise ValueError('wells {} do not have data in layer'.format(well_nums[pd.isnull(layer)]))
            elif pd.isnull(row).sum() != 0:
                raise ValueError('wells {} do not have data in row'.format(well_nums[pd.isnull(row)]))
            elif pd.isnull(col).sum() != 0:
                raise ValueError('wells {} do not have data in col'.format(well_nums[pd.isnull(col)]))

        if return_value:
            return layer[0], row[0], col[0]
        else:
            return layer, row, col

    def calc_elv_db(self,recalc=False):
        """
        calculates the elevation database (bottoms with the top of layer one on top or loads pickel
        :param recalc: force recalculates the elv_db from the gns data even if it is not present
        :return: elv_db
        """
        import pickle

        elv_db = pickle.load(open(self.elv_path))

        return elv_db

    def get_base_model(self,recalc=False):
        import flopy
        m = flopy.modflow.Modflow.load(self.base_mod_path, model_ws=os.path.dirname(self.base_mod_path),
                                       forgive=False)
        return m

    def get_package_spd(self, package, recalc=False):
        import pickle

        picklepath = '{}/base_{}.p'.format(self.pickle_dir,package)

        if os.path.exists(picklepath) and not recalc:
            base_data = pickle.load(open(picklepath))
            return base_data

        org_m = self.get_base_model()
        if package.lower in ['str','sfr','drn','wel']:
            base_data = org_m.get_package().stress_period_data.data[0]
        elif package.lower == 'rch':
            raise NotImplementedError() #todo
        else:
            raise ValueError('unexpected package {}, may not be implemented'.format(package))
        pickle.dump(base_data, open(picklepath, 'w'))

        return base_data

        # #todo think about adding get stress period data... input the package names and pickle the file if needed so you don't always have to load the thing



    #todo think about adding default model_maps
