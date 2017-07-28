# -*- coding: utf-8 -*-
"""
Functions to delineate catchments.
"""


def catch_net(catch_sites_csv, catch_sites_col=['GRIDCODE', 'SITE']):
    """
    Function to create a dataframe of all the upstream catchments from specific locations. Input should be the output from ArcGIS script (2 columns of catchments and sites).
    """
    from numpy import append, isnan, in1d
    from pandas import DataFrame, read_csv

    ## Read in data
    catch_sites_names=['catch', 'site']
    catch_sites = read_csv(catch_sites_csv)[catch_sites_col]
    catch_sites.columns = catch_sites_names

    ## Reorganize and select intial catchments
    catch_sites1 = catch_sites[catch_sites.site != 0]
    catch_sites2 = catch_sites1[catch_sites1.catch != catch_sites1.site]

    catch_unique = catch_sites2.catch.unique()

    singles = catch_sites1.catch[~catch_sites1.catch.duplicated(keep=False)]

    ## Network processing
    df = catch_sites2
    index1 = catch_unique
    catch_set2 = []
    for i in index1:
        catch1 = df.loc[df.catch == i, 'site'].values
        catch_set1 = catch1
        check1 = in1d(df.catch, catch1)
        while sum(check1) >= 1:
#            if sum(check1) > len(catch1):
#                print('Index numbering is wrong!')
            catch2 = df[check1].site.values.flatten()
            catch3 = catch2[~isnan(catch2)]
            catch_set1 = append(catch_set1, catch3)
            check1 = in1d(df.catch, catch3)
            catch1 = catch3
        catch_set2.append(catch_set1.tolist())

    df2 = DataFrame(catch_set2, index=index1)
    return([df2, singles.values])


def agg_catch(catch_del_shp, catch_sites_csv, catch_sites_col=['GRIDCODE', 'SITE'], catch_col='GRIDCODE'):
    """
    Function to take the output of the ArcGIS catchment delineation polygon shapefile and cathcment sites csv and return a shapefile with appropriately delineated polygons.
    """
    from geopandas import read_file, GeoDataFrame, GeoSeries
    from core.spatial import catch_net
    from pandas import concat
    from numpy import in1d, append

    ## Catchment areas shp
    catch = read_file(catch_del_shp)[[catch_col, 'geometry']]

    ## dissolve the polygon
    catch3 = catch.dissolve(catch_col)

    ## Determine upstream catchments
    catch_df, singles_df = catch_net(catch_sites_csv, catch_sites_col)

    base1 = catch3[in1d(catch3.index, singles_df)].geometry
    for i in catch_df.index:
            t1 = append(catch_df.loc[i, :].dropna().values, i)
            t2 = GeoSeries(catch3[in1d(catch3.index, t1)].unary_union, index=[i])
            base1 = GeoSeries(concat([base1, t2]))

    ## Convert to GeoDataFrame (so that all functions can be applied to it)
    base2 = GeoDataFrame(base1.index, geometry=base1.geometry.values, crs=catch.crs)
    base2.columns = ['site', 'geometry']
    return(base2)

