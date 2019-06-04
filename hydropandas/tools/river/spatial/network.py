# -*- coding: utf-8 -*-
"""
Network processing and analysis functions.
"""
import numpy as np
import pandas as pd
import networkx as nx
import geopandas as gpd

############################################
#### Network processing and analysis


def nx_shp(shp_pts, shp_lines, site_col='site'):
    """
    Function to import shapefiles into nx networks. The lines become the edges and the points are used as the node (names). The points shapefile should have a site name column that is precisely at the intersection of the shp_lines.
    """

    ## Read in shapefiles
    t2 = nx.read_shp(shp_pts)
    t3 = nx.read_shp(shp_lines)

    ## extract the site names
    sites = [i[1][site_col] for i in t2.nodes(data=True)]

    ## Set up rename dictionaries
    rename1 = {(i[0], i[1]): (round(i[0]), round(i[1])) for i in t3.nodes()}
    rename2 = {(round(i[0][0]), round(i[0][1])): i[1]['site'] for i in t2.nodes(data=True)}

    ## Rename nodes
    g2 = nx.relabel_nodes(t3, rename1)
    g3 = nx.relabel_nodes(g2, rename2)

    ## Remove unnecessary nodes
    remove1 = [g3.nodes()[i] for i in np.where(~np.in1d(g3.nodes(), sites))[0]]
    g3.remove_nodes_from(remove1)

    return g3


def str_paths(nx1):
    """
    Function to process the stream network to determine the nodes and edges to downstream gauging sites. The input is from the output of nx_shp. The output is a two dictorionary object list of site nodes and site edges.
    """

    def iter1(g1, d2, site):

        keys1 = g1.keys()
        sites2 = [i for i in keys1 if ((i != site) & (i < 10000000))]
        if not sites2:
            output = [site]
        else:
            len1 = [d2[site][i] for i in sites2]
            down_site = sites2[np.argmin(len1)]
            output = g1[down_site]
        return output

    ## Determine all paths
    p1 = nx.all_pairs_shortest_path(nx1)
    d1 = nx.all_pairs_dijkstra_path_length(nx1, None, 'len')

    ## Make list of all sites
    sites = [i for i in nx1.nodes() if (i < 10000000)]

    ## Extract the paths for all sites (into a dict)
    p2 = {i: p1[i] for i in sites}
    d2 = {i: d1[i] for i in sites}

    site_nodes = {i: iter1(p2[i], d2, i) for i in p2}
    site_paths = {i: [j[2] for j in nx1.out_edges(site_nodes[i], data='num')][0:-1] for i in site_nodes}
    return site_nodes, site_paths


def up_branch(df, index_col=1):
    """
    Function to create a dataframe of all the interconnected values looking upstream from specific locations.
    """

    col1 = df.columns[index_col-1]
    index1 = df[col1]
    df2 = df.drop(col1, axis=1)
    catch_set2 = []
    for i in index1:
        catch1 = df2[index1 == i].dropna(axis=1).values[0]
        catch_set1 = catch1
        check1 = index1.isin(catch1)
        while sum(check1) >= 1:
            if sum(check1) > len(catch1):
                print('Index numbering is wrong!')
            catch2 = df2[check1].values.flatten()
            catch3 = catch2[~np.isnan(catch2)]
            catch_set1 = np.append(catch_set1, catch3)
            check1 = index1.isin(catch3)
            catch1 = catch3
        catch_set2.append(catch_set1.tolist())

    output1 = pd.DataFrame(catch_set2, index=index1)
    return output1


#####################################################
#### MFE REC streams network


def find_upstream_rec(nzreach, rec_streams_shp):
    """
    Function to estimate all of the reaches (and nodes) upstream of specific reaches.

    Parameters
    ----------
    nzreach : list, ndarray, Series of int
        The NZ reach IDs
    rec_streams_shp : str path or GeoDataFrame
        str path to the REC streams shapefile or the equivelant GeoDataFrame.

    Returns
    -------
    DataFrame

    """
    if not isinstance(nzreach, (list, np.ndarray, pd.Series)):
        raise TypeError('nzreach must be a list, ndarray or Series.')

    ### Parameters
#    server = 'SQL2012PROD05'
#    db = 'GIS'
#    table = 'MFE_NZTM_REC'
#    cols = ['NZREACH', 'NZFNODE', 'NZTNODE']
#
#    ### Load data
#    rec = rd_sql(server, db, table, cols)
    if isinstance(rec_streams_shp, gpd.GeoDataFrame):
        rec = rec_streams_shp.copy().drop('geometry', axis=1)
    elif isinstance(rec_streams_shp, str):
        if rec_streams_shp.endswith('shp'):
            rec = gpd.read_file(rec_streams_shp).drop('geometry', axis=1)
    else:
        raise TypeError('rec_shp must be either a GeoDataFrame or a shapefile')

    ### Run through all nzreaches
    reaches_lst = []
    for i in nzreach:
        reach1 = rec[rec.NZREACH == i]
        up1 = rec[rec.NZTNODE.isin(reach1.NZFNODE)]
        while not up1.empty:
            reach1 = pd.concat([reach1, up1])
            up1 = rec[rec.NZTNODE.isin(up1.NZFNODE)]
        reach1.loc[:, 'start'] = i
        reaches_lst.append(reach1)

    reaches = pd.concat(reaches_lst)
    reaches.set_index('start', inplace=True)
    return reaches

