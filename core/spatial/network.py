# -*- coding: utf-8 -*-
"""
Network processing and analysis functions.
"""

############################################
#### Network processing and analysis


def nx_shp(shp_pts, shp_lines, site_col='site'):
    """
    Function to import shapefiles into nx networks. The lines become the edges and the points are used as the node (names). The points shapefile should have a site name column that is precisely at the intersection of the shp_lines.
    """
    from networkx import read_shp, relabel_nodes
    from numpy import where, in1d

    ## Read in shapefiles
    t2 = read_shp(shp_pts)
    t3 = read_shp(shp_lines)

    ## extract the site names
    sites = [i[1][site_col] for i in t2.nodes(data=True)]

    ## Set up rename dictionaries
    rename1 = {(i[0], i[1]): (round(i[0]), round(i[1])) for i in t3.nodes()}
    rename2 = {(round(i[0][0]), round(i[0][1])): i[1]['site'] for i in t2.nodes(data=True)}

    ## Rename nodes
    g2 = relabel_nodes(t3, rename1)
    g3 = relabel_nodes(g2, rename2)

    ## Remove unnecessary nodes
    remove1 = [g3.nodes()[i] for i in where(~in1d(g3.nodes(), sites))[0]]
    g3.remove_nodes_from(remove1)

    return(g3)


def str_paths(nx1):
    """
    Function to process the stream network to determine the nodes and edges to downstream gauging sites. The input is from the output of nx_shp. The output is a two dictorionary object list of site nodes and site edges.
    """
    from networkx import all_pairs_shortest_path, all_pairs_dijkstra_path_length

    def iter1(g1, d2, site):
        from numpy import argmin

        keys1 = g1.keys()
        sites2 = [i for i in keys1 if ((i != site) & (i < 10000000))]
        if not sites2:
            output = [site]
        else:
            len1 = [d2[site][i] for i in sites2]
            down_site = sites2[argmin(len1)]
            output = g1[down_site]
        return(output)

    ## Determine all paths
    p1 = all_pairs_shortest_path(nx1)
    d1 = all_pairs_dijkstra_path_length(nx1, None, 'len')

    ## Make list of all sites
    sites = [i for i in nx1.nodes() if (i < 10000000)]

    ## Extract the paths for all sites (into a dict)
    p2 = {i: p1[i] for i in sites}
    d2 = {i: d1[i] for i in sites}

    site_nodes = {i: iter1(p2[i], d2, i) for i in p2}
    site_paths = {i: [j[2] for j in nx1.out_edges(site_nodes[i], data='num')][0:-1] for i in site_nodes}
    return([site_nodes, site_paths])
