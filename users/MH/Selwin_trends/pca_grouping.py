"""
Author: matth
Date Created: 28/03/2017 5:34 PM
"""

from __future__ import division
from core import env
import pandas as pd
import os
import numpy as np
from sklearn.preprocessing import robust_scale
from sklearn.cluster import AgglomerativeClustering
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import mpl_toolkits.mplot3d.axes3d as p3
import shutil
from core.ecan_io import rd_sql, sql_db

base_dir = env.sci("Groundwater/Trend_analysis/Data/organized data")
well_details = rd_sql(**sql_db.wells_db.well_details)
well_details = well_details.set_index('WELL_NO')

years = [1952, 1975, 1985, 1995, 2005]
for dir_ in ['groundwater', 'surfacewater']:
    all_ex_var = {}
    all_ex_var_rat = {}
    pca_output = {}
    site_lists = {}
    print('completeing PCA analysis and grouping for {}'.format(dir_))
    outdir = '{bd}/{d}/PCA_grouping'.format(bd=base_dir, d=dir_)
    for year in years:
        # set up dirs and load data
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        poutdir = '{od}/diagnostic_plots'.format(od=outdir)
        if not os.path.exists(poutdir):
            os.makedirs(poutdir)

        indir = '{bd}/{d}/filled_data_by_time_period/filled_month_{d}_data_{y}.csv'.format(bd=base_dir, d=dir_, y=year)
        try:
            data = pd.read_csv(indir,index_col=0)
        except IOError:
            continue
        if year == 1985 and dir_ == 'surfacewater':
            continue

        data.index = pd.to_datetime(data.index)
        site_lists[year] = data.keys()

        # calculate principle comments
        # there are some difference between the matlab PCA and this one.
        # the matlab script only centers the data on the mean, while this script
        # standardises the data based on the median and interquartile range (see robust_scale below)

        in_data = np.array(data)
        in_data = robust_scale(in_data)  # we typically have a skewed normal distribution so robust_scaling

        pca = PCA(svd_solver='full')
        pca.fit(in_data)
        pca_output[year] = pca.components_.transpose()
        ex_var = pca.explained_variance_
        ex_var_ratio = pca.explained_variance_ratio_

        all_ex_var[year] = ex_var
        all_ex_var_rat[year] = ex_var_ratio

        # export pca analysis
        idxs = ['explained_variance', 'explained_variance_ratio'] + list(data.keys())
        cols = ['pc_{}'.format(x+1) for x in range(pca.n_components_)]
        outdata = pd.DataFrame(index=idxs,columns=cols)
        outdata.loc['explained_variance',:] = ex_var
        outdata.loc['explained_variance_ratio',:] = ex_var_ratio
        outdata.iloc[2:,:] = pca_output[year][:,:]

        outdata.to_csv('{}/pca_analysis_{}.csv'.format(outdir,year))


    # look at explained varience plots
    if dir_ == 'surfacewater':
        years2 = [1995, 2005]
    else:
        years2 = years
    length = [x.shape[0] for x in all_ex_var.values()]
    exvar_df = pd.DataFrame(index=np.arange(np.array(length).max()), columns=years2)
    exvar_rat_df = pd.DataFrame(index=np.arange(np.array(length).max()), columns=years2)
    for key in years2:
        n=len(all_ex_var[key])
        exvar_rat_df[key].iloc[0:n] = all_ex_var_rat[key][0:n]
        exvar_df[key].iloc[0:n] = all_ex_var[key][0:n]
    colors = ['r','b','g','y','k']
    for i, key in enumerate(exvar_rat_df.keys()):
        plt.plot(exvar_rat_df[key], marker = 'o', color=colors[i])

    plt.close()

    # run the agglomerative clustering in 3d PC space
    print('completing clustering for {}'.format(dir_))

    for year in years2:
        if year == 1975:
            continue
        cluster_data = pca_output[year][:,0:3]
        cluster_nums = np.arange(2,12)
        cluster_nums = cluster_nums[cluster_nums < cluster_data.shape[0]]
        cols = ['{}_clusters'.format(e) for e in cluster_nums]

        plt_dir = '{}/cluster_plots_{}'.format(outdir,year)
        if not os.path.exists(plt_dir):
            os.makedirs(plt_dir)

        for linkage in ['ward', 'complete', 'average']:
            mem_dir = '{}/cluster_memory_{}'.format(outdir, year)

            if not os.path.exists(mem_dir):
                os.makedirs(mem_dir)

            # delete everything in the mem directory as I don't quite understand how Agg_cluster handles memory
            shutil.rmtree(mem_dir)
            cluster_output = pd.DataFrame(index=site_lists[year],columns=cols)
            for c_num, c_name in zip(cluster_nums, cols):
                # compute clusters for each cluster number

                cluster = AgglomerativeClustering(n_clusters=c_num,
                                                  memory=mem_dir,
                                                  compute_full_tree=True,
                                                  linkage=linkage)

                cluster_output[c_name] = cluster.fit_predict(cluster_data)

                #3d plot everything
                fig = plt.figure()
                ax = p3.Axes3D(fig)
                for val in range(c_num):
                    idx = np.where(np.array(cluster_output[c_name])== val)
                    plot_data = cluster_data[idx]

                    ax.view_init(7, -80)
                    ax.plot3D(plot_data[:, 0], plot_data[:, 1], plot_data[:, 2],'o',
                              color=plt.cm.jet(val / np.max(np.arange(c_num+1))))
                    ax.set_title(linkage)
                fig.savefig('{}/{}_groups_{}.png'.format(plt_dir,c_num,linkage))
                plt.close(fig)

            # add gis data
            if dir_ == 'groundwater':
                for key in cluster_output.index:
                    lat = well_details.loc[key, 'NZTMY']
                    lon = well_details.loc[key, 'NZTMX']
                    cluster_output.loc[key, 'lon'] = lon
                    cluster_output.loc[key, 'lat'] = lat
            cluster_output.to_csv('{}/clusters_{}_{}.csv'.format(outdir,year,linkage))








