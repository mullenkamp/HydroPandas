# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 12/11/2017 2:12 PM
"""

import timeit
import pandas as pd
from sklearn.decomposition import PCA

data = pd.read_csv(r"T:\Temp\temp_gw_files\temp_param.csv", index_col=0).transpose()
data = data.dropna(axis=0)
pca = PCA(svd_solver='full')
pca.fit(data.values)
pca_output = pca.components_.transpose()
ex_var = pca.explained_variance_
ex_var_ratio = pca.explained_variance_ratio_

print('done')

