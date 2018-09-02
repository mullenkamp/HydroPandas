# -*- coding: utf-8 -*-
"""
Created on Wed Aug 29 08:37:45 2018

@author: MichaelEK
"""

import os
import pandas as pd
from pdsql import mssql
import statsmodels as sm
from pandas.plotting import autocorrelation_plot
from matplotlib import pyplot
import numpy as np
import pandas as pd
from pdsql import mssql
import os
from hydropandas.tools.general.spatial.vector import xy_to_gpd, sel_sites_poly
import geopandas as gpd
from shapely.geometry import Point
import pickle
from hydrolm.lm import LM
from seaborn import regplot
import matplotlib.pyplot as plt
from statsmodels import robust
from sklearn import datasets, svm
from sklearn.feature_selection import SelectPercentile, f_classif, f_regression, mutual_info_regression
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel, Sum
from sklearn.linear_model import HuberRegressor, Ridge, LinearRegression
from itertools import combinations
from scipy import stats, special


def tsreg(ts, freq=None, interp=False, maxgap=None):
    """
    Function to regularize a time series object (pandas).
    The first three indeces must be regular for freq=None!!!

    ts -- pandas time series dataframe.\n
    freq -- Either specify the known frequency of the data or use None and
    determine the frequency from the first three indices.\n
    interp -- Interpolation method.
    """

    if freq is None:
        freq = pd.infer_freq(ts.index[:3])
    ts1 = ts.resample(freq).mean()
    if isinstance(interp, str):
        ts1 = ts1.interpolate(interp, limit=maxgap)

    return ts1

server = 'sql2012test01'
database = 'hydro'
ts_table = 'TSDataNumericDaily'

sites = ['69302']
datasets = [5]

tsdata1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DatasetTypeID', 'DateTime', 'Value'], where_col={'ExtSiteID': sites, 'DatasetTypeID': datasets})
tsdata1.DateTime = pd.to_datetime(tsdata1.DateTime)

tsdata2 = tsdata1.drop(['ExtSiteID', 'DatasetTypeID'], axis=1).set_index('DateTime').Value
tsdata3 = tsreg(tsdata2, freq='D', interp='time')

tsdata3.plot()

autocorrelation_plot(tsdata3)
pyplot.show()
d1 = sm.tsa.seasonal.seasonal_decompose(tsdata3)
#m1 = sm.tsa.ar_model.AR(tsdata2, freq='D')
#f1 = m1.fit(m1)

m1 = sm.tsa.arima_model.ARIMA(tsdata3, freq='D', order=(50, 1, 2))
fit1 = m1.fit(m1, display=0)

#######################
from __future__ import print_function
import numpy as np
from scipy import stats
import pandas as pd
import matplotlib.pyplot as plt

import statsmodels.api as sm

from statsmodels.graphics.api import qqplot

print(sm.datasets.sunspots.NOTE)

dta = sm.datasets.sunspots.load_pandas().data

dta.index = pd.Index(sm.tsa.datetools.dates_from_range('1700', '2008'))
del dta["YEAR"]

dta.values.squeeze()

pacf = sm.graphics.tsa.plot_pacf


pacf(tsdata3[:500])


arma_mod20 = sm.tsa.ARMA(tsdata3[:500], (8,0)).fit(disp=False)
print(arma_mod20.params)
fig = plt.figure(figsize=(12,8))
ax = fig.add_subplot(111)
ax = arma_mod20.resid.plot(ax=ax);

#########################################
import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt

# NBER recessions
from pandas_datareader.data import DataReader
from datetime import datetime
usrec = DataReader('USREC', 'fred', start=datetime(1947, 1, 1), end=datetime(2013, 4, 1))

############################################
### Parameters

server = 'sql2012test01'
database = 'hydro'
ts_daily_table = 'TSDataNumericDaily'
ts_hourly_table = 'TSDataNumericHourly'
ts_summ_table = 'TSDataNumericDailySumm'
lf_table = 'LowFlowRestrSite'
sites_table = 'ExternalSite'


lf_date = '2018-05-28'
recent_date = '2018-01-01'
min_date = '2000-01-01'
min_count = 8
search_dis = 50000
n_ind = 2

dep_loss = 0.04
nobs_loss = 0.002

rec_datasets = [5, 1521, 4503, 4515]
man_datasets = [8, 1637, 4558, 4564, 4570]

qual_codes = [200, 400, 500, 520, 600]

export_dir = r'E:\ecan\shared\projects\low_flow_regressions'
fig_sub_dir = 'plots'
export_summ1 = 'summary_2018-08-27.csv'

############################################
### Extract summary data and determine the appropriate sites to use

rec_summ_data = mssql.rd_sql(server, database, ts_summ_table, where_col={'DatasetTypeID': rec_datasets})
rec_summ_data.FromDate = pd.to_datetime(rec_summ_data.FromDate)
rec_summ_data.ToDate = pd.to_datetime(rec_summ_data.ToDate)

rec_sites = rec_summ_data[(rec_summ_data.ToDate > recent_date)]

### Extract low flow sites

lf_sites = mssql.rd_sql(server, database, lf_table, ['site', 'flow_method', 'min_trig', 'max_trig'], where_col={'date': [lf_date], 'site_type': ['LowFlow'], 'flow_method': ['Correlated from Telem', 'Gauged', 'Manually Calculated', 'Visually Gauged']})
lf_sites.rename(columns={'site': 'ExtSiteID'}, inplace=True)

man_summ_data = mssql.rd_sql(server, database, ts_summ_table, where_col={'DatasetTypeID': man_datasets})
man_summ_data.FromDate = pd.to_datetime(man_summ_data.FromDate)
man_summ_data.ToDate = pd.to_datetime(man_summ_data.ToDate)

lf_gauge_sites = pd.merge(man_summ_data, lf_sites, on='ExtSiteID')

gauge_sites1 = lf_gauge_sites[lf_gauge_sites.Count >= min_count].copy()

## Remove sites that are also recorders

gauge_sites = gauge_sites1[~gauge_sites1.ExtSiteID.isin(rec_sites.ExtSiteID.unique())]

## Summaries of bad sites

mis_lf_gauge_sites = lf_sites[~lf_sites.ExtSiteID.isin(man_summ_data.ExtSiteID)]
less_than_10_lf_sites = lf_gauge_sites[lf_gauge_sites.Count < min_count]

## Get a full list of sites to retrieve site data for

all_sites = set(rec_sites.ExtSiteID.tolist())
all_sites.update(gauge_sites.ExtSiteID.tolist())

###########################################
### Get site data

site_xy = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'ExtSiteName', 'NZTMX', 'NZTMY'], where_col={'ExtSiteID': list(all_sites)})

geometry = [Point(xy) for xy in zip(site_xy['NZTMX'], site_xy['NZTMY'])]
site_xy1 = gpd.GeoDataFrame(site_xy['ExtSiteID'], geometry=geometry, crs=2193)

gauge_xy = site_xy1[site_xy1.ExtSiteID.isin(gauge_sites.ExtSiteID)].set_index('ExtSiteID').copy()
rec_xy = site_xy1[site_xy1.ExtSiteID.isin(rec_sites.ExtSiteID)].set_index('ExtSiteID').copy()


###########################################
### Iterate through the low flow sites

results_dict = {}
for g in gauge_sites.ExtSiteID.tolist():
    print(g)

    ## Max trig
    max_trig = gauge_sites[gauge_sites.ExtSiteID == g].max_trig.tolist()[0]

    ## Determine recorder sites within search distance
    gauge_loc = gauge_xy.loc[[g]].buffer(search_dis)
    near_rec_sites = sel_sites_poly(rec_xy, gauge_loc)
    print('There are ' + str(len(near_rec_sites)) + ' recorder sites within range')

    ## Extract all ts data
    g_data = mssql.rd_sql(server, database, ts_daily_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'DatasetTypeID': man_datasets, 'ExtSiteID': [g], 'QualityCode': qual_codes}, from_date=min_date, date_col='DateTime')
    g_data.DateTime = pd.to_datetime(g_data.DateTime)
    g_data.loc[g_data.Value <= 0, 'Value'] = np.nan
    g_data.loc[g_data.Value > max_trig*2, 'Value'] = np.nan
    r_data = mssql.rd_sql(server, database, ts_daily_table, ['ExtSiteID', 'DateTime', 'Value'], where_col={'DatasetTypeID': rec_datasets, 'ExtSiteID': near_rec_sites.index.tolist(), 'QualityCode': qual_codes}, from_date=min_date, date_col='DateTime')
    r_data.DateTime = pd.to_datetime(r_data.DateTime)
    r_data.loc[r_data.Vaalue <= 0, 'Value'] = np.nan

    ## Re-organise the datasets
    g_data1 = g_data.pivot_table('Value', 'DateTime', 'ExtSiteID')
    r_data1 = r_data.pivot_table('Value', 'DateTime', 'ExtSiteID')

    ## Filter the recorder data by guagings
    set1 = pd.concat([g_data1, r_data1], join='inner', axis=1)
    if len(set1) < min_count:
        continue

    x_names = r_data1.columns.tolist()

    bestf = {}
    bestm = {}
    combos = set(combinations(x_names, n_ind))
    for c in combos:
        xi = list(c)
        set2 = pd.concat([g_data1, r_data1[xi]], axis=1).dropna()
        if len(set2) < min_count:
            continue
        X = set2[xi].values
        y = set2[g].values
        f2 = f_regression(X, y)
        bestf.update({c: round(f2[0][0], 1)})
        m2 = mutual_info_regression(X, y)
        bestm.update({c: round(m2[0], 3)})




#    ## regressions!
#    ols = LM(r_data1, g_data1)
#    ols1 = ols.ols(1, log_x=False, log_y=False, min_obs=min_count)
#    ols2 = ols.ols(2, log_x=False, log_y=False, min_obs=min_count)
#    ols3 = ols.ols(3, log_x=False, log_y=False, min_obs=min_count)
#
#    ## Save
#    results_dict.update({g: {1: ols1, 2: ols2, 3: ols3}})


### Produce summary table

cols = ['site', 'nrmse', 'Adj R2', 'nobs', 'y range', 'f value', 'f p value', 'dep sites', 'reg params']

res_list_df = []

for s in range(1, 4):
    res_site = pd.DataFrame()
    res_list = []
    for i in results_dict:
        model1 = results_dict[i][s]
        if model1 is not None:
            if model1.sm_xy:
                nrmse1 = model1.nrmse()[i]
                adjr2 = round(model1.sm[i].rsquared_adj, 3)
                nobs = model1.sm[i].nobs
                y_range = model1.sm_xy[i]['y'].max() - model1.sm_xy[i]['y'].min()
                dep_sites = model1.sm_xy[i]['x'].columns.tolist()
                fvalue = round(model1.sm[i].fvalue, 3)
                fpvalue = round(model1.sm[i].f_pvalue, 3)
                params1 = model1.sm[i].params.round(5).tolist()

                site_res = [i, nrmse1, adjr2, nobs, y_range, fvalue, fpvalue, dep_sites, params1]
                res_list.append(site_res)

                ## Plots
                fig = model1.plot_ccpr_grid(i)
                fig.savefig(os.path.join(export_dir, fig_sub_dir, i + '_' + str(s) + '.png'), bbox_inches='tight')
                plt.close(fig)

    res_site1 = pd.DataFrame(res_list, columns=cols).set_index('site')
    res_list_df.append(res_site1)

res_df = pd.concat(res_list_df, axis=1, keys=range(1, len(res_list_df) + 1))


### Categorize the results and assign which is best

levels1 = res_df.columns.levels[0].tolist()

f1 = res_df.loc[:, (slice(None), 'f value')]
f1.columns = f1.columns.droplevel(1)

winner = f1.idxmax(axis=1)
winner.name = 'winner'

res_df['winner'] = winner


















x = '67001'

set2 = set1[[g, x]].dropna()

X = set2[x].copy().values.reshape(-1, 1)
y = set2[g].copy().values

f1 = f_classif(X, y)
f2 = f_regression(X, y)
pyplot.show()


g1 = GaussianProcessRegressor().fit(X, y)

x1min = np.min(X.T[0])
x1max = np.max(X.T[0])
x2min = np.min(X.T[1])
x2max = np.max(X.T[1])

x1 = np.atleast_2d(np.linspace(x1min, x1max, 100)).T
x2 = np.atleast_2d(np.linspace(x2min, x2max, 100)).T

x = np.concatenate((x1, x2), axis=1)

y_pred, sigma = g1.predict(x, return_std=True)

h1 = HuberRegressor().fit(X, y)
y_pred = h1.predict(x)

l1 = LinearRegression().fit(X, y)
y_pred = l1.predict(x)

x = np.atleast_2d(np.linspace(0, 0.1, 100)).T


fig = plt.figure()
plt.plot(X, y, 'r.', markersize=10, label=u'Observations')
plt.plot(x, y_pred, 'b-', label=u'Prediction')
#plt.fill(np.concatenate([x, x[::-1]]),
#         np.concatenate([y_pred - 1.9600 * sigma,
#                        (y_pred + 1.9600 * sigma)[::-1]]),
#         alpha=.5, fc='b', ec='None', label='95% confidence interval')
plt.xlabel('$x$')
plt.ylabel('$f(x)$')
#plt.ylim(-10, 20)
plt.legend(loc='upper left')
pyplot.show()





data = sm.datasets.longley.load()
data.exog = sm.add_constant(data.exog)
results = sm.RLM(data.endog, data.exog).fit()
A = np.identity(len(results.params))
A = A[1:,:]
print(results.f_test(A))

















