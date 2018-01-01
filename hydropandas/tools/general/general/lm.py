# -*- coding: utf-8 -*-
"""
Functions for regressions.
"""


from __future__ import division
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
from itertools import combinations
from scipy import stats, log, exp, mean, sqrt
from statsmodels.tools import eval_measures
from copy import copy

eval_measures_names = ['bias', 'iqr', 'maxabs', 'meanabs', 'medianabs', 'mse', 'rmse', 'stde', 'vare']
neval_measures_names = ['meanabs', 'medianabs', 'mse', 'rmse']
single_plots_names = ['plot_ccpr', 'plot_regress_exog', 'plot_fit']
multi_plots_names = ['plot_ccpr_grid', 'plot_partregress_grid', 'influence_plot']


class LM(object):
    """
    Class to handle predictive linear models. Only OLS is supported at the moment.
    """

    def __init__(self, x, y):
        """
        Function to load in the data.

        Parameters
        ----------
        x: DataFrame
            With header names as the variable names and can be with or without a DateTimeIndex.
        y: DataFrame
            With header names as the variable names and can be with or without a DateTimeIndex.

        Returns
        -------
        LM class
        """
        if (not isinstance(x, pd.DataFrame)) | (not isinstance(y, pd.DataFrame)):
            raise TypeError('x and y must be DataFrames')
        x_names = x.columns.values
        y_names = y.columns.values

        if isinstance(x.index, pd.DatetimeIndex) & isinstance(y.index, pd.DatetimeIndex):
            self.timeindex = True
        elif len(x) == len(y):
            self.timeindex = False
            xy = pd.concat([x, y], axis=1)
            xy1 = xy.dropna()
            x = xy1[x_names].copy()
            y = xy1[y_names].copy()
        else:
            raise ValueError('The x and y DataFrames must either have a DateTimeIndex or must be the same length.')

        self.x = x
        self.y = y
        self.y_names = y_names
        self.x_names = x_names
        self.copy = copy

    def __repr__(self):
        if hasattr(self, 'sm'):
            strr = ''
            for i in self.sm:
                strr = strr + repr(self.sm[i].summary())
            return strr
        else:
            return repr(pd.concat([self.x, self.y], axis=1))

    def __getitem__(self, key):
        n3 = self.sm[key].summary()
        return n3

    def ols(self, n_ind=1, log_x=False, log_y=False, min_obs=10):
        """
        Function to perform an OLS on the contained dataset.

        Parameters
        ----------
        n_ind: int
            Number of independent variables to choose from.
        log_x: bool
            Should the x variables be logged?
        log_y: bool
            Should the y variables be logged?
        min_obs: int
            Minimum number of combined x and y data to perform the OLS.

        Returns
        -------
        LM class with Statsmodels results contained within.
        """
        model1 = self.copy(self)
        y_names = model1.y_names
        x_names = model1.x_names

        best1 = {}
#        best_xy = {}
        best_xy_orig = {}
        predict_dict = {}
        for yi in y_names:
            if log_x:
                x = model1.x.apply(log)
            else:
                x = model1.x
            if log_y:
                y = model1.y.apply(log)
            else:
                y = model1.y

            if model1.timeindex:
                both1 = pd.concat([x, y[yi]], axis=1).dropna()
            else:
                both1 = pd.concat([x, y[yi]], axis=1)

            if both1.empty | (len(both1) < min_obs):
#                raise ValueError('No or not enough data is available to run the OLS')
                print('Dep variable ' + str(yi) + ' has no or not enough data available to run the OLS. Returning None...')
                best1.update({yi: None})

            combos = set(combinations(x_names, n_ind))

            models = {}
            models_mae = {}
            xy_dict = {}
            for xi in combos:
                x_df = both1[list(xi)]
                y_df = both1[yi]

                xy_dict.update({yi: {'x': x_df, 'y': y_df}})

                x_df = sm.add_constant(x_df)
                model = sm.OLS(y_df, x_df).fit()
                models.update({xi: model})
                mae1 = eval_measures.meanabs(model.predict(), y_df)
                models_mae.update({np.round(mae1, 6): xi})

            best_x = models_mae[np.min(models_mae.keys())]
            bestm = models[best_x]
            best1.update({yi: bestm})
#            best_xy.update({yi: xy_dict[yi]})

            xy_orig = xy_dict[yi].copy()
            predict1 = bestm.predict()
            if log_x:
                xy_orig.update({'x': xy_orig['x'].apply(exp)})
            if log_y:
                xy_orig.update({'y': xy_orig['y'].apply(exp)})
                predict1 = exp(predict1)

            best_xy_orig.update({yi: xy_orig})
            predict_dict.update({yi: predict1})

        setattr(model1, 'sm', best1)
        setattr(model1, 'sm_xy', best_xy_orig)
#        setattr(model1, 'sm_xy_base', best_xy_orig)
        setattr(model1, 'sm_predict', predict_dict)

        ### Create stat and plot functions
        for s in eval_measures_names:
            setattr(model1, s, model1._stat_err_gen(s))

        for ns in neval_measures_names:
            setattr(model1, 'n' + ns, model1._nstat_err_gen(ns))

        for sp in single_plots_names:
            setattr(model1, sp, model1._single_plots_gen(sp))

        for mp in multi_plots_names:
            setattr(model1, mp, model1._multi_plots_gen(mp))

        setattr(model1, 'mane', model1._mane_fun)

        ### Return
        return model1

    def _stat_err_gen(self, fun_name):
        """

        """
        fun = getattr(eval_measures, fun_name)

        def stat_err_fun(y=None, round_dig=5):
            """
            Produces the associated Statsmodels eval_measures.

            Parameters
            ----------
            y: list, str, or None
                The name(s) of the dependent variable(s).
            round_dig: int
                The number of digits to round.

            Returns
            -------
            Dict
                Where the key is the y name associated with the stat.
            """
            if y is None:
                stat1 = {}
                for i in self.y_names:
                    stat1.update({i: fun(self.sm_xy[i]['y'].values, self.sm_predict[i])})
            elif isinstance(y, list):
                stat1 = {}
                for i in y:
                    stat1.update({i: fun(self.sm_xy[i]['y'].values, self.sm_predict[i])})
            elif isinstance(y, str):
                stat1 = {y: fun(self.sm_xy[y]['y'].values, self.sm_predict[y])}

            if isinstance(round_dig, int):
                stat1 = {j: np.round(stat1[j], round_dig) for j in stat1}

            return stat1

        return stat_err_fun

    def _nstat_err_gen(self, fun_name):
        """

        """
        fun = getattr(eval_measures, fun_name)

        def nstat_err_fun(y=None, round_dig=5):
            """
            Produces the associated normalised Statsmodels eval_measures.

            Parameters
            ----------
            y: list, str, or None
                The name(s) of the dependent variable(s).
            round_dig: int
                The number of digits to round.

            Returns
            -------
            Dict
                Where the key is the y name associated with the stat.
            """
            if y is None:
                stat1 = {}
                for i in self.y_names:
                    stat1.update({i: fun(self.sm_xy[i]['y'].values, self.sm_predict[i])/mean(self.sm_xy[i]['y'].values)})
            elif isinstance(y, list):
                stat1 = {}
                for i in y:
                    stat1.update({i: fun(self.sm_xy[i]['y'].values, self.sm_predict[i])/mean(self.sm_xy[i]['y'].values)})
            elif isinstance(y, str):
                stat1 = {y: fun(self.sm_xy[y]['y'].values, self.sm_predict[y])/mean(self.sm_xy[y]['y'].values)}

            if isinstance(round_dig, int):
                stat1 = {j: np.round(stat1[j], round_dig) for j in stat1}

            return stat1

        return nstat_err_fun

    def _single_plots_gen(self, fun_name):
        """

        """
        plot_fun = getattr(sm.graphics, fun_name)

        def single_plot(y, x):
            """

            """
            if isinstance(y, str) & isinstance(x, str):
                plot1 = plot_fun(self.sm[y], x)
            return plot1

        return single_plot

    def _multi_plots_gen(self, fun_name):
        """

        """
        plot_fun = getattr(sm.graphics, fun_name)

        def multi_plot(y):
            """

            """
            if isinstance(y, str):
                plot1 = plot_fun(self.sm[y])
            return plot1

        return multi_plot

    def _mane_fun(self, y=None, round_dig=3):
        """
        Produces the mean absolute normalised error.

        Parameters
        ----------
        y: list, str, or None
            The name(s) of the dependent variable(s).
        round_dig: int
            The number of digits to round.

        Returns
        -------
        Dict
            Where the key is the y name associated with the stat.
        """
        if y is None:
            stat1 = {}
            for i in self.y_names:
                mane1 = mean(np.abs(self.sm_xy[i]['y'].values - self.sm_predict[i])/(self.sm_xy[i]['y'].values))
                stat1.update({i: mane1})
        elif isinstance(y, list):
            stat1 = {}
            for i in y:
                mane1 = mean(np.abs(self.sm_xy[i]['y'].values - self.sm_predict[i])/(self.sm_xy[i]['y'].values))
                stat1.update({i: mane1})
        elif isinstance(y, str):
            mane1 = mean(np.abs(self.sm_xy[y]['y'].values - self.sm_predict[y])/(self.sm_xy[y]['y'].values))
            stat1 = {y: mane1}

        if isinstance(round_dig, int):
            stat1 = {j: np.round(stat1[j], round_dig) for j in stat1}

        return stat1


if __name__ == '__main__':
    df = pd.read_csv(r'S:\Surface Water\temp\test_df1.csv', header=[0,1], index_col=0, parse_dates=True, infer_datetime_format=True)
    df[df <= 0] = np.nan

    y = df['gauging'].copy()
    x = df['flow'].copy()

    ols1 = LM(x, y)
    ols2 = ols1.ols(2, log_x=True, log_y=True)
    print(ols2['137'])

