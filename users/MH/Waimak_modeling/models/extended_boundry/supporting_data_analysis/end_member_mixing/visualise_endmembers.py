# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 9/11/2017 8:50 AM
"""

from __future__ import division
from core import env
import numpy as np
import matplotlib.pyplot as plt

if __name__ == '__main__':
    end_mean_o18 = {'inland': -8.76, 'coastal': -8.00, 'river': -9.25, 'eyre': -8.90}
    end_sd_o18 = {'inland': 0.21, 'coastal': 0.64, 'river': 0.22, 'eyre': 0.54}

    end_mean_cl = {'inland': 10.38, 'coastal': 25.43, 'river': 1.05, 'eyre': 3.67}
    end_sd_cl = {'inland': 4.21, 'coastal': 9.49, 'river': 0.15, 'eyre': 0.29}
    o18s = {}
    cls = {}
    for site in end_mean_cl.keys():
        cls[site] = np.random.normal(end_mean_cl[site], end_sd_cl[site], 100000)
        o18s[site] = np.random.normal(end_mean_o18[site], end_sd_o18[site], 100000)

    # o18
    fig, ax = plt.subplots(figsize=(18.5, 9.5))
    ax.hist(o18s['coastal'], bins=101, color='orange', label='coastal', alpha=0.5)
    ax.hist(o18s['inland'], bins=101, color='r', label='inland', alpha=0.5)
    ax.hist(o18s['river'], bins=101, color='b', label='alpine_river', alpha=0.5)
    ax.hist(o18s['eyre'], bins=101, color='g', label='eyre', alpha=0.5)
    ax.set_title('o18')
    ax.legend()

    # cl
    fig2, ax2 = plt.subplots(figsize=(18.5, 9.5))
    ax2.hist(cls['coastal'], bins=101, color='orange', label='coastal', alpha=0.5)
    ax2.hist(cls['inland'], bins=101, color='r', label='inland', alpha=0.5)
    ax2.hist(cls['river'], bins=101, color='b', label='alpine_river', alpha=0.5)
    ax2.hist(cls['eyre'], bins=101, color='g', label='eyre', alpha=0.5)
    ax2.set_title('cl')
    ax2.legend()

    fig3, ax3 = plt.subplots(figsize=(18.5, 9.5))
    ax3.boxplot([cls['coastal'], cls['inland'], cls['river'], cls['eyre']])
    plt.show()
