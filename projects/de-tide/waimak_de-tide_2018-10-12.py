# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 12:02:53 2018

@author: michaelek
"""
import pandas as pd
from pyhydrotel import get_mtypes, get_sites_mtypes, get_ts_data
import statsmodels.api as sm
from hydrointerp.util import tsreg
import scipy
import matplotlib.pyplot as plt
import numpy as np

pd.options.display.max_columns = 10

######################################
### Parameters

server = 'sql2012prod05'
database = 'hydrotel'

site = '66401'
mtype = 'water level'

from_date = '2018-08-01'
to_date = '2018-10-12'

freq_int = 149

######################################
### Get data

#mtypes1 = get_sites_mtypes(server, database, sites=site)

tsdata = get_ts_data(server, database, mtype, site, from_date, to_date, None)

tsdata1 = tsreg(tsdata.reset_index().drop(['ExtSiteID', 'MType'], axis=1).set_index('DateTime')).interpolate('pchip')

#tsdata1.plot()

roll1 = tsdata1.rolling(12, center=True).mean().Value.dropna()
roll1.name = 'Smoothed original'

s1 = sm.tsa.seasonal_decompose(roll1, freq=freq_int, extrapolate_trend=12)
#s1.plot()

s2 = s1.seasonal.copy()

tsdata2 = roll1[s2 < (s2.min() + 1.5)].copy()

tsdata3 = tsdata2.asfreq('5T').interpolate('pchip')
tsdata3.name = 'de-tided'

#minmax3 = scipy.signal.argrelextrema(s1.seasonal.Value.values, np.greater, order=20)[0]
#
#minmax3 = scipy.signal.argrelextrema(roll1.Value.values, np.less, order=100)[0]
#
#minmax3a = minmax3[1:]
#minmax3b = minmax3[:-1]
#minmax3a - minmax3b

p1 = roll1.plot()
tsdata3.plot(ax=p1)



sig1 = s1.seasonal.Value

fft1 = scipy.fftpack.rfft(sig1)
plt.plot(fft1)

tsdata2 = tsdata1 - tsdata1.mean()

fr = np.fft.fftfreq(len(tsdata2), freq_int)
fou = np.fft.fft(tsdata2.Value)
fou1 = np.fft.fftshift(fou)

plt.plot(fou1)

fou1[(fou1 > 200000) | (fou1 < -200000)] = 0
fou1[(fou1 > 200000)] = 0

new1 = np.real(np.fft.ifft(fou1))

plt.plot(new1)

time_vec = tsdata2.index.values
sig = tsdata2.Value.values

sig_fft = np.fft.fft(tsdata2.Value)
power = np.abs(sig_fft)
sample_freq = np.fft.fftfreq(len(tsdata2), freq_int)

# Plot the FFT power
plt.figure(figsize=(6, 5))
plt.plot(sample_freq, power)
plt.xlabel('Frequency [Hz]')
plt.ylabel('plower')

pos_mask = np.where(sample_freq > 0)
freqs = sample_freq[pos_mask]
peak_freq = freqs[power[pos_mask].argmax()]

np.allclose(peak_freq, 1./freq_int)


axes = plt.axes([0.55, 0.3, 0.3, 0.5])
plt.title('Peak frequency')
plt.plot(freqs[:8], power[:8])
plt.setp(axes, yticks=[])

high_freq_fft = sig_fft.copy()
#high_freq_fft[np.abs(sample_freq) > 200000] = 0
high_freq_fft[150:165] = 0

filtered_sig = np.fft.ifft(high_freq_fft)

plt.figure(figsize=(6, 5))
plt.plot(time_vec, sig, label='Original signal')
plt.plot(time_vec, filtered_sig, linewidth=1, label='Filtered signal')
plt.xlabel('Time [s]')
plt.ylabel('Amplitude')

plt.legend(loc='best')


fftr1 = scipy.fftpack.rfft(sig)
plt.plot(fftr1)

