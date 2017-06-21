"""
This is really bogus
Author: matth
Date Created: 15/03/2017 9:22 AM
"""

from __future__ import division
import pandas as pd
import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.sandbox.regression.predstd import wls_prediction_std
import matplotlib.pyplot as plt
from copy import deepcopy

well_data = pd.read_csv(r"P:\Groundwater\Trend_analysis\Data\modified_data\Level\well_water_level.csv",index_col='DMY')
well_data.index = pd.to_datetime(well_data.index)
plotdata = deepcopy(well_data)
for key in plotdata.keys():
    plotdata[key] = plotdata[key] / plotdata[key].median()

#well_data['well'] = plotdata.median(axis=1)



enso = pd.read_table("P:\Groundwater\Trend_analysis\Data\climate\NOAA_best_ENSO_1month.txt",
                      names=['year',1,2,3,4,5,6,7,8,9,10,11,12],
                      delim_whitespace=True)
enso2 = pd.melt(enso,
                    id_vars=['year'],
                    value_vars=[1,2,3,4,5,6,7,8,9,10,11,12],
                    var_name='month',
                    value_name='enso')
enso2['day'] = 15
enso2['DMY'] = pd.to_datetime(enso2[['year','month','day']])
enso2 = enso2.sort_values('DMY')
enso2 = enso2.set_index('DMY')
enso2 = enso2.drop(['year','month','day'],1)

sam = pd.read_table(r"P:\Groundwater\Trend_analysis\Data\climate\SAM_NOAA_CPC_AO.txt",delim_whitespace=True,)
sam['day'] = 15
sam['DMY'] = pd.to_datetime(sam[['year','month','day']])
sam = sam.set_index('DMY')
sam = sam.drop(['year','month','day'],1)

well_data = well_data.join(enso2)
well_data = well_data.join(sam)
well_data['month'] = well_data.index.month
well_data['DMY'] = np.arange(1,len(well_data.index)+1)

well = 'M35/3614'
fit_data = well_data[[well,'month','sam','enso','DMY']].dropna()
fit_data = fit_data[fit_data.index>= pd.to_datetime('01/01/1985')]
fit_data = fit_data.rename(columns={well:'well'})

mod = smf.glm(formula='well ~ DMY + month + sam + enso + 1', data=fit_data)
mod = smf.glm(formula='well ~ DMY + month + 1', data=fit_data)
res = mod.fit()
print(res.summary())

fig,ax = plt.subplots(1)
ax.plot(fit_data.index, fit_data['well'], 'b-')
ax.plot(fit_data.index, res.fittedvalues, 'r-')
plt.show()

plt.close()

plt.plot(fit_data.enso,fit_data.well, 'bo')
plt.show()
plt.close()

plt.plot(fit_data.sam, fit_data.well, 'bo')
plt.show()
plt.close()


print('test')

