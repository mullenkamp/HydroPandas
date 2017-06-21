# -*- coding: utf-8 -*-
"""
Created on Wed May 10 11:08:07 2017

@author: TinaB
"""
####### Plot seasonal chla boxplot from Squalarc
## 19 May 2017

###########################################################
import seaborn as sns
import matplotlib.pyplot as plt
import numpy
##############################################

#### Reading squalarc data
from core.ecan_io import rd_squalarc
sites1 = ['SQ31096']
wq1 = rd_squalarc(sites1)
wq1['parameter'].unique()
wq1 = rd_squalarc(sites1)

wq1['parameter'].sort_values().unique().tolist()
#wq1['source'].sort_values().unique().tolist()
wq1['month'] = wq1.date.dt.month
wq1['date'] = wq1.date.dt.date

############### Useful information only
# this creates colum for chla only when datapoint present
#chla_column = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']

#  this exports values (but stored as string)
#wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['val'] 
# to calls rows chla.loc[18:, 'val']
############### Info end

# To create numpy float array with chla data
chla_values = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['val'].astype(float).values
chla_month = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['month'].astype(int).values
#print chla_values
#print chla_month

## Chla is stored in two columns in Squalarc with two diffrent units, so again for second column:
# Create numpy float array
chla_values2 = wq1[wq1['parameter'] == 'Chlorophyll a']['val'].astype(float).values
chla_month2 = wq1[wq1['parameter'] == 'Chlorophyll a']['month'].astype(int).values
#print len(chla_values2)
#print len(chla_month2)

# Second chla array needs to be converted to microg/L
# NOTE: Some values are in wrong unit in this column. 
# Don't convert values that are above 0.5, multiply other by 1000 to get microg/L
chla = numpy.zeros(len(chla_values2))
for i in range (0,len(chla_values2)):
    if (chla_values2[i] < 0.5):
        chla[i] = 1000.0*chla_values2[i]
    else:
        chla[i] = chla_values2[i]
print chla
print len(chla)

## scatterplt for converted array vs date
#chla_date2 = wq1[wq1['parameter'] == 'Chlorophyll a']['date']
##print chla_date2
##print len(chla_date2)
#plt.plot(chla_date2, chla)
#plt.show

# now combine the two arrays
chla_month_combined1 =  numpy.concatenate([chla_month,chla_month2])
chla_values_combined1 = numpy.concatenate([chla_values,chla])

# Only use if all months should be shown in plot. Note: values wont be shown in plot
othermonths= [6,7,8,9,10,11]
fake_values= [-1,-1,-1,-1,-1,-1]
chla_month_combined =  numpy.concatenate([chla_month_combined1,othermonths])
chla_values_combined = numpy.concatenate([chla_values_combined1,fake_values])

# Boxplot chla vs month
sns.set_style("whitegrid")
sns.set(font_scale=1.5)  # increase font on axis
ax = sns.boxplot(x=chla_month_combined, y=chla_values_combined, showmeans=True, color='white')
ax.set(ylim=(0.00, 6), xlabel = 'Month', ylabel = 'chla')


########## to plot from csv fies
#import seaborn as sns
#import pandas as pd
#
#df2 = pd.read_csv('C:\data\Alex.csv', delimiter=',')
#sns.set_style("whitegrid")
#sns.set(font_scale=1.5)  # increase font on axis
#ax = sns.boxplot(x="Month", y="chla", data=df2, showmeans=True, color='white')
#ax.set(ylim=(0.08, 6))
