# -*- coding: utf-8 -*-
"""
Created on Fri Jun 23 13:52:46 2017

@author: TinaB
"""
###########################################################
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy
##############################################
## define path where graphs are saved
datapath_out = 'C:\\data\\'

from core.ecan_io import rd_squalarc
sites1 = ['SQ30144']
sitename = 'Marion'
   
def plot_chl_box():
    filename = "chla_box_" + sitename 
    print sitename       
    wq1 = rd_squalarc(sites1)
    
    wq1['parameter'].sort_values().unique().tolist()
    #wq1['source'].sort_values().unique().tolist()
    wq1['month'] = wq1.date.dt.month
    wq1['date'] = wq1.date.dt.date    
   
    # To create numpy float array with chla data, if below DL then assign value 0.1
    #chla_values = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['val'].astype(float).fillna(0.1).values
    chla_1 = pd.to_numeric(wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['val'], errors='coerce') 
    chla_values = chla_1.astype(float).fillna(0.1).values
    chla_month = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['month'].astype(int).values
    #print chla_values
    #print chla_month
    
    ## Chla is stored in two columns in Squalarc with two diffrent units, so again for second column:
    # Create numpy float array
    #chla_values2 = wq1[wq1['parameter'] == 'Chlorophyll a']['val'].astype(float).fillna(0.0001).values
    chla_2 = pd.to_numeric(wq1[wq1['parameter'] == 'Chlorophyll a']['val'], errors='coerce') 
    chla_values2 = chla_2.astype(float).fillna(0.0001).values
                 
    chla_month2 = wq1[wq1['parameter'] == 'Chlorophyll a']['month'].astype(int).values
    #print len(chla_values2)
    #print len(chla_month2)
    
    # Second chla array needs to be converted to microg/L
    # NOTE: Some values are in wrong unit in this column. 
    # Don't convert values that are above 0.5, multiply other by 1000 to get microg/L
    chla = numpy.zeros(len(chla_values2))
    for i in range (0,len(chla_values2)):
        if (chla_values2[i] < 0.19):
            chla[i] = 1000.0*chla_values2[i]
        else:
            chla[i] = chla_values2[i]
    print chla
    print len(chla)    
   
    # now combine the two arrays
    chla_month_combined1 =  numpy.concatenate([chla_month,chla_month2])
    chla_values_combined1 = numpy.concatenate([chla_values,chla])
    
    ## get rid of 55 datapoint
    for i in range (0,len(chla_values_combined1)):
        if (chla_values_combined1[i] == 55):
            chla_values_combined1[i] = 2.0
        else:
            chla_values_combined1[i] = chla_values_combined1[i]
            
    # Only use if all months should be shown in plot. Note: values wont be shown in plot
    #!!! This do not work for lakes that have old data outside Dec-May
    if (sitename == 'Katrine') or (sitename == 'Selfe') or (sitename == 'Ida')  or (sitename == 'Georgina') or (sitename == 'Heron') or (sitename == 'Taylor'):
            othermonths= [6,7,8,9]
            fake_values= [-1,-1,-1,-1]
            chla_month_combined =  numpy.concatenate([chla_month_combined1,othermonths])
            chla_values_combined = numpy.concatenate([chla_values_combined1,fake_values])
    else:
        othermonths= [6,7,8,9,10,11]
        fake_values= [-1,-1,-1,-1,-1,-1]
        chla_month_combined =  numpy.concatenate([chla_month_combined1,othermonths])
        chla_values_combined = numpy.concatenate([chla_values_combined1,fake_values])
    
    # Boxplot chla vs month
    sns.set_style("whitegrid")
    sns.set(font_scale=1.5)  # increase font on axis
    ax = sns.boxplot(x=chla_month_combined, y=chla_values_combined, showmeans=True, color='white')
    plt.ylim(ymin=0)
    ax.set(xlabel = 'Month', ylabel = 'chla')
    plt.savefig(str(datapath_out)+filename+'.jpg')
    plt.close()
    #wq1 = 0.0

plot_chl_box()    

   

