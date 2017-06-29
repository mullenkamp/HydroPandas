# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 10:58:59 2017

@author: TinaB
"""

###########################################################
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy
import scipy.stats as stats
##############################################
## define path where graphs are saved
datapath_out = 'C:\\data\\csv\\dl\\TLI\\'

#### Reading squalarc data
from core.ecan_io import rd_squalarc
sites1 = ['SQ30147']
sitename = 'Katrine'

mtypes = ['Total Phosphorus', 'Total Nitrogen', 'Chlorophyll a', 'Chlorophyll a (plankton)']

def TLI(x):
    print sitename       
    wq1 = rd_squalarc(x,  mtypes=mtypes, from_date='2004-01-12', to_date='2017-05-01', convert_dtl=True, dtl_method='trend')
    wq1 = wq1.sort_values('date')
    wq1['date'] = wq1.date.dt.date
    dates = wq1['date']
   
    # To create numpy float array with TP data, if below DL then assign value 
    TP_values = 1000*pd.to_numeric(wq1[wq1['parameter'] == 'Total Phosphorus']['val_dtl']) 
    
    # To create numpy float array with TN data, if below DL then assign value
    TN_values = 1000.0*pd.to_numeric(wq1[wq1['parameter'] == 'Total Nitrogen']['val_dtl']) 
        
#    # To create numpy float array with chla data
#    chla_values = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['val_dtl'].astype(float).values
#    
#    ## Chla is stored in two columns in Squalarc with two diffrent units, so again for second column:
#    # Create numpy float array
#    chla_values2 = wq1[wq1['parameter'] == 'Chlorophyll a']['val_dtl'].astype(float).values
#    
#    # Second chla array needs to be converted to microg/L
#    # NOTE: Some values are in wrong unit in this column. 
#    # Don't convert values that are above 0.5, multiply other by 1000 to get microg/L
#    chla = numpy.zeros(len(chla_values2))
#    for i in range (0,len(chla_values2)):
#        if (chla_values2[i] < 0.5):
#            chla[i] = 1000.0*chla_values2[i]
#        else:
#            chla[i] = chla_values2[i]
#    
#    # now combine the two arrays
#    chla_a = numpy.concatenate([chla_values,chla])

    ## make csv file with Date,TP
    raw_data = {'Date': dates,'TP': TP_values, 'TN' : TN_values}#, 'chla' : chla_a}
    df = pd.DataFrame(raw_data, columns = ['Date', 'TP', 'TN'])#, 'chla'])
    df.to_csv(str(datapath_out)+'TLI'+sitename+'.csv', header= ['Date','TP','TN'])#, 'chla'])
    
TLI(sites1)

sites1 = ['SQ30079']
sitename = 'Sumner'
TLI(sites1)

sites1 = ['SQ30141']
sitename = 'Taylor'
TLI(sites1)

sites1 = ['SQ30140']
sitename = 'Sheppard'
TLI(sites1)

sites1 = ['SQ30144']
sitename = 'Marion'
TLI(sites1)

sites1 = ['SQ35642']
sitename = 'Mason'
TLI(sites1)

sites1 = ['SQ35362']
sitename ='Emily'
TLI(sites1)

sites1 = ['SQ35363']
sitename = 'MaoriFront'
TLI(sites1)

sites1 = ['SQ35364']
sitename = 'MaoriBack'
TLI(sites1)

sites1 = ['SQ35888']
sitename = 'Denny'
TLI(sites1)

sites1 = ['SQ31093']
sitename = 'Heron'
TLI(sites1)

sites1 = ['SQ32801']
sitename = 'Emma'
TLI(sites1)

sites1 = ['SQ32802']
sitename = 'Camp'
TLI(sites1)

sites1 = ['SQ32804']
sitename = 'Clearwater'
TLI(sites1)

sites1 = ['SQ30521']
sitename = 'Sarah'
TLI(sites1)

sites1 = ['SQ30525']
sitename = 'Grasmere'
TLI(sites1)

sites1 = ['SQ30497']
sitename = 'Pearson'
TLI(sites1)

sites1 = ['SQ30486']
sitename = 'Hawdon'
TLI(sites1)

sites1 = ['SQ31043']
sitename = 'Lyndon'
TLI(sites1)

sites1 = ['SQ31047']
sitename = 'Georgina'
TLI(sites1)

sites1 = ['SQ31051']
sitename = 'Ida'
TLI(sites1)

sites1 = ['SQ31065']
sitename = 'Selfe'
TLI(sites1)

sites1 = ['SQ31045']
sitename = 'Coleridge'
TLI(sites1)

sites1 = ['SQ31050']
sitename = 'Evelyn'
TLI(sites1)

sites1 = ['SQ31052']
sitename = 'Catherine'
TLI(sites1)

sites1 = ['SQ31064']
sitename = 'Henrietta'
TLI(sites1)

sites1 = ['SQ35823']
sitename = 'McGregor'
TLI(sites1)
 
sites1 = ['SQ20927']
sitename = 'Middleton'
TLI(sites1)

sites1 = ['SQ31096']
sitename = 'Alexandrina'
TLI(sites1)

sites1 = ['SQ32908']
sitename = 'Tekapo'
TLI(sites1)

sites1 = ['SQ34908']
sitename = 'Pukaki'
TLI(sites1)

sites1 = ['SQ32909']
sitename = 'Ohau'
TLI(sites1)

sites1 = ['SQ34907']
sitename = 'Benmore_Haldon'
TLI(sites1)

sites1 = ['SQ35639']
sitename = 'Benmore_Ahuriri'
TLI(sites1)

sites1 = ['SQ35640']
sitename = 'Benmore_Dam'
TLI(sites1)

sites1 = ['SQ35641']
sitename = 'Aviemore'
TLI(sites1)

sites1 = ['SQ35833']
sitename = 'Kellands_mid'
TLI(sites1)

sites1 = ['SQ10805']
sitename = 'Kellands_shore'
TLI(sites1)
