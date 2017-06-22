# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 08:05:17 2017

@author: TinaB
"""
####### Plot regplots for all lakes for TN,TP, chla

# site list
#SQ30147	Loch (Lake) Katrine	
#SQ30079	Lake Sumner	
#SQ30141	Lake Taylor	
#SQ30140	Lake Sheppard	
#SQ30144	Lake Marion	
#SQ35642	Lake Mason	
#SQ35362	Lake Emily	
#SQ35363	Maori Lake Front	
#SQ35364	Maori Lake Back	
#SQ35888	Lake Denny	
#SQ31093	Lake Heron	
#SQ32801	Lake Emma	
#SQ32802	Lake Camp	
#SQ32804	Lake Clearwater	
#SQ30521	Lake Sarah	
#SQ30525	Lake Grasmere	
#SQ30497	Lake Pearson	
#SQ30486	Lake Hawdon	
#SQ31043	Lake Lyndon	
#SQ31047	Lake Georgina	
#SQ31051	Lake Ida	
#SQ31065	Lake Selfe	
#SQ31045	Lake Coleridge	
#SQ31050	Lake Evelyn	
#SQ31052	Lake Catherine	
#SQ31064	Lake Henrietta	
#SQ35823	Lake McGregor	
#SQ20927	Lake Middleton	
#SQ31096	Lake Alexandrina	
#SQ32908	Lake Tekapo	
#SQ34908	Lake Pukaki*	
#SQ32909	Lake Ohau*	
#SQ34907	Lake Benmore - Haldon	
#SQ35639	Lake Benmore- Ahuriri	
#SQ35640	Lake Benmore - Dam	
#SQ35641	Lake Aviemore	
#SQ35833	Kellands Pond off 2nd point	
#SQ10805	Kellands Pond shore	

###########################################################
#import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy
#import scipy.stats as stats
##############################################
## define path where graphs are saved
datapath_out = 'C:\\data\\scatterplots\\TP\\'
datapath_out2  = 'C:\\data\\scatterplots\\TN\\'
datapath_out3  = 'C:\\data\\scatterplots\\chla\\'
datapath_out4  = 'C:\\data\\scatterplots\\Turbidity\\'

#### Reading squalarc data
from core.ecan_io import rd_squalarc
sites1 = ['SQ30147']
sitename = 'Katrine'
mtypes = ['Total Phosphorus', 'Total Nitrogen', 'Chlorophyll a', 'Chlorophyll a (plankton)', 'Turbidity', 'date']
#wq1 = rd_squalarc(['SQ30147'], mtypes=mtypes, from_date='2004-01-12', to_date='2017-05-01', convert_dtl=True, dtl_method='trend')

def extract_TP(x):
    print sitename       
    wq1 = rd_squalarc(x, mtypes=mtypes, from_date='2004-01-12', to_date='2017-06-01', convert_dtl=True, dtl_method='trend')
    wq1 = wq1.sort_values('date')
    #wq1['parameter'].sort_values('date').unique().tolist()      
    wq1['date'] = wq1.date.dt.date  
       
    ## TP #####
    dates_TP = wq1[wq1['parameter'] == 'Total Phosphorus']['date']
    #print dates_TP
    
#    dates_TP = wq1[wq1['parameter'] == 'Total Phosphorus']['date'].astype(int).values
#    print dates_TP
      
    # To create numpy float array with TP data, if below DL then assign value 
    TP_values = 1000*pd.to_numeric(wq1[wq1['parameter'] == 'Total Phosphorus']['val_dtl'], errors='ignore')
    #print TP_values
    
#    ## make new dataframe
    raw_data = {'Date': dates_TP,'TP': TP_values}
    df = pd.DataFrame(raw_data, columns = ['Date', 'TP'])
    
    # Plot regplt
    filename = sitename+'_scatter_TP_'
    #sns.regplot(x='Date', y='TP', data = df);
    #plt.plot(time = df['Date'], df['TP']);
    #sns.tsplot(data = df['TP'], time = df['Date']);
    #plt.scatter(dates_TP,TP_values)
    df[['Date','TP']].set_index('Date').plot(style='.', title = sitename)
    plt.legend(frameon=True, facecolor = 'white', framealpha = 1.0)
    plt.show()
    plt.savefig(str(datapath_out)+filename+'.jpg')
    plt.close()
    
    ##to transfer datetime to float: mdates.strpdate2num()
    # or see https://docs.python.org/3/library/time.html#time.strftime
    
    ### TN ###
    # To create numpy float array with TN data, if below DL then assign value
    dates_TN = wq1[wq1['parameter'] == 'Total Nitrogen']['date']
    TN_values = 1000.0*pd.to_numeric(wq1[wq1['parameter'] == 'Total Nitrogen']['val_dtl'], errors='coerce') 
#            
    ## make csv file with Date,TN
    raw_data2 = {'Date': dates_TN,'TN': TN_values}
    df2 = pd.DataFrame(raw_data2, columns = ['Date', 'TN'])
    
  # Plot regplt
    filename = sitename+'_scatter_TN_'
    df2[['Date','TN']].set_index('Date').plot(style='.', title = sitename)
    plt.legend(frameon=True, facecolor = 'white', framealpha = 1.0)
    plt.show()
    plt.savefig(str(datapath_out2)+filename+'.jpg')
    plt.close()

    ## chla ###
    # To create numpy float array with chla data
    dates_chla1 = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['date']
    chla_values = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['val_dtl'].astype(float).values
#    under_dl = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['dtl_ratio'].astype(float).values
#    print 'chla', under_dl[1]
    
#    ## Chla is stored in two columns in Squalarc with two diffrent units, so again for second column:
#    # Create numpy float array
    dates_chla2 = wq1[wq1['parameter'] == 'Chlorophyll a']['date']
    chla_values2 = wq1[wq1['parameter'] == 'Chlorophyll a']['val_dtl'].astype(float).values
#    under_dl = wq1[wq1['parameter'] == 'Chlorophyll a']['dtl_ratio'].astype(float).values
#    print 'chla', under_dl[1]
    
    # Second chla array needs to be converted to microg/L
    # NOTE: Some values are in wrong unit in this column. 
    # Don't convert values that are above 0.5, multiply other by 1000 to get microg/L
    chla = numpy.zeros(len(chla_values2))
    for i in range (0,len(chla_values2)):
        if (chla_values2[i] < 0.19):
            chla[i] = 1000.0*chla_values2[i]
        #else if (sitename == 'Aviemore') or (sitename == 'Selfe') and (chla_values2[i] < 0.19):
        else:
            chla[i] = chla_values2[i]
    
    # now combine the two arrays
    chla_dates = numpy.concatenate([dates_chla1,dates_chla2])
    chla_a = numpy.concatenate([chla_values,chla])
    
    ## make csv file with Date,TN
    raw_data3 = {'Date': chla_dates,'chla': chla_a}
    df3 = pd.DataFrame(raw_data3, columns = ['Date', 'chla'])
  # Plot regplt
    filename = sitename+'_scatter_chla_'
    df3[['Date','chla']].set_index('Date').plot(style='.', title = sitename)
    plt.legend(frameon=True, facecolor = 'white', framealpha = 1.0)
    plt.show()
    plt.savefig(str(datapath_out3)+filename+'.jpg')
    plt.close()

    ### Turbidity ###
    # To create numpy float array with TN data, if below DL then assign value
    dates_Turbidity = wq1[wq1['parameter'] == 'Turbidity']['date']
    Turbidity_values = pd.to_numeric(wq1[wq1['parameter'] == 'Turbidity']['val'], errors='coerce') 
#            
    ## make csv file with Date,TN
    raw_data4 = {'Date': dates_Turbidity,'Turbidity': Turbidity_values}
    df4 = pd.DataFrame(raw_data4, columns = ['Date', 'Turbidity'])
    
  # Plot regplt
    filename = sitename+'_scatter_Turbidity_'
    df4[['Date','Turbidity']].set_index('Date').plot(style='.', title = sitename)
    plt.legend(frameon=True, facecolor = 'white', framealpha = 1.0)
    plt.show()
    plt.savefig(str(datapath_out4)+filename+'.jpg')
    plt.close()
extract_TP(sites1)

sites1 = ['SQ30079']
sitename = 'Sumner'
extract_TP(sites1)

sites1 = ['SQ30141']
sitename = 'Taylor'
extract_TP(sites1)

sites1 = ['SQ30140']
sitename = 'Sheppard'
extract_TP(sites1)

sites1 = ['SQ30144']
sitename = 'Marion'
extract_TP(sites1)

sites1 = ['SQ35642']
sitename = 'Mason'
extract_TP(sites1)

sites1 = ['SQ35362']
sitename ='Emily'
extract_TP(sites1)

sites1 = ['SQ35363']
sitename = 'MaoriFront'
extract_TP(sites1)

sites1 = ['SQ35364']
sitename = 'MaoriBack'
extract_TP(sites1)

sites1 = ['SQ35888']
sitename = 'Denny'
extract_TP(sites1)

sites1 = ['SQ31093']
sitename = 'Heron'
extract_TP(sites1)

sites1 = ['SQ32801']
sitename = 'Emma'
extract_TP(sites1)

sites1 = ['SQ32802']
sitename = 'Camp'
extract_TP(sites1)

sites1 = ['SQ32804']
sitename = 'Clearwater'
extract_TP(sites1)

sites1 = ['SQ30521']
sitename = 'Sarah'
extract_TP(sites1)

sites1 = ['SQ30525']
sitename = 'Grasmere'
extract_TP(sites1)

sites1 = ['SQ30497']
sitename = 'Pearson'
extract_TP(sites1)

sites1 = ['SQ30486']
sitename = 'Hawdon'
extract_TP(sites1)

sites1 = ['SQ31043']
sitename = 'Lyndon'
extract_TP(sites1)

sites1 = ['SQ31047']
sitename = 'Georgina'
extract_TP(sites1)

sites1 = ['SQ31051']
sitename = 'Ida'
extract_TP(sites1)

sites1 = ['SQ31065']
sitename = 'Selfe'
extract_TP(sites1)

sites1 = ['SQ31045']
sitename = 'Coleridge'
extract_TP(sites1)

sites1 = ['SQ31050']
sitename = 'Evelyn'
extract_TP(sites1)

sites1 = ['SQ31052']
sitename = 'Catherine'
extract_TP(sites1)

sites1 = ['SQ31064']
sitename = 'Henrietta'
extract_TP(sites1)

sites1 = ['SQ35823']
sitename = 'McGregor'
extract_TP(sites1)
 
sites1 = ['SQ20927']
sitename = 'Middleton'
extract_TP(sites1)

sites1 = ['SQ31096']
sitename = 'Alexandrina'
extract_TP(sites1)

sites1 = ['SQ32908']
sitename = 'Tekapo'
extract_TP(sites1)

sites1 = ['SQ34908']
sitename = 'Pukaki'
extract_TP(sites1)

sites1 = ['SQ32909']
sitename = 'Ohau'
extract_TP(sites1)

sites1 = ['SQ34907']
sitename = 'Benmore_Haldon'
extract_TP(sites1)

sites1 = ['SQ35639']
sitename = 'Benmore_Ahuriri'
extract_TP(sites1)

sites1 = ['SQ35640']
sitename = 'Benmore_Dam'
extract_TP(sites1)

sites1 = ['SQ35641']
sitename = 'Aviemore'
extract_TP(sites1)

sites1 = ['SQ35833']
sitename = 'Kellands_mid'
extract_TP(sites1)

sites1 = ['SQ10805']
sitename = 'Kellands_shore'
extract_TP(sites1)
