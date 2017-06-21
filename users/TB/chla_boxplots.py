# -*- coding: utf-8 -*-
"""
Created on Fri May 19 10:52:03 2017

@author: TinaB
"""
####### Plot seasonal chla boxplot from Squalarc for a number of sites
## 19 May 2017

###########################################################
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy
##############################################
## define path where graphs are saved
datapath_out = 'C:\\data\\'

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

#### Reading squalarc data
from core.ecan_io import rd_squalarc
sites1 = ['SQ31096']
sitename = 'Alexandrina'

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
    
    # Only use if all months should be shown in plot. Note: values wont be shown in plot
    #!!! This do not work for lakes that have old data outside Dec-May
    if (sitename == 'Katrine') or (sitename == 'Selfe') or (sitename == 'Ida')  or (sitename == 'Georgina') or (sitename == 'Heron') or (sitename == 'Taylor'):
            othermonths= [6,7,8,9]
            fake_values= [-1,-1,-1,-1]
            chla_month_combined =  numpy.concatenate([chla_month_combined1,othermonths])
            chla_values_combined = numpy.concatenate([chla_values_combined1,fake_values])
    elif (sitename == 'Henrietta') or (sitename == 'Middleton') or (sitename == 'Pearson') or (sitename == 'Grasmere') or (sitename == 'Sarah'):
            othermonths= [6,7,8,9,11]
            fake_values= [-1,-1,-1,-1,-1]
            chla_month_combined =  numpy.concatenate([chla_month_combined1,othermonths])
            chla_values_combined = numpy.concatenate([chla_values_combined1,fake_values])
    elif (sitename == 'BenmoreHaldon') or (sitename == 'BenmoreAhuriri') or (sitename == 'BenmoreDam'):
            othermonths= [6,7,8,9,10]
            fake_values= [-1,-1,-1,-1,-1]
            chla_month_combined =  numpy.concatenate([chla_month_combined1,othermonths])
            chla_values_combined = numpy.concatenate([chla_values_combined1,fake_values])    
    elif (sitename == 'Kellands-mid') or (sitename == 'Kellands-shore'):
        chla_month_combined =  chla_month_combined1
        chla_values_combined = chla_values_combined1
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

sites1 = ['SQ32801']
sitename = 'Emma'
plot_chl_box()     

sites1 = ['SQ30147']
sitename = 'Katrine'
plot_chl_box()     

sites1 = ['SQ30079']
sitename = 'Sumner'
plot_chl_box()

sites1 = ['SQ30141']
sitename = 'Taylor'
plot_chl_box()     

sites1 = ['SQ30140']
sitename = 'Sheppard'
plot_chl_box()     

sites1 = ['SQ30144']
sitename = 'Marion'
plot_chl_box()     

sites1 = ['SQ35642']
sitename = 'Mason'
plot_chl_box()
  
sites1 = ['SQ35362']
sitename ='Emily'
plot_chl_box()   

sites1 = ['SQ35363']
sitename = 'Maori_Lake_Front'
plot_chl_box()  

sites1 = ['SQ35364']
sitename = 'Maori_Lake_Back'
plot_chl_box()  

sites1 = ['SQ35888']
sitename = 'Denny'
plot_chl_box()  

sites1 = ['SQ31093']
sitename = 'Heron'
plot_chl_box()  

sites1 = ['SQ32802']
sitename = 'Camp'
plot_chl_box()  

sites1 = ['SQ32804']
sitename = 'Clearwater'
plot_chl_box()  

sites1 = ['SQ30521']
sitename = 'Sarah'
plot_chl_box()  

sites1 = ['SQ30525']
sitename = 'Grasmere'
plot_chl_box()  

sites1 = ['SQ30497']
sitename = 'Pearson'
plot_chl_box()  

sites1 = ['SQ30486']
sitename = 'Hawdon'
plot_chl_box()  

sites1 = ['SQ31043']
sitename = 'Lyndon'
plot_chl_box()  

sites1 = ['SQ31047']
sitename = 'Georgina'
plot_chl_box()  

sites1 = ['SQ31051']
sitename = 'Ida'
plot_chl_box()  

sites1 = ['SQ31065']
sitename = 'Selfe'
plot_chl_box()  

sites1 = ['SQ31045']
sitename = 'Coleridge'
plot_chl_box()  

sites1 = ['SQ31050']
sitename = 'Evelyn'
plot_chl_box()  

sites1 = ['SQ31052']
sitename = 'Catherine'
plot_chl_box()  

sites1 = ['SQ31064']
sitename = 'Henrietta'
plot_chl_box()  

sites1 = ['SQ35823']
sitename = 'McGregor'
plot_chl_box() 
 
sites1 = ['SQ20927']
sitename = 'Middleton'
plot_chl_box()  

#sites1 = ['SQ31096']
#sitename = 'Alexandrina'
#plot_chl_box()  

sites1 = ['SQ32908']
sitename = 'Tekapo'
plot_chl_box()  

sites1 = ['SQ34908']
sitename = 'Pukaki'
plot_chl_box()  

sites1 = ['SQ32909']
sitename = 'Ohau'
plot_chl_box()  

sites1 = ['SQ34907']
sitename = 'BenmoreHaldon'
plot_chl_box()  

sites1 = ['SQ35639']
sitename = 'BenmoreAhuriri'
plot_chl_box()  

sites1 = ['SQ35640']
sitename = 'BenmoreDam'
plot_chl_box()  

sites1 = ['SQ35641']
sitename = 'Aviemore'
plot_chl_box()  

sites1 = ['SQ35833']
sitename = 'Kellands-mid'
plot_chl_box()  

sites1 = ['SQ10805']
sitename = 'Kellands-shore'
plot_chl_box()  
