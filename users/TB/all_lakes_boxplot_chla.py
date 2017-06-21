# -*- coding: utf-8 -*-
"""
Created on Fri May 26 13:06:26 2017

@author: TinaB
"""
# -*- coding: utf-8 -*-
"""
Created on Fri May 19 10:52:03 2017

@author: TinaB
"""
####### Plot seasonal chla boxplot from Squalarc for a number of sites
## 19 May 2017

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

## to calls rows chla.loc[18:, 'val']
# wq1.date > '2010'
#from core.ecan_io import rd_squalarc
#wq1 = rd_squalarc(['SQ30147'], from_date='2010-01-01', to_date='2016-01-01')
#wq1.sort_values('date')

###########################################################
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy
##############################################
## define path where graphs are saved
datapath_out = 'C:\\data\\'

#### Reading squalarc data
from core.ecan_io import rd_squalarc
sites1 = ['SQ30147']
sitename = 'Katrine'

def combine_chla(x):
    print sitename       
    wq1 = rd_squalarc(x, from_date='2012-01-12', to_date='2017-05-01')
    
    wq1['parameter'].sort_values().unique().tolist()
#    wq1['month'] = wq1.date.dt.month
    wq1['date'] = wq1.date.dt.date    
   
    # To create numpy float array with chla data, if below DL then assign value 0.1
    chla_1 = pd.to_numeric(wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['val'], errors='coerce') 
    chla_values = chla_1.astype(float).fillna(0.1).values
                               
    #chla_date = wq1[wq1['parameter'] == 'Chlorophyll a (plankton)']['date'].astype(int).values
    #print chla_values
    #print chla_month
    
    ## Chla is stored in two columns in Squalarc with two diffrent units, so again for second column:
    # Create numpy float array
    chla_2 = pd.to_numeric(wq1[wq1['parameter'] == 'Chlorophyll a']['val'], errors='coerce') 
    chla_values2 = chla_2.astype(float).fillna(0.0001).values               
    #chla_date2 = wq1[wq1['parameter'] == 'Chlorophyll a']['date'].astype(int).values
    
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
    #chla_date_combined =  numpy.concatenate([chla_date,chla_date2])
    chla_values_combined = numpy.concatenate([chla_values,chla])
    y = chla_values_combined
    return y
    
chla_site1 = pd.DataFrame({sitename:combine_chla(sites1)})
#print df1

sites1 = ['SQ30079']
sitename = 'Sumner'
chla_site2 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ30141']
sitename = 'Taylor'
chla_site3 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ30140']
sitename = 'Sheppard'
chla_site4 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ30144']
sitename = 'Marion'
chla_site5 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35642']
sitename = 'Mason'
chla_site6 = pd.DataFrame({sitename:combine_chla(sites1)})
  
sites1 = ['SQ35362']
sitename ='Emily'
chla_site7 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35363']
sitename = 'MaoriFront'
chla_site8 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35364']
sitename = 'MaoriBack'
chla_site9 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35888']
sitename = 'Denny'
chla_site10 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31093']
sitename = 'Heron'
chla_site11 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ32801']
sitename = 'Emma'
chla_site12 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ32802']
sitename = 'Camp'
chla_site13 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ32804']
sitename = 'Clearwater'
chla_site14 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ30521']
sitename = 'Sarah'
chla_site15 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ30525']
sitename = 'Grasmere'
chla_site16 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ30497']
sitename = 'Pearson'
chla_site17 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ30486']
sitename = 'Hawdon'
chla_site18 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31043']
sitename = 'Lyndon'
chla_site19 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31047']
sitename = 'Georgina'
chla_site20 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31051']
sitename = 'Ida'
chla_site21 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31065']
sitename = 'Selfe'
chla_site22 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31045']
sitename = 'Coleridge'
chla_site23 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31050']
sitename = 'Evelyn'
chla_site24 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31052']
sitename = 'Catherine'
chla_site25 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31064']
sitename = 'Henrietta'
chla_site26 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35823']
sitename = 'McGregor'
chla_site27 = pd.DataFrame({sitename:combine_chla(sites1)})
 
sites1 = ['SQ20927']
sitename = 'Middleton'
chla_site28 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ31096']
sitename = 'Alexandrina'
chla_site29 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ32908']
sitename = 'Tekapo'
chla_site30 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ34908']
sitename = 'Pukaki'
chla_site31 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ32909']
sitename = 'Ohau'
chla_site32 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ34907']
sitename = 'Benmore_Haldon'
chla_site33 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35639']
sitename = 'Benmore_Ahuriri'
chla_site34 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35640']
sitename = 'Benmore_Dam'
chla_site35 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35641']
sitename = 'Aviemore'
chla_site36 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ35833']
sitename = 'Kellands_mid'
chla_site37 = pd.DataFrame({sitename:combine_chla(sites1)})

sites1 = ['SQ10805']
sitename = 'Kellands_shore'
chla_site38 = pd.DataFrame({sitename:combine_chla(sites1)})


# Boxplot chla for all lakes
chla_data = pd.concat([chla_site1, chla_site2, chla_site3, chla_site4, chla_site5, chla_site6, chla_site7, chla_site8, chla_site9,chla_site10, chla_site11, chla_site12, chla_site13, chla_site14, chla_site15, chla_site16, chla_site17, chla_site18, chla_site19,chla_site20, chla_site21, chla_site22, chla_site23, chla_site24, chla_site25, chla_site26, chla_site27, chla_site28, chla_site29, chla_site30, chla_site31, chla_site32, chla_site33, chla_site34, chla_site35, chla_site36, chla_site37, chla_site38], axis=1)

### plot lines for mesotrop[hic and eutrophic thresholds]
ln1x =  [0,1, 5, 10, 20, 30, 38]
ln1y = [2,2, 2, 2, 2, 2, 2]

ln2x =  [0,1, 5, 10, 20, 30, 38]
ln2y = [5,5, 5, 5,5, 5, 5]

filename = "all lakes_chla"
print 'plotting'
print filename
plt.figure(figsize=(22, 11))
sns.set_style("whitegrid")
sns.set(font_scale=2)  # increase font on axis
#ax = sns.boxplot(data = df, showmeans=True, color='white')
#ax = sns.boxplot(x=sites, y=chla_data, showmeans=True, color='white')
ax = sns.boxplot(data = chla_data, showmeans=True, color='white')
                 #, line_kws={'xdata': '0,1','ydata': '0,0','color': 'k', 'linestyle':'-', 'linewidth':'5'})
for item in ax.get_xticklabels():
    item.set_rotation(90)
plt.ylim(ymin=0, ymax=15)
plt.plot(ln1x,ln1y, 'y-')
plt.plot(ln2x,ln2y, 'r-')
plt.tight_layout()
#plt.xlabel('Lake', fontsize = 22)
plt.ylabel('Chlorophyll a', fontsize = 22)
plt.savefig(str(datapath_out)+filename+'.jpg')
plt.close()
print 'finished plotting'
#

