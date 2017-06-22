# -*- coding: utf-8 -*-
"""
Created on Mon May 29 11:19:46 2017

@author: TinaB
"""
####### Plot seasonal TP boxplot from Squalarc for a number of sites
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

## to calls rows TP.loc[18:, 'val']
# wq1.date > '2010'
#from core.ecan_io import rd_squalarc
#wq1 = rd_squalarc(['SQ30147'], from_date='2010-01-01', to_date='2016-01-01')
#wq1.sort_values('date')

#Total Nitrogen, Turbidity

###########################################################
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy
##############################################
## define path where graphs are saved
datapath_out = 'C:\\data\\all_lakes\\'

#### Reading squalarc data
from core.ecan_io import rd_squalarc
sites1 = ['SQ30147']
sitename = 'Katrine'

def extract_TP(x):
    print sitename       
    wq1 = rd_squalarc(x, from_date='2012-01-12', to_date='2017-05-01')
    
    wq1['parameter'].sort_values().unique().tolist()
#    wq1['month'] = wq1.date.dt.month
    wq1['date'] = wq1.date.dt.date 
   
    # To create numpy float array with TP data, if below DL then assign value 0.1
    TP_1 = pd.to_numeric(wq1[wq1['parameter'] == 'Total Phosphorus']['val'], errors='coerce')
    TP_values = TP_1.astype(float).fillna(0.002).values
    TP = numpy.zeros(len(TP_values))
    for i in range (0,len(TP_values)):
        if ((sitename == 'Sumner') or (sitename == 'Coleridge') or (sitename == 'Benmore_Haldon')) and (TP_values[i] > 0.055): 
            TP[i] = 2.0            
        elif ((sitename == 'Marion') and (TP_values[i] > 0.3)):
             TP[i] = 13.0
        else:
            TP[i] = 1000.0*TP_values[i]            
    y = TP
    return y
    
TP_site1 = pd.DataFrame({sitename:extract_TP(sites1)})
print len(TP_site1)

def extract_TN(x):
    print sitename       
    wq1 = rd_squalarc(x, from_date='2012-01-12', to_date='2017-05-01')
    
    wq1['parameter'].sort_values().unique().tolist()
#    wq1['month'] = wq1.date.dt.month
    wq1['date'] = wq1.date.dt.date
   
    # To create numpy float array with TN data, if below DL then assign value 0.1
    TN_1 = pd.to_numeric(wq1[wq1['parameter'] == 'Total Nitrogen']['val'], errors='coerce') 
    TN_values = TN_1.astype(float).fillna(0.005).values
    TN = numpy.zeros(len(TN_values))
    for i in range (0,len(TN_values)):
        if ((sitename == 'Marion') and (TN_values[i] > 1.3)):
             TN[i] = 350.0
        else:
            TN[i] = 1000.0*TN_values[i]                 
    y = TN
    return y
    
TN_site1 = pd.DataFrame({sitename:extract_TN(sites1)})
print len(TN_site1)

sites1 = ['SQ30079']
sitename = 'Sumner'
TP_site2 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site2 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ30141']
sitename = 'Taylor'
TP_site3 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site3 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ30140']
sitename = 'Sheppard'
TP_site4 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site4 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ30144']
sitename = 'Marion'
TP_site5 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site5 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35642']
sitename = 'Mason'
TP_site6 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site6 = pd.DataFrame({sitename:extract_TN(sites1)})
  
sites1 = ['SQ35362']
sitename ='Emily'
TP_site7 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site7 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35363']
sitename = 'MaoriFront'
TP_site8 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site8 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35364']
sitename = 'MaoriBack'
TP_site9 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site9 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35888']
sitename = 'Denny'
TP_site10 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site10 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31093']
sitename = 'Heron'
TP_site11 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site11 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ32801']
sitename = 'Emma'
TP_site12 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site12 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ32802']
sitename = 'Camp'
TP_site13 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site13 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ32804']
sitename = 'Clearwater'
TP_site14 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site14 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ30521']
sitename = 'Sarah'
TP_site15 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site15 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ30525']
sitename = 'Grasmere'
TP_site16 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site16 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ30497']
sitename = 'Pearson'
TP_site17 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site17 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ30486']
sitename = 'Hawdon'
TP_site18 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site18 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31043']
sitename = 'Lyndon'
TP_site19 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site19 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31047']
sitename = 'Georgina'
TP_site20 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site20 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31051']
sitename = 'Ida'
TP_site21 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site21 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31065']
sitename = 'Selfe'
TP_site22 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site22 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31045']
sitename = 'Coleridge'
TP_site23 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site23 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31050']
sitename = 'Evelyn'
TP_site24 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site24 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31052']
sitename = 'Catherine'
TP_site25 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site25 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31064']
sitename = 'Henrietta'
TP_site26 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site26 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35823']
sitename = 'McGregor'
TP_site27 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site27 = pd.DataFrame({sitename:extract_TN(sites1)})
 
sites1 = ['SQ20927']
sitename = 'Middleton'
TP_site28 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site28 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ31096']
sitename = 'Alexandrina'
TP_site29 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site29 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ32908']
sitename = 'Tekapo'
TP_site30 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site30 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ34908']
sitename = 'Pukaki'
TP_site31 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site31 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ32909']
sitename = 'Ohau'
TP_site32 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site32 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ34907']
sitename = 'Benmore_Haldon'
TP_site33 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site33 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35639']
sitename = 'Benmore_Ahuriri'
TP_site34 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site34 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35640']
sitename = 'Benmore_Dam'
TP_site35 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site35 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35641']
sitename = 'Aviemore'
TP_site36 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site36 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ35833']
sitename = 'Kellands_mid'
TP_site37 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site37 = pd.DataFrame({sitename:extract_TN(sites1)})

sites1 = ['SQ10805']
sitename = 'Kellands_shore'
TP_site38 = pd.DataFrame({sitename:extract_TP(sites1)})
TN_site38 = pd.DataFrame({sitename:extract_TN(sites1)})


# Boxplot TP for all lakes
TP_data = pd.concat([TP_site1, TP_site2, TP_site3, TP_site4, TP_site5, TP_site6, TP_site7, TP_site8, TP_site9,TP_site10, TP_site11, TP_site12, TP_site13, TP_site14, TP_site15, TP_site16, TP_site17, TP_site18, TP_site19,TP_site20, TP_site21, TP_site22, TP_site23, TP_site24, TP_site25, TP_site26, TP_site27, TP_site28, TP_site29, TP_site30, TP_site31, TP_site32, TP_site33, TP_site34, TP_site35, TP_site36, TP_site37, TP_site38], axis=1)

### plot lines for mesotrophic and eutrophic thresholds
ln1x =  [0,1, 5, 10, 20, 30, 38]
ln1y = [9,9,9,9,9,9,9]

ln2x =  [0,1, 5, 10, 20, 30, 38]
ln2y = [20,20,20,20,20,20,20]

filename = "all lakes_TP"
print 'plotting'
print filename
plt.figure(figsize=(22, 11))
sns.set_style("whitegrid")
sns.set(font_scale=2)  # increase font on axis
medianprops = dict(linewidth=2.5, color='grey')
meanpointprops = dict(marker='o', markerfacecolor='red', markersize=12,
                  linestyle='none')
ax = sns.boxplot(data = TP_data, medianprops=medianprops, meanprops=meanpointprops, meanline=False,
               showmeans=True, color='white')
for item in ax.get_xticklabels():
    item.set_rotation(90)
plt.ylim(ymin=0, ymax=60)
plt.plot(ln1x,ln1y, 'y-')
plt.plot(ln2x,ln2y, 'r-')
plt.tight_layout()
#plt.xlabel('Lake', fontsize = 22)
plt.ylabel('TP in microg/L', fontsize = 22)
plt.savefig(str(datapath_out)+filename+'.jpg')
plt.close()
print 'finished plotting'

# Boxplot TN for all lakes
TN_data = pd.concat([TN_site1, TN_site2, TN_site3, TN_site4, TN_site5, TN_site6, TN_site7, TN_site8, TN_site9,TN_site10, TN_site11, TN_site12, TN_site13, TN_site14, TN_site15, TN_site16, TN_site17, TN_site18, TN_site19,TN_site20, TN_site21, TN_site22, TN_site23, TN_site24, TN_site25, TN_site26, TN_site27, TN_site28, TN_site29, TN_site30, TN_site31, TN_site32, TN_site33, TN_site34, TN_site35, TN_site36, TN_site37, TN_site38], axis=1)

### plot lines for mesotrophic and eutrophic thresholds
ln1x =  [0,1, 5, 10, 20, 30, 38]
ln1y = [157,157,157,157,157,157,157]

ln2x =  [0,1, 5, 10, 20, 30, 38]
ln2y = [337,337,337,337,337,337,337]

filename = "all lakes_TN"
print 'plotting'
print filename
plt.figure(figsize=(22, 11))
sns.set_style("whitegrid")
sns.set(font_scale=2)  # increase font on axis
medianprops = dict(linewidth=2.5, color='grey')
meanpointprops = dict(marker='o', markerfacecolor='red', markersize=12,
                  linestyle='none')
ax = sns.boxplot(data = TN_data, medianprops=medianprops, meanprops=meanpointprops, meanline=False,
               showmeans=True, color='white')
for item in ax.get_xticklabels():
    item.set_rotation(90)
plt.ylim(ymin=0, ymax=1000)
plt.plot(ln1x,ln1y, 'y-')
plt.plot(ln2x,ln2y, 'r-')
plt.tight_layout()
#plt.xlabel('Lake', fontsize = 22)
plt.ylabel('TN in microg/L', fontsize = 22)
plt.savefig(str(datapath_out)+filename+'.jpg')
plt.close()
print 'finished plotting'
# for polishing plot see https://matplotlib.org/examples/statistics/bxp_demo.html

########## TN to TP ratio

# Boxplot TN for all lakes
TNtoTP = TN_data/TP_data

### plot lines for mesotrophic and eutrophic thresholds
## Kelly Draft report
#ln1x =  [0,1, 5, 10, 20, 30, 38]
#ln1y = [3.5,3.5,3.5,3.5,3.5,3.5,3.5]
#
#ln2x =  [0,1, 5, 10, 20, 30, 38]
#ln2y = [14,14,14,14,14,14,14]

#Adrian 2007
#ln1x =  [0,1, 5, 10, 20, 30, 38]
#ln1y = [30,30,30,30,30,30,30]
#
#ln2x =  [0,1, 5, 10, 20, 30, 38]
#ln2y = [15,15,15,15,15,15,15]

# Abell 2010
ln1x =  [0,1, 5, 10, 20, 30, 38]
ln1y = [15,15,15,15,15,15,15]

ln2x =  [0,1, 5, 10, 20, 30, 38]
ln2y = [7,7,7,7,7,7,7]

filename = "all lakes_TNtoTP"
print 'plotting'
print filename
plt.figure(figsize=(22, 11))
sns.set_style("whitegrid")
sns.set(font_scale=2)  # increase font on axis
ax = sns.boxplot(data = TNtoTP,medianprops=medianprops, meanprops=meanpointprops, meanline=False,
               showmeans=True, color='white')
for item in ax.get_xticklabels():
    item.set_rotation(90)
plt.ylim(ymin=0, ymax=100)
plt.plot(ln1x,ln1y, 'b-')
plt.plot(ln2x,ln2y, 'g-')
plt.tight_layout()
#plt.xlabel('Lake', fontsize = 22)
plt.ylabel('TN to TP ratio', fontsize = 22)
plt.savefig(str(datapath_out)+filename+'.jpg')
plt.close()
print 'finished plotting'
#

#Adrian 2007
ln1x =  [0,1, 5, 10, 20, 30, 38]
ln1y = [30,30,30,30,30,30,30]

ln2x =  [0,1, 5, 10, 20, 30, 38]
ln2y = [15,15,15,15,15,15,15]


filename = "all lakes_TNtoTP_old_ratio"
print 'plotting'
print filename
plt.figure(figsize=(22, 11))
sns.set_style("whitegrid")
sns.set(font_scale=2)  # increase font on axis
ax = sns.boxplot(data = TNtoTP, medianprops=medianprops, meanprops=meanpointprops, meanline=False,
               showmeans=True, color='white')
for item in ax.get_xticklabels():
    item.set_rotation(90)
plt.ylim(ymin=0, ymax=100)
plt.plot(ln1x,ln1y, 'b-')
plt.plot(ln2x,ln2y, 'g-')
plt.tight_layout()
#plt.xlabel('Lake', fontsize = 22)
plt.ylabel('TN to TP ratio', fontsize = 22)
plt.savefig(str(datapath_out)+filename+'.jpg')
plt.close()
print 'finished plotting'
#


