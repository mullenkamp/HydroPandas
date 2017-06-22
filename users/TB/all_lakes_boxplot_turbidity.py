# -*- coding: utf-8 -*-
"""
Created on Mon May 29 12:28:19 2017

@author: TinaB
"""
###########################################################
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
#import numpy
##############################################
## define path where graphs are saved
datapath_out = 'C:\\data\\'

#### Reading squalarc data
from core.ecan_io import rd_squalarc
sites1 = ['SQ30147']
sitename = 'Katrine'

def extract_Turbidity(x):
    print sitename       
    wq1 = rd_squalarc(x, from_date='2012-01-12', to_date='2017-05-01')   
    wq1['parameter'].sort_values().unique().tolist()
    wq1['date'] = wq1.date.dt.date    
   
    # To create numpy float array with Turbidity data, if below DL then assign value 0.1
    Turbidity_1 = pd.to_numeric(wq1[wq1['parameter'] == 'Turbidity']['val'], errors='coerce') 
    Turbidity_values = Turbidity_1.astype(float).fillna(0.025).values  
    y = Turbidity_values
    return y
    
Turbidity_site1 = pd.DataFrame({sitename:extract_Turbidity(sites1)})
print len(Turbidity_site1)

sites1 = ['SQ30079']
sitename = 'Sumner'
Turbidity_site2 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ30141']
sitename = 'Taylor'
Turbidity_site3 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ30140']
sitename = 'Sheppard'
Turbidity_site4 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ30144']
sitename = 'Marion'
Turbidity_site5 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35642']
sitename = 'Mason'
Turbidity_site6 = pd.DataFrame({sitename:extract_Turbidity(sites1)})
  
sites1 = ['SQ35362']
sitename ='Emily'
Turbidity_site7 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35363']
sitename = 'MaoriFront'
Turbidity_site8 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35364']
sitename = 'MaoriBack'
Turbidity_site9 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35888']
sitename = 'Denny'
Turbidity_site10 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31093']
sitename = 'Heron'
Turbidity_site11 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ32801']
sitename = 'Emma'
Turbidity_site12 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ32802']
sitename = 'Camp'
Turbidity_site13 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ32804']
sitename = 'Clearwater'
Turbidity_site14 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ30521']
sitename = 'Sarah'
Turbidity_site15 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ30525']
sitename = 'Grasmere'
Turbidity_site16 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ30497']
sitename = 'Pearson'
Turbidity_site17 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ30486']
sitename = 'Hawdon'
Turbidity_site18 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31043']
sitename = 'Lyndon'
Turbidity_site19 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31047']
sitename = 'Georgina'
Turbidity_site20 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31051']
sitename = 'Ida'
Turbidity_site21 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31065']
sitename = 'Selfe'
Turbidity_site22 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31045']
sitename = 'Coleridge'
Turbidity_site23 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31050']
sitename = 'Evelyn'
Turbidity_site24 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31052']
sitename = 'Catherine'
Turbidity_site25 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31064']
sitename = 'Henrietta'
Turbidity_site26 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35823']
sitename = 'McGregor'
Turbidity_site27 = pd.DataFrame({sitename:extract_Turbidity(sites1)})
 
sites1 = ['SQ20927']
sitename = 'Middleton'
Turbidity_site28 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ31096']
sitename = 'Alexandrina'
Turbidity_site29 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ32908']
sitename = 'Tekapo'
Turbidity_site30 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ34908']
sitename = 'Pukaki'
Turbidity_site31 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ32909']
sitename = 'Ohau'
Turbidity_site32 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ34907']
sitename = 'Benmore_Haldon'
Turbidity_site33 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35639']
sitename = 'Benmore_Ahuriri'
Turbidity_site34 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35640']
sitename = 'Benmore_Dam'
Turbidity_site35 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35641']
sitename = 'Aviemore'
Turbidity_site36 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ35833']
sitename = 'Kellands_mid'
Turbidity_site37 = pd.DataFrame({sitename:extract_Turbidity(sites1)})

sites1 = ['SQ10805']
sitename = 'Kellands_shore'
Turbidity_site38 = pd.DataFrame({sitename:extract_Turbidity(sites1)})


# Boxplot Turbidity for all lakes
Turbidity_data = pd.concat([Turbidity_site1, Turbidity_site2, Turbidity_site3, Turbidity_site4, Turbidity_site5, Turbidity_site6, Turbidity_site7, Turbidity_site8, Turbidity_site9,Turbidity_site10, Turbidity_site11, Turbidity_site12, Turbidity_site13, Turbidity_site14, Turbidity_site15, Turbidity_site16, Turbidity_site17, Turbidity_site18, Turbidity_site19,Turbidity_site20, Turbidity_site21, Turbidity_site22, Turbidity_site23, Turbidity_site24, Turbidity_site25, Turbidity_site26, Turbidity_site27, Turbidity_site28, Turbidity_site29, Turbidity_site30, Turbidity_site31, Turbidity_site32, Turbidity_site33, Turbidity_site34, Turbidity_site35, Turbidity_site36, Turbidity_site37, Turbidity_site38], axis=1)

# Boxplot
filename = "all lakes_Turbidity"
print 'plotting'
print filename
plt.figure(figsize=(22, 11))
sns.set_style("whitegrid")
sns.set(font_scale=2)  # increase font on axis
#ax = sns.boxplot(data = Turbidity_data, showmeans=True, color='white')
medianprops = dict(linewidth=2.5, color='grey')
meanpointprops = dict(marker='o', markerfacecolor='red', markersize=12,
                  linestyle='none')
ax = sns.boxplot(data = Turbidity_data, medianprops=medianprops, meanprops=meanpointprops, meanline=False,
               showmeans=True, color='white')
for item in ax.get_xticklabels():
    item.set_rotation(90)
plt.ylim(ymin=0, ymax=10)
#plt.plot(ln1x,ln1y, 'y-')
#plt.plot(ln2x,ln2y, 'r-')
plt.tight_layout()
#plt.xlabel('Lake', fontsize = 22)
plt.ylabel('Turbidity in NTU', fontsize = 22)
plt.savefig(str(datapath_out)+filename+'.jpg')
plt.close()
print 'finished plotting'