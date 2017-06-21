# -*- coding: utf-8 -*-
"""
Created on Mon Jul 25 09:21:14 2016

@author: KateSt
"""
import pandas as pd
import numpy as np
import scipy.stats as sp
import seaborn as sns
import matplotlib.pyplot as plt
matplotlib.style.use('seaborn-darkgrid')
from pandas import read_csv

siteinfo = read_csv('C:/Users/KateSt/Desktop/OrariSH1.csv', skiprows=3, nrows=3, usecols=[0], header=0)
site = 'Orari at SH1'
sitename = 'Seadown Drain at Aorangi Road'
period = '08/08/2015 - 16/06/2016'

path = 'C:/Users/KateSt/Desktop/1698 Flow Duration Analysis.csv'
site1698 = read_csv('C:/Users/KateSt/Desktop/1698 Flow Duration Analysis.csv', 
                header=8, skiprows=[9], index_col=0, usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13], nrows=51)

site1698.sort_index(axis=0, ascending=True, inplace=True)                

site1698.plot(y='Total', title=site+' - '+sitename)               
plt.xlabel('Percentage of time above flow')
plt.ylabel('Flow (m^3/s)')
plt.plot([0,100],[0.2,0.2])

df = rd_hydstra_csv(csv_path='C:\Users\KateSt\Desktop\ORARISH1.csv', min_filter=False)
df['flow'] = sp.rankdata(df.iloc[:0])


plt.figure()
plt.scatter(prob,df,label='69510')

start_date = df_first_valid(df)
start = start_date.iloc[0]
        
end_date = pd.to_datetime(df_last_valid(df))
end = end_date.iloc[0]