# -*- coding: utf-8 -*-
"""
Created on Wed Jun 29 13:57:22 2016

@author: KateSt
"""


csv_path = 'S:/KateSt/OTOP/Allocation/Opuha River Allocation4.csv'
river = "Opuha River"

def alloc_plot(csv_path, river):
    
    import pandas as pd
    import numpy as np
    import seaborn as sns
    import matplotlib.pyplot as plt
    matplotlib.style.use('ggplot')
    from palettable.colorbrewer.qualitative import Set3_9

    Alloc = pd.read_csv(csv_path,index_col=(0), header=[0,1], skiprows=[2])
    Alloc = Alloc.fillna(0)
    Alloc = Alloc.rename(columns={'S&D no min flow': 'Stock and domestic, no minimum flow'})
    
    #Irrigation use and allocation band plot
    Alloc1 = Alloc.sum(axis=1, level=1)
    
    AllocPlot1 = Alloc1.plot.bar(stacked=True,colormap='Set3')
    AllocPlot1 = plt.xlabel('Year')
    AllocPlot1 = plt.ylabel('Allocation (L/s)')
    AllocPlot1 = plt.title(river+' allocation')
    AllocPlot2 = plt.legend(loc='upper left', title="Water use & allocation band")
    
    
    #Irrigation source plot
    Alloc2 = Alloc.sum(axis=1, level=0)
    #Alloc2 = Alloc2.apply(lambda r: r/r.sum(), axis=1)
    AllocPlot2 = Alloc2.plot.area(stacked=True)
    AllocPlot2 = plt.xlabel('Year')
    AllocPlot2 = plt.ylabel('Allocation (L/s)')
    AllocPlot2 = plt.title(river+' allocation')
    AllocPlot2 = plt.legend(["Groundwater","Surface water"], loc='upper left', title="Water source")

    return(AllocPlot1, AllocPlot2)

alloc_plot(csv_path, river)


