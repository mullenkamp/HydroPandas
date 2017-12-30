# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 13:34:01 2017

@author: MichaelEK
Functions for river networks.
"""
from numpy import isnan, append
from pandas import DataFrame


def up_branch(df, index_col=1):
    """
    Function to create a dataframe of all the interconnected values looking upstream from specific locations.
    """

    col1 = df.columns[index_col-1]
    index1 = df[col1]
    df2 = df.drop(col1, axis=1)
    catch_set2 = []
    for i in index1:
        catch1 = df2[index1 == i].dropna(axis=1).values[0]
        catch_set1 = catch1
        check1 = index1.isin(catch1)
        while sum(check1) >= 1:
            if sum(check1) > len(catch1):
                print('Index numbering is wrong!')
            catch2 = df2[check1].values.flatten()
            catch3 = catch2[~isnan(catch2)]
            catch_set1 = append(catch_set1, catch3)
            check1 = index1.isin(catch3)
            catch1 = catch3
        catch_set2.append(catch_set1.tolist())

    output1 = DataFrame(catch_set2, index=index1)
    return(output1)