# -*- coding: utf-8 -*-
"""
Functions for processing land use.
"""


def random_areas(poly, name_col, change_area, land_change, tol=0.05):
    """
    Function to randomly select parcels then randomly assign a land use type. See otop_land_use_irr1.py for an example.

    Arguments:\n
    poly -- Polygon GeoDataFrame of the parcels with a column associated with each study area.\n
    name_col -- The column in poly that contains the study are names.\n
    change_area -- A Series with the area to be assigned with the index as the study area names.\n
    land_change -- A DataFrame of the percentage of each land use type associated with each study area name.\n
    tol -- The tolerance ratio of the change_area when assigning parcels. The cumulative parcels sizes must not be >+- the tolerance ratio.
    """
    from pandas import Series, concat
    from numpy import random, nan, abs

    poly1 = poly.copy()
    poly1['area'] = poly.area
    grp = poly1.groupby(name_col)['area']

    sel1 = Series(name='land_use')
    for i in grp.groups.keys():
        change1 = change_area[i] * 10000
        if change1 == 0:
            continue
        set1 = grp.get_group(i)
        index1 = set1.index.tolist()
        prob1 = (land_change.loc[i, :]/100.0).sort_values()
        prob2 = prob1[prob1 > 0]
        while True:
            random.shuffle(index1)
            set2 = set1[index1].cumsum()
            if set2.values[-1] <= change1:
                raise ValueError('The change_area assigned is larger than the available area!')
            abs_diff = (set2 - change1).abs()/change1
            error = abs_diff.min()
            if error > tol:
                continue
            set3 = set2.loc[:abs_diff.idxmin()]
            prob3 = (prob2 * set3.iloc[-1]).cumsum()
            land_index = prob3.apply(lambda x: ((set3 - x).abs()/x).idxmin())
            error2 = abs(set3.loc[land_index].values - prob3.values)/prob3.values
            if all(error2 <= (tol*2)):
                break
        t_s = Series(nan, index=set3.index, name='land_use')
        land_index2 = land_index.iloc[::-1]
        for j in land_index2.index:
            t_s.loc[:land_index2[j]] = j

        sel1 = sel1.append(t_s)
    poly_new = concat([poly, sel1], axis=1, join='inner')
    return(poly_new)





