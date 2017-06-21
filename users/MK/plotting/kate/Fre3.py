# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


def fre_accrual(csv_path, min_filter=True, min_yrs=4, max_missing=120, fre_mult=3, w_month = 'JUN'):

    from pandas import to_datetime, DateOffset
    from numpy import where, nan
    from misc_fun import df_first_valid, df_last_valid
    from import_hydstra_fun import rd_hydstra_csv

    df = rd_hydstra_csv(csv_path, min_filter, min_yrs)
    df = df.dropna(axis=1, how='all')
    sites = df.columns.values
    howmany = len(sites)

    medians = df.median()
    fre_val =  medians*fre_mult

    for i in range(howmany):
        site = sites[i]
        threshold = fre_val[site]
        dfsite = df[[site]]

        #start_date = df_first_valid(dfsite)
        #start = start_date.iloc[0]
        start = to_datetime('2006-08-31')

        end_date = df_last_valid(dfsite)
        end = end_date.iloc[0]

        find_events = dfsite[start: end]

        find_events['Exceed'] = where(find_events[site]>=threshold, 1, 0)  #find days where threshold is exceeded
        find_events['DaysOver'] = find_events['Exceed'].rolling(window=6).sum() #on how many days of the last 6 has the threshold been exceeded

        find_events['DaysOver'] = where((find_events['Exceed'] == 1),
            find_events['DaysOver'].fillna(1),find_events['DaysOver'].fillna(0))    #fill NaN values in DaysOver with 1 if flow over threshold, 0 if not

        find_events['Event'] = where((find_events['Exceed'] == 1) & (find_events['DaysOver'] == 1), 1, 0) #mark as a new event if the threshold is exceeded, and the 5 previous days were below the threshold

        #find the number of days in the year that are in the period of record, make sure it is at least 335
        n_days_yr = find_events[site].fillna(0).resample('A-' + w_month).count()
        day_filter = n_days_yr >= 335
        n_days_yr2 = n_days_yr[day_filter]

        events_year = find_events['Event'].resample('A-' + w_month).sum()[day_filter] #find number of events that occurred each year

        data_days = find_events[site].resample('A-' + w_month).count()[day_filter] #find number of days with data
        missing_days = n_days_yr2 - data_days #find number of missing days per year
        events_year[missing_days > max_missing] = nan   # remove years with more than the maximum alllowance number of missing days
        n_years = events_year.count()  #find the length of the record

        #Fre calc
        fre_calc = events_year.mean()

        #calculate accrual period
        find_accrual = find_events.loc[find_events['Exceed'] == 1]
        find_accrual['start_accrual'] = find_accrual.index
        find_accrual['end_accrual'] = find_accrual['start_accrual'].shift(periods=-1) + DateOffset(-1)
        find_accrual['accrual_time'] = abs(find_accrual['start_accrual'] - find_accrual['end_accrual'])
        accrual_periods = find_accrual.loc[find_accrual['accrual_time'] > '5 days']

        mean_accrual = accrual_periods['accrual_time'].astype('timedelta64[D]').astype('int').mean()
        print(site, fre_val[site], fre_calc, mean_accrual, n_years)

    return()


def fre_accrual_split(csv_path, min_filter=True, min_yrs=4, max_missing=120, fre_mult=3, w_month = 'JUN', gap_start=gapstart, gap_end=gapend):#, gap_start2=gapstart2, gap_end2=gapend2):#, gap_start3=gapstart3, gap_end3=gapend3):

    from pandas import to_datetime, DateOffset, concat
    from numpy import where, nan
    from misc_fun import df_first_valid, df_last_valid
    from import_hydstra_fun import rd_hydstra_csv

    df = rd_hydstra_csv(csv_path, min_filter, min_yrs)

    sites = df.columns.values

    medians = df.median()
    fre_val =  medians*fre_mult

    site = sites[0]
    threshold = fre_val[site]

    start_date = df_first_valid(df)
    start = start_date.iloc[0]

    end_date = df_last_valid(df)
    end = end_date.iloc[0]

    find_events = df[start: end]

    find_events['Exceed'] = where(find_events[site]>=threshold, 1, 0)  #find days where threshold is exceeded
    find_events['DaysOver'] = find_events['Exceed'].rolling(window=6).sum() #on how many days of the last 6 has the threshold been exceeded

    find_events['DaysOver'] = where((find_events['Exceed'] == 1),
    find_events['DaysOver'].fillna(1),find_events['DaysOver'].fillna(0))    #fill NaN values in DaysOver with 1 if flow over threshold, 0 if not

    find_events['Event'] = where((find_events['Exceed'] == 1) & (find_events['DaysOver'] == 1), 1, 0) #mark as a new event if the threshold is exceeded, and the 5 previous days were below the threshold

    #find the number of days in the year that are in the period of record, make sure it is at least 335
    n_days_yr = find_events[site].fillna(0).resample('A-' + w_month).count()
    day_filter = n_days_yr >= 335
    n_days_yr2 = n_days_yr[day_filter]

    events_year = find_events['Event'].resample('A-' + w_month).sum()[day_filter] #find number of events that occurred each year

    data_days = find_events[site].resample('A-' + w_month).count()[day_filter] #find number of days with data
    missing_days = n_days_yr2 - data_days #find number of missing days per year
    events_year[missing_days > max_missing] = nan   # remove years with more than the maximum alllowance number of missing days
    n_years = events_year.count()  #find the length of the record

    #Fre calc
    fre_calc = events_year.mean()

    #part1
    start1 = start
    end1 = to_datetime(gap_start)
    part1 = find_events[start1:end1]

    #calculate part1 accrual periods
    part1_accrual = part1.loc[part1['Exceed'] == 1]
    part1_accrual['start_accrual'] = part1_accrual.index
    part1_accrual['end_accrual'] = part1_accrual['start_accrual'].shift(periods=-1) + DateOffset(-1)
    part1_accrual['accrual_time'] = abs(part1_accrual['start_accrual'] - part1_accrual['end_accrual'])
    part1_acc_periods = part1_accrual.loc[part1_accrual['accrual_time'] > '5 days']

    #part2
    start2 = to_datetime(gap_end)
    end2 = end
    part2 = find_events[start2:end2]

    #calculate part1 accrual periods
    part2_accrual = part2.loc[part2['Exceed'] == 1]
    part2_accrual['start_accrual'] = part2_accrual.index
    part2_accrual['end_accrual'] = part2_accrual['start_accrual'].shift(periods=-1) + DateOffset(-1)
    part2_accrual['accrual_time'] = abs(part2_accrual['start_accrual'] - part2_accrual['end_accrual'])
    part2_acc_periods = part2_accrual.loc[part2_accrual['accrual_time'] > '5 days']

    #part3
    """
    start3 = to_datetime(gap_end2)
    end3 = end #to_datetime(gap_start3)
    part3 = find_events[start3:end3]

    #calculate part1 accrual periods
    part3_accrual = part3.loc[part3['Exceed'] == 1]
    part3_accrual['start_accrual'] = part3_accrual.index
    part3_accrual['end_accrual'] = part3_accrual['start_accrual'].shift(periods=-1) + DateOffset(-1)
    part3_accrual['accrual_time'] = abs(part3_accrual['start_accrual'] - part3_accrual['end_accrual'])
    part3_acc_periods = part3_accrual.loc[part3_accrual['accrual_time'] > '5 days']

    #part4
    start4 = to_datetime(gap_end3)
    end4 = end
    part4 = find_events[start4:end4]

    #calculate part1 accrual periods
    part4_accrual = part4.loc[part4['Exceed'] == 1]
    part4_accrual['start_accrual'] = part4_accrual.index
    part4_accrual['end_accrual'] = part4_accrual['start_accrual'].shift(periods=-1) + DateOffset(-1)
    part4_accrual['accrual_time'] = abs(part4_accrual['start_accrual'] - part4_accrual['end_accrual'])
    part4_acc_periods = part4_accrual.loc[part4_accrual['accrual_time'] > '5 days']
   """
    parts = (part1_acc_periods, part2_acc_periods)#, part3_acc_periods)#, part4_acc_periods)
    accrual_periods = concat(parts, axis=0)

    mean_accrual = accrual_periods['accrual_time'].astype('timedelta64[D]').astype('int').mean()
    print(site, fre_val[site], fre_calc, mean_accrual, n_years)

    return()
