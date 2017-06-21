"""
Author: matth
Date Created: 2/03/2017 10:13 AM
"""

from __future__ import division
import pandas as pd
import numpy as np
def calc_gaps(path, data):
    gap_id_out = None
    gap_length_out = None
    start_out = None
    end_out = None
    well_out = None
    for i, key in enumerate(data.keys()):
        gap_id, gap_length, start, end = get_gap_len_pos_monthly(data[key])
        well = np.zeros(gap_id.shape, dtype=object)
        well[:] = key

        if i == 0:
            gap_id_out = gap_id
            gap_length_out = gap_length
            start_out = start
            end_out = end
            well_out = well
        else:
            gap_id_out = np.concatenate((gap_id_out, gap_id))
            gap_length_out = np.concatenate((gap_length_out, gap_length))
            start_out = np.concatenate((start_out, start))
            end_out = np.concatenate((end_out, end))
            well_out = np.concatenate((well_out, well))

    gap_data = pd.DataFrame({'WELL_NO': well_out,
                             'gap_id': gap_id_out,
                             'gap_len': gap_length_out,
                             'time_1_start': start_out,
                             'time_2_end': end_out})

    gap_data.to_csv(path)

def get_gap_len_pos_monthly(series):
    """
    assumes a time index, assumes data in time order if no gaps returns same with arrays length 0
    :param series:
    :return:
    """

    gaps = series[pd.isnull(series)]
    if pd.isnull(series).sum() == 0:
        gap_id = np.array([0])
        gap_length = np.array([0])
        start = np.array([0])
        end = np.array([0])
        return gap_id, gap_length, start, end
    times = np.array(gaps.index)

    gap_num = np.zeros(gaps.shape)
    for i, val in enumerate(times):
        if i == 0:
            gap_num[i] = 0
            previous_val = val
            continue

        if (val - previous_val).astype('timedelta64[D]')/np.timedelta64(1, 'D') <= 50: # one month with some wiggle room...
            gap_num[i] = gap_num.max()
        else:
            gap_num[i] = gap_num.max()+1
        previous_val = val

    gm = int(gap_num.max()+1)
    start = np.zeros(gm,dtype=object)
    end = np.zeros(gm,dtype=object)
    gap_length = np.zeros(gm,dtype=object)
    gap_id = np.array(range(gm))
    for gap in range(gm):
        temp_times = times[gap_num == gap]
        start[gap] = temp_times.min()
        end[gap] = temp_times.max()
        gap_length[gap] = len(temp_times)

    return gap_id, gap_length, start, end

if __name__ == '__main__':
    test = pd.Series([1,2,3,np.nan,np.nan,1,2,3,np.nan,np.nan,np.nan,np.nan,2,np.nan],
                     index=pd.date_range('01/01/1990',periods=14,freq='MS'))
    print(test)
    output = get_gap_len_pos_monthly(test)
    for val in output:
        print(val)