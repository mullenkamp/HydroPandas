"""
Author: matth
Date Created: 28/02/2017 11:41 AM
"""

from __future__ import division

from Well_base import WellBase
from load_data import *


class WellQuant(WellBase):

    def load_quant_data(self,frequency='M', df=None):
        # load quant data from wells and eventually from telemetered data
        if frequency == 'M': # monthly averages
            self.quant_data = load_monthly(df=df)
        elif frequency == 'Y': # yearly averages
            self.quant_data = load_yearly(df=df)
        elif frequency == 'D': # individual readings
            self.quant_data = load_daily(df=df)
        elif frequency == 'T': # telemetered data
            self.quant_data = load_telemetered(df=df)
        elif frequency == 'B': # highest avalible
            self.quant_data = load_best_frequency(df=df)
        else:
            raise ValueError('unexpected frequency code')



    raise ValueError('not implemented')