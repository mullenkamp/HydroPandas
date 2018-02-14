# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 18:29:20 2018

@author: MichaelEK
"""

import sqlite3

try:
    with sqlite3.connect(":memory:") as conn:
        conn.enable_load_extension(True)
        conn.load_extension("mod_spatialite")
    print('success')

except Exception as err:
    print(err)

