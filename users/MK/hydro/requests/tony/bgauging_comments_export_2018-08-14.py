# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 09:27:42 2018

@author: michaelek
"""

from pdsql.mssql import rd_sql

server = 'SQL2012PROD05'
database = 'Bgauging'
table = 'Data'

export_csv = r'E:\ecan\local\Projects\requests\tony\2018-08-14\bgauging_comments_2018-08-14.csv'

comments = rd_sql(server, database, table)

comments2 = comments.loc[:, comments.columns != 'timestamp']

comments2.to_csv(export_csv, index=False)