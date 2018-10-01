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

###############################
### GeoAlchemy2

'https://geoalchemy-2.readthedocs.io/en/latest/spatialite_tutorial.html'

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.event import listen
from sqlalchemy.sql import select, func
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from sqlalchemy.orm import sessionmaker


def load_spatialite(dbapi_conn, connection_record):
    dbapi_conn.enable_load_extension(True)
    dbapi_conn.load_extension('mod_spatialite')


engine = create_engine('sqlite:///gis3.db', echo=True)
listen(engine, 'connect', load_spatialite)

conn = engine.connect()

conn.execute(select([func.InitSpatialMetaData()]))
conn.close()


Base = declarative_base()

class Lake(Base):
    __tablename__ = 'lake'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    geom = Column(Geometry(geometry_type='POLYGON', management=True, use_st_prefix=False))

Lake.__table__.create(engine)
#Lake.__table__.drop(engine)

Session = sessionmaker(bind=engine)
session = Session()

lake = Lake(name='Majeur', geom='POLYGON((0 0,1 0,1 1,0 1,0 0))')
session.add(lake)
session.commit()
#session.rollback()
#session.close()

our_lake = session.query(Lake).filter_by(name='Majeur').first()


#####################################
### Straight sqlite connection

conn = sqlite3.connect('gis2.db')

conn.enable_load_extension(True)
conn.load_extension("mod_spatialite")

# creating a Cursor
cur = conn.cursor()

# testing library versions
rs = cur.execute('SELECT sqlite_version(), spatialite_version()')
for row in rs:
    msg = "> SQLite v%s Spatialite v%s" % (row[0], row[1])
    print(msg)

# initializing Spatial MetaData
# using v.2.4.0 this will automatically create
# GEOMETRY_COLUMNS and SPATIAL_REF_SYS
sql = 'SELECT InitSpatialMetadata()'
cur.execute(sql)

# creating a POINT table
sql = 'CREATE TABLE test_pt ('
sql += 'id INTEGER NOT NULL PRIMARY KEY,'
sql += 'name TEXT NOT NULL)'
cur.execute(sql)
# creating a POINT Geometry column
sql = "SELECT AddGeometryColumn('test_pt', 'geom', 4326, 'POINT', 'XY')"
cur.execute(sql)

# creating a LINESTRING table
sql = 'CREATE TABLE test_ln ('
sql += 'id INTEGER NOT NULL PRIMARY KEY,'
sql += 'name TEXT NOT NULL)'
cur.execute(sql)
# creating a LINESTRING Geometry column
sql = "SELECT AddGeometryColumn('test_ln', "
sql += "'geom', 4326, 'LINESTRING', 'XY')"
cur.execute(sql)

# creating a POLYGON table
sql = 'CREATE TABLE test_pg ('
sql += 'id INTEGER NOT NULL PRIMARY KEY,'
sql += 'name TEXT NOT NULL)'
cur.execute(sql)
# creating a POLYGON Geometry column
sql = "SELECT AddGeometryColumn('test_pg', "
sql += "'geom', 4326, 'POLYGON', 'XY')"
cur.execute(sql)

# inserting some POINTs
# please note well: SQLite is ACID and Transactional
# so (to get best performance) the whole insert cycle
# will be handled as a single TRANSACTION
for i in range(100000):
    name = "test POINT #%d" % (i+1)
    geom = "GeomFromText('POINT("
    geom += "%f " % (i / 1000.0)
    geom += "%f" % (i / 1000.0)
    geom += ")', 4326)"
    sql = "INSERT INTO test_pt (id, name, geom) "
    sql += "VALUES (%d, '%s', %s)" % (i+1, name, geom)
    cur.execute(sql)
conn.commit()

# checking POINTs
sql = "SELECT DISTINCT Count(*), ST_GeometryType(geom), "
sql += "ST_Srid(geom) FROM test_pt"
rs = cur.execute(sql)
for row in rs:
    msg = "> Inserted %d entities of type " % (row[0])
    msg += "%s SRID=%d" % (row[1], row[2])
    print(msg)

# inserting some LINESTRINGs
for i in range(100000):
    name = "test LINESTRING #%d" % (i+1)
    geom = "GeomFromText('LINESTRING("
    if (i%2) == 1:
    # odd row: five points
        geom += "-180.0 -90.0, "
        geom += "%f " % (-10.0 - (i / 1000.0))
        geom += "%f, " % (-10.0 - (i / 1000.0))
        geom += "%f " % (10.0 + (i / 1000.0))
        geom += "%f" % (10.0 + (i / 1000.0))
        geom += ", 180.0 90.0"
    else:
    # even row: two points
        geom += "%f " % (-10.0 - (i / 1000.0))
        geom += "%f, " % (-10.0 - (i / 1000.0))
        geom += "%f " % (10.0 + (i / 1000.0))
        geom += "%f" % (10.0 + (i / 1000.0))
    geom += ")', 4326)"
    sql = "INSERT INTO test_ln (id, name, geom) "
    sql += "VALUES (%d, '%s', %s)" % (i+1, name, geom)
    cur.execute(sql)
conn.commit()

# checking LINESTRINGs
sql = "SELECT DISTINCT Count(*), ST_GeometryType(geom), "
sql += "ST_Srid(geom) FROM test_ln"
rs = cur.execute(sql)
for row in rs:
    msg = "> Inserted %d entities of type " % (row[0])
    msg += "%s SRID=%d" % (row[1], row[2])
    print(msg)

# inserting some POLYGONs
for i in range(100000):
    name = "test POLYGON #%d" % (i+1)
    geom = "GeomFromText('POLYGON(("
    geom += "%f " % (-10.0 - (i / 1000.0))
    geom += "%f, " % (-10.0 - (i / 1000.0))
    geom += "%f " % (10.0 + (i / 1000.0))
    geom += "%f, " % (-10.0 - (i / 1000.0))
    geom += "%f " % (10.0 + (i / 1000.0))
    geom += "%f, " % (10.0 + (i / 1000.0))
    geom += "%f " % (-10.0 - (i / 1000.0))
    geom += "%f, " % (10.0 + (i / 1000.0))
    geom += "%f " % (-10.0 - (i / 1000.0))
    geom += "%f" % (-10.0 - (i / 1000.0))
    geom += "))', 4326)"
    sql = "INSERT INTO test_pg (id, name, geom) "
    sql += "VALUES (%d, '%s', %s)" % (i+1, name, geom)
    cur.execute(sql)
conn.commit()

# checking POLYGONs
sql = "SELECT DISTINCT Count(*), ST_GeometryType(geom), "
sql += "ST_Srid(geom) FROM test_pg"
rs = cur.execute(sql)
for row in rs:
    msg = "> Inserted %d entities of type " % (row[0])
    msg += "%s SRID=%d" % (row[1], row[2])
    print(msg)

rs.close()
conn.close()



































