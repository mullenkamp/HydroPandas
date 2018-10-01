# -*- coding: utf-8 -*-
"""
Created on Mon Feb 12 18:29:20 2018

@author: MichaelEK
"""

###############################
### GeoAlchemy2

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

print('success')
