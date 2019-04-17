# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 15:59:41 2019

@author: michaelek
"""

############################################
### NIWA SOS

import pandas as pd
from owslib.sos import SensorObservationService as sos
from owslib.ows import OperationsMetadata
from owslib.fes import FilterCapabilities
from owslib.swe.observation.sos200 import SOSGetObservationResponse
from owslib.swe.sensor.sml import SensorML
from owslib.etree import etree

pd.options.display.max_columns = 10


service = sos('https://hydro-sos.niwa.co.nz/?service=SOS', version='2.0.0')

for content in sorted(service.contents):
    print(content)

id1 = service.identification

get_foi=service.get_operation_by_name('GetFeatureOfInterest')

get_obs=service.get_operation_by_name('GetObservation')
get_obs.parameters['responseFormat']['values']

getcap=service.get_operation_by_name('GetCapabilities')
isinstance(getcap, OperationsMetadata)

getcap.constraints
getcap.parameters
getcap.methods

contents = service.contents

station = contents['HG.Master@15341']

get_obs=service.get_operation_by_name('GetObservation')
get_obs.methods[0].update({'url': service.url})

response = service.get_observation(responseFormat=station.response_formats[-1], offerings=['HG.Master@15341'], observedProperties=station.observed_properties, procedure=station.procedures[0], eventTime='phenomenonTime,2014-01-01/2014-02-01')

xml_tree = etree.fromstring(response)
parsed_response = SOSGetObservationResponse(xml_tree)

o1 = parsed_response.observations[0]

r1 = o1.get_result()





getdesc=service.get_operation_by_name('describesensor')
getdesc.parameters

procedure = 'http://www.opengis.net/sensorML/1.0.1'
of = 'http://www.opengis.net/sensorML/1.0.1'

ds1 = service.describe_sensor(outputFormat=of, procedure=station.procedures[0])

getdesc.methods[0].update({'url': service.url})

xml_tree = etree.fromstring(ds1)

parsed_response = SOSGetObservationResponse(xml_tree)

root = SensorML(xml_tree)

t1 = xml_tree.findall('{http://www.opengis.net/sensorML/1.0.1}capabilities')


































