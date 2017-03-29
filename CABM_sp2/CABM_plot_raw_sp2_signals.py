#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
from struct import *
import math
from datetime import datetime
import calendar
from mysql_db_connection import dbConnection
import argparse
import SP2_raw_data
import SP2_utilities
import CABM_utilities
from SP2_particle_record import ParticleRecord


#set arguments
parser = argparse.ArgumentParser(description='''
	Reads raw .sp2b files and displays the raw signals 
	''')
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('raw_data_path', help='full path for directory containing .sp2b file',type=str)
parser.add_argument('record_number', help='particle number to view',type=int)
args = parser.parse_args()

#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur

#setup
instr_location_ID 		= CABM_utilities.getLocationID(args.location)
instr_ID 				= CABM_utilities.getInstrID(args.instr_number)
instr_owner 			= 'ECCC'
database_name 			= 'CABM_SP2'


##create parameters dictionary
instr_id,number_of_channels,acquisition_rate,bytes_per_record,min_detectable_signal = SP2_utilities.getInstrInfo(instr_owner,instr_ID,database_name,cursor)

parameters = {
'instr_id':instr_ID,
'instr_locn_ID':instr_location_ID,
'number_of_channels':number_of_channels,
'acq_rate': acquisition_rate,
'bytes_per_record':bytes_per_record,
'min_detectable_signal':min_detectable_signal,
}

##Note: there was a change in the byte rate of SP2 #17 (from 2458 to 1498) when it was installed at East Trout Lake in 2013.  This change must be applied to all data from SP2 #17 at ETL and Resolute
#location ids of ETL and Resolute are 2 and 4.  This snippet corrects the byte rate.
if parameters['instr_locn_ID'] in [2,4] and parameters['instr_id'] == 17:
	parameters['bytes_per_record'] = 1498


with open(args.raw_data_path, 'rb') as sp2b_file:
	sp2b_file.seek(parameters['bytes_per_record']*args.record_number)
	record = sp2b_file.read(parameters['bytes_per_record'])	
	particle_record = ParticleRecord(record, parameters['acq_rate'])
	SP2_raw_data.make_plot(particle_record)
