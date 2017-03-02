# -*- coding: UTF-8 -*-
#this script is used to add individual particle incandescence information to the database

import sys
import os
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
from struct import *
import math
import mysql.connector
from datetime import datetime
import calendar
import SP2_utilities
from CABM_SP2_time_interval import CABMTimeInterval



#locations
Alert = 1
ETL = 2
Egbert = 3
Resolute = 4
Whistler = 5
#instruments
sp2_17 = 1
sp2_44 = 2
sp2_58 = 3

#setup
instr_ID 				= sp2_17
instr_location_ID 		= ETL
start_analysis 			= datetime(2013,5,9)
end_analysis	 		= datetime(2014,4,28)
interval_length			= 60					#in seconds
database_name 			= 'CABM_SP2'
interval_table_name		= 'sp2_interval_data_locn'+str(instr_location_ID)
t0 = datetime.now()

i = 0
multiple_records = []
interval_start 	= calendar.timegm(start_analysis.utctimetuple())
while interval_start <= calendar.timegm(end_analysis.utctimetuple()):
	
	interval_end = interval_start + interval_length
	#instantiate a time interval object
	time_interval = CABMTimeInterval(database_name,instr_location_ID, instr_ID, interval_start, interval_end)
	#create a database insert statement 
	interval_insert_statement = time_interval.createIntervalInsertStatement(interval_table_name)
	#retreive single particle data for the interval
	time_interval.retrieveSingleParticleData()
	#print progress reports
	if interval_start % 3600 == 0:
 		print datetime.utcfromtimestamp(interval_start)
		print len(time_interval.single_particle_data)
	#only write intervals with data to the database
	if time_interval.single_particle_data !=[]:
		#assemble the individual records into interval data
		time_interval.assembleIntervalData()
		#generate a record for database insertions
		interval_record = time_interval.createIntervalRecord()
		#append the record to the records list
		multiple_records.append(interval_record)
		i+= 1

	#bulk insert to db table
	if i%1440 == 0:
		time_interval.db_cur.executemany(interval_insert_statement, multiple_records)
		time_interval.db_connection.commit()
		multiple_records = []
		print datetime.now() - t0
		t0 = datetime.now()


	interval_start += interval_length
	
#insert any remaining records to database
if multiple_records != []:
	time_interval.db_cur.executemany(interval_insert_statement, multiple_records)
	time_interval.db_connection.commit()
	multiple_records = []

