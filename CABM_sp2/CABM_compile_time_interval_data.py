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
import SP2_plotting
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
instr_ID 					= sp2_17
instr_location_ID 			= ETL
start_analysis 				= datetime(2013,6,1)
end_analysis	 			= datetime(2013,6,2)
binning_increment			= 10 						#nm
interval_length				= 60					#in seconds
database_name 				= 'CABM_SP2'
intervals_table_name 		= 'sp2_interval_data_locn'+str(instr_location_ID)
distrs_table_name 			= 'sp2_distribution_data_locn'+str(instr_location_ID)


i = 0
multiple_records = []
interval_start 	= calendar.timegm(start_analysis.utctimetuple())
while interval_start < calendar.timegm(end_analysis.utctimetuple()):
	interval_end = interval_start + interval_length
 	
 	if datetime.utcfromtimestamp(interval_start).minute%60 == 0:
 		print datetime.utcfromtimestamp(interval_start)
	
	#instantiate a time interval object
	time_interval = CABMTimeInterval(database_name,instr_location_ID, instr_ID, interval_start, interval_end)
	
	#check for QC issues
 	if time_interval.checkForQCInterval() == True:
 		print 'QC Issues'
 		interval_start += interval_length
 		continue


	#set the binning limits based on the range of the calibration
	time_interval.setBinningLimits()
	#print 'min VED: ', time_interval.min_VED
	#print 'max VED: ', time_interval.max_VED
	
	#retreive single particle data for the interval
	time_interval.retrieveSingleParticleData()
	#print len(time_interval.single_particle_data)

	#only write intervals with data to the database
	if time_interval.single_particle_data !=[]:
		
		#assemble the individual records into a distribution interval
		time_interval.assembleIntervalData()
		
		#bin the assembled data
		time_interval.binAssembledData(binning_increment)
		
		#insert the distribution interval data to database 
		distr_interval_insert_statement = time_interval.createIntervalInsertStatement(intervals_table_name)
		if np.isnan(time_interval.LG_calibration_ID):
			time_interval.LG_calibration_ID = None
		distr_interval_record = time_interval.createIntervalRecord()
		time_interval.insertSingleRecord(distr_interval_insert_statement,distr_interval_record)

		#insert the distribution data to database
		distr_insert_statement = time_interval.createDistributionInsertStatement(distrs_table_name)
		time_interval.insertDistributionRecords(distr_insert_statement)

	interval_start += interval_length



