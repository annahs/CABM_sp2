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
import argparse
import SP2_utilities
import CABM_utilities
from CABM_SP2_time_interval import CABMTimeInterval

#set arguments
parser = argparse.ArgumentParser(description='''
	This script is used to add rBC mass and number information at specifc time intervals to the database.
	It also adds binned mass and number information to a separate table
	''')
parser.add_argument('start_time', help='beginning of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('end_time', help='end of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('-b','--bin_width', help='sets the bin width for binning by rBC core diameter, default is 10nm',default=10, type= int)
parser.add_argument('-mn','--min_VED', help='sets the minimum particle diameter', type= int)
parser.add_argument('-mx','--max_VED', help='sets the maximum particle diameter', type= int)
parser.add_argument('-i','--interval_length', help='sets the interval length in minutes, default is 1 minute',default=1, type= float)
parser.add_argument('-e','--extrapolate_calibration', help='if set the interval will include particles with masses outside the calibration range',action='store_true')
args = parser.parse_args()

#set inputs
start_analysis				= calendar.timegm(args.start_time.utctimetuple())
end_analysis				= calendar.timegm(args.end_time.utctimetuple())
instr_location_ID	 		= CABM_utilities.getLocationID(args.location)
instr_ID					= CABM_utilities.getInstrID(args.instr_number)
binning_increment			= args.bin_width 						
min_VED						= args.min_VED 						
max_VED						= args.max_VED 						
interval_length				= args.interval_length*60				
database_name 				= 'CABM_SP2'
intervals_table_name 		= 'sp2_time_intervals_locn'+str(instr_location_ID)
distrs_table_name 			= 'sp2_time_interval_binned_data_locn'+str(instr_location_ID)


i = 0
multiple_records = []
interval_start 	= start_analysis
while interval_start < end_analysis:
	interval_end = interval_start + interval_length
 	
 	#print the timestamp
 	if datetime.utcfromtimestamp(interval_start).minute%60 == 0:
 		print datetime.utcfromtimestamp(interval_start)
	
	#instantiate a time interval object
	time_interval = CABMTimeInterval(database_name,instr_location_ID, instr_ID, interval_start, interval_end)
	
	#set flag for extrapolation of calibration, default is to not extrapolate, but rather to ignore particles with masses outside the calibration range
	if args.extrapolate_calibration:
		time_interval.extrapolate_calibration = True
		time_interval.retrieveCalibrationData()

	#check for QC issues
 	if time_interval.checkForQCInterval() == True:
 		print 'QC Issues'
 		interval_start += interval_length
 		continue


	#set the binning limits based on the range of the calibration or on the user defined limits
	time_interval.setBinningLimits()
	if args.min_VED:
		time_interval.min_VED = min_VED
	if args.max_VED:
		time_interval.max_VED = max_VED
	

	#print 'min VED: ', time_interval.min_VED
	#print 'max VED: ', time_interval.max_VED


	#retreive single particle data for the interval
	time_interval.retrieveSingleParticleData()

	#only write intervals with data to the database
	if time_interval.single_particle_data !=[]:
		
		#assemble the individual particle data into an interval record 
		time_interval.assembleIntervalData()

		#very occasionally the volume calculated is negative, in these cases skip the interval
		if time_interval.assembled_interval_data['sampled volume'] > 0 :

			#assemble the binned particle data for the interval
			time_interval.binAssembledData(binning_increment)
			
			#create an insert statement for the interval 
			distr_interval_insert_statement = time_interval.createIntervalInsertStatement(intervals_table_name)
			if np.isnan(time_interval.LG_calibration_ID):
				time_interval.LG_calibration_ID = None

			#create an interval record to be inserted into the database
			distr_interval_record = time_interval.createIntervalRecord()

			#delete any pre-existing record for this interval
			time_interval.deleteExistingIntervalRecord(intervals_table_name)

			#insert the interval record into the database
			time_interval.insertSingleRecord(distr_interval_insert_statement,distr_interval_record)

			#insert the binned data into the database
			distr_insert_statement = time_interval.createBinnedDataInsertStatement(distrs_table_name)
			time_interval.insertBinnedData(distr_insert_statement)

	interval_start += interval_length



