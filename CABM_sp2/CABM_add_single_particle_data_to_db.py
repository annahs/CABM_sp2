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


#set arguments
parser = argparse.ArgumentParser(description='''
	Reads raw .sp2b files and enters the incandescent particle data into the database 
	The first day and last day of raw files to be analyzed must be specified. The instrument location, instrument number, and the fll path to the directory containing the daily raw data folders musr also be given.
	Use the optional -s parameter if all of the .sp2b files are in a single folder (i.e. not seaprated by day).
	''')
parser.add_argument('analysis_start', help='Date to start on - format YYYY-MM-DD ',type=SP2_utilities.valid_date)
parser.add_argument('analysis_end', help='Date to finish on - format YYYY-MM-DD ',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('raw_data_path', help='full path for directory containing the daily raw data folders or full path for directory conatining all files (use if all .sp2b files are in a single folder)',type=str)
parser.add_argument('-s','--single_folder', help='use if all of the .sp2b files are in a single folder (i.e. not seaprated by day)', action='store_true')
args = parser.parse_args()


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


#setup
instr_location_ID 		= CABM_utilities.getLocationID(args.location)
instr_ID 				= CABM_utilities.getInstrID(args.instr_number)
start_analysis_at 		= args.analysis_start
end_analysis_at 		= args.analysis_end
data_dir 				= args.raw_data_path
instr_owner 			= 'ECCC'


##create parameters dictionary
instr_id,number_of_channels,acquisition_rate,bytes_per_record,min_detectable_signal = SP2_utilities.getInstrInfo(instr_owner,instr_ID,cursor)

parameters = {
'instr_id'				:instr_id,
'instr_locn_ID'			:instr_location_ID,
'number_of_channels'	:number_of_channels,
'acq_rate'				:acquisition_rate,
'bytes_per_record'		:bytes_per_record,
'min_detectable_signal'	:min_detectable_signal,
}

##Note: there was a change in the byte rate of SP2 #17 (from 2458 to 1498) when it was installed at East Trout Lake in 2013.  This change must be applied to all data from SP2 #17 at ETL and Resolute
#location ids of ETL and Resolute are 2 and 4.  This snippet corrects the byte rate.
if parameters['instr_locn_ID'] in [2,4] and parameters['instr_id'] == 17:
	parameters['bytes_per_record'] = 1498


#create db insert statement
table_name = 'sp2_single_particle_data_locn'+ str(instr_location_ID)
insert_statement = SP2_raw_data.defineIncandInsertStatement(parameters['number_of_channels'],table_name)

#script
count = 0
prev_particle_ts = 0
os.chdir(data_dir)

if args.single_folder:
	parameters['directory']=os.getcwd()
	for file in os.listdir('.'):
		if file.endswith('.sp2b') and (file.endswith('gnd.sp2b')==False):
			print file
			parameters['file_name'] = file
			path = parameters['directory'] + '/' + str(file)
			file_bytes = os.path.getsize(path) #size of entire file in bytes
			parameters['number_of_records']= (file_bytes/parameters['bytes_per_record'])
			if parameters['number_of_records'] == 0:
				continue
			with open(file, 'rb') as sp2b_file:
				prev_particle_ts,count = SP2_raw_data.writeIncandParticleData(sp2b_file,parameters,prev_particle_ts,count,insert_statement,cnx,cursor)
	
	print count


else:
	for directory in os.listdir(data_dir):
		if os.path.isdir(directory) == True and directory.startswith('20'):
			parameters['folder']= directory
			folder_date = datetime.strptime(directory, '%Y%m%d')
			if folder_date >= start_analysis_at and folder_date < end_analysis_at:
				parameters['directory']= os.getcwd() +  '/' + parameters['folder']
				os.chdir(parameters['directory'])
				for file in os.listdir('.'):
					if file.endswith('.sp2b') and (file.endswith('gnd.sp2b')==False):
						print file
						parameters['file_name'] = file
						path = parameters['directory'] + '/' + str(file)
						file_bytes = os.path.getsize(path) #size of entire file in bytes
						parameters['number_of_records']= (file_bytes/parameters['bytes_per_record'])
						if parameters['number_of_records'] == 0:
							continue
						
						#if folder_date < datetime(2015,11,17) and float(file[9:12]) < 3	:
						#	continue

						with open(file, 'rb') as sp2b_file:						
							prev_particle_ts,count = SP2_raw_data.writeIncandParticleData(sp2b_file,parameters,prev_particle_ts,count,insert_statement,cnx,cursor)
				os.chdir(data_dir)
	print count




	
