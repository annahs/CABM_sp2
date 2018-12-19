#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import math
import calendar
import os.path
import dateutil
import argparse
import SP2_housekeeping
from mysql_db_connection import dbConnection
import SP2_utilities
import CABM_utilities

#set arguments
parser = argparse.ArgumentParser(description='''
	Reads housekeeping files and enters houskeeping data into the database.
	It is necessary to know what column is used in the housekeeping files for each parameter. 
	''')
parser.add_argument('analysis_start', 		help='Date to start on - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('analysis_end', 		help='Date to finish on - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('location', 			help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', 		help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('raw_data_path', 		help='full path for directory containing the daily raw data folders or full path for directory conatining all files (use if all .sp2b files are in a single folder)',type=str)
parser.add_argument('seconds_col', 			help='Column for seconds past midnight',type=int)
parser.add_argument('sample_flow_col', 		help='Column for sample flow',type=int)
parser.add_argument('yag_power_col', 		help='Column for yag power',type=int)
parser.add_argument('sheath_flow_col', 		help='Column for sheath flow',type=int)
parser.add_argument('xtal_temp_col', 		help='Column for yag crystal temperature',type=int)
parser.add_argument('-s','--single_folder', help='use if all of the .sp2b files are in a single folder (i.e. not seaprated by day)', action='store_true')
parser.add_argument('-lt','--local_time', 	help='use if the file timestamps are not in UTC',default=0, type= int)
parser.add_argument('-ti','--record_interval', help='use if housekeeping record interval is not 1 second',default=1, type= int)
args = parser.parse_args()


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


#setup
instr_location_ID 		= CABM_utilities.getLocationID(args.location)
instr_ID 				= CABM_utilities.getInstrID(args.instr_number)

parameters = {
'start_analysis_at'			: args.analysis_start,
'end_analysis_at'			: args.analysis_end,
'instr_ID'					: instr_ID,
'instr_location_ID' 		: instr_location_ID,
'timezone'					: args.local_time,
'hk_interval' 			  	: args.record_interval,    			
'seconds_past_midnight_col' : args.seconds_col,
'sample_flow_col'  		  	: args.sample_flow_col,
'yag_power_col'    			: args.yag_power_col,
'sheath_flow_col'  			: args.sheath_flow_col,
'yag_xtal_temp_col'			: args.xtal_temp_col,
'data_dir' 					: args.raw_data_path, 
'hk_table'					: 'sp2_hk_data_locn' + str(instr_location_ID),
}

#create db insert statement
add_interval = SP2_housekeeping.defineHKInsertStatement(parameters['hk_table'])

##script
os.chdir(parameters['data_dir'])
last_ts = np.nan

multiple_records = []
if args.single_folder:
	for file in os.listdir('.'):
		if file.endswith('.hk'):
			parameters['file_date'] = dateutil.parser.parse(file[0:8])
			if parameters['file_date'] >= parameters['start_analysis_at'] and parameters['file_date'] < parameters['end_analysis_at']:
				print file, parameters['file_date']

				with open(file, 'r') as hk_file:
					multiple_records, last_ts = SP2_housekeeping.HKfileToDatabase(hk_file,add_interval,parameters,last_ts,cnx,cursor)

			#bulk insert of remaining records to db
			if multiple_records != []:
				cursor.executemany(add_interval, multiple_records)
				cnx.commit()


else:
	for directory in os.listdir('.'):
		if os.path.isdir(directory) == True and directory.startswith('20'):
			folder_path = os.path.join(parameters['data_dir'], directory)	
			os.chdir(folder_path)
			for file in os.listdir('.'):

				if file.endswith('.hk'):

					if file == '20131201161826.hk':
						continue

					parameters['file_date'] = dateutil.parser.parse(file[0:8])
					if parameters['file_date'] >= parameters['start_analysis_at'] and parameters['file_date'] < parameters['end_analysis_at']:

						print file, parameters['file_date']

						with open(folder_path + '/' + file, 'r') as hk_file:
							multiple_records, last_ts = SP2_housekeeping.HKfileToDatabase(hk_file,add_interval,parameters,last_ts,cnx,cursor)
									
					#bulk insert of remaining records to db
					if multiple_records != []:
						cursor.executemany(add_interval, multiple_records)
						cnx.commit()
					
			os.chdir(parameters['data_dir'])
	
	


