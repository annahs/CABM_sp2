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
import SP2_raw_data
import SP2_utilities
from mysql_db_connection import dbConnection


Alert = 1
ETL = 2
Egbert = 3
Resolute = 4
Whistler = 5

#setup

instr_owner 			= 'ECCC'
instr_number 			= 17
instrument_locn 		= ETL
files_in_single_folder	= False
start_analysis_at 		= datetime(2015,9,12)
end_analysis_at 		= datetime(2016,5,7)
database_name 			= 'CABM_SP2'
data_dir 				= '/Volumes/"LaCie"/ETL/'
show_plot 				= False


##create parameters dictionary
instr_id,number_of_channels,acquisition_rate,bytes_per_record,min_detectable_signal = SP2_utilities.getInstrInfo(instr_owner,instr_number,database_name)

parameters = {
'instr_id':instr_id,
'instr_locn_ID':instrument_locn,
'number_of_channels':number_of_channels,
'acq_rate': acquisition_rate,
'bytes_per_record':bytes_per_record,
'min_detectable_signal':min_detectable_signal,
'show_plot': show_plot,
}

##Note: there was a change in the byte rate of SP2 #17 (from 2458 to 1498) when it was installed at East Trout Lake in 2013.  This change must be applied to all data from SP2 #17 at ETL and Resolute
#location ids of ETL and Resolute are 2 and 4.  This snippet corrects the byte rate.
if parameters['instr_locn_ID'] in [2,4]:
	parameters['bytes_per_record'] = 1498

#create db connection and cursor
database_connection = dbConnection(database_name)
cnx = database_connection.db_connection
cursor = database_connection.db_cur

#create db insert statement
insert_statement = SP2_raw_data.defineIncandInsertStatement(parameters['number_of_channels'],instrument_locn)


##run script
count = 0
prev_particle_ts = 0
os.chdir(data_dir)

if files_in_single_folder == True:
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
						
						if folder_date < datetime(2015,9,13) and float(file[9:12]) < 21:
							continue

						with open(file, 'rb') as sp2b_file:						
							prev_particle_ts,count = SP2_raw_data.writeIncandParticleData(sp2b_file,parameters,prev_particle_ts,count,insert_statement,cnx,cursor)
				os.chdir(data_dir)
	print count




	
