import sys
import os
from datetime import datetime
from pprint import pprint
from datetime import timedelta
import calendar
from mysql_db_connection import dbConnection
import SP2_configuration
import argparse
import SP2_utilities
import CABM_utilities


#set arguments
parser = argparse.ArgumentParser(description='''
	Reads SP2 config files and records the particle sample factor and the time sample factor.
	particle sample factor = 1 out of how many detected particles was saved,
	time sample factor     = 1 out of how many minutes the instrument was recording data
	''')
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('analysis_start', help='Date for first config file in batch - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('analysis_end', help='Date for final config file in batch - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('config_data_path', help='full path for directory containing the daily data folders or full path for directory conatining all files (use if all of files are in a single folder)',type=str)
parser.add_argument('-s','--single_folder', help='use if all of the files are in a single folder (i.e. not separated by day)', action='store_true')
parser.add_argument('-lt','--local_time', help='use if the file timestamps are not in UTC',default=0, type= int)
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
data_dir 				= args.config_data_path


parameters = {
'instr_ID'			:instr_ID,
'instr_locn_ID'		:instr_location_ID,
'data_dir' 			:data_dir,
'timezone'			:args.local_time,								
'UNIX_UTC_end_date'	:calendar.timegm(end_analysis_at.utctimetuple()),
'config_table'		:'sp2_config_parameters',
}



#script functions
def parseConfigFiles(prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time):
	for file in os.listdir('.'):	
		if file.endswith('.ini'):
			ini_date,sample_factor_particle,sample_factor_time = SP2_configuration.getConfigData(file)
			print ini_date,sample_factor_particle,sample_factor_time
			if prev_ini_date != prev_ini_date:
				UNIX_UTC_start = calendar.timegm(prev_ini_date.utctimetuple()) 
				UNIX_UTC_end = calendar.timegm(ini_date.utctimetuple())

				SP2_configuration.writeConfigData(parameters,UNIX_UTC_start,UNIX_UTC_end,prev_sample_factor_particle,prev_sample_factor_time,cnx,cursor)
				prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time = ini_date,sample_factor_particle,sample_factor_time

	return prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time






####script
os.chdir(parameters['data_dir'])
prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time = start_analysis_at,1,0


if args.single_folder:
	prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time = parseConfigFiles(prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time)
	
	#take care of final interval
	UNIX_UTC_start = calendar.timegm(prev_ini_date.utctimetuple())
	SP2_configuration.writeConfigData(parameters,UNIX_UTC_start,parameters['UNIX_UTC_end_date'],prev_sample_factor_particle,prev_sample_factor_time,cnx,cursor)

else:
	for directory in os.listdir(parameters['data_dir']):
		if os.path.isdir(directory) == True and directory.startswith('20'):
			folder_date = datetime.strptime(directory, '%Y%m%d')
			#if folder_date < datetime(2015,3,1) or folder_date > datetime(2016,2,21):
			#	continue
			print folder_date
			folder_path = os.path.join(parameters['data_dir'], directory)
			os.chdir(folder_path)
			
			prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time = parseConfigFiles(prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time)
		
		os.chdir(parameters['data_dir'])

	#take care of final interval
	UNIX_UTC_start = calendar.timegm(prev_ini_date.utctimetuple())
	SP2_configuration.writeConfigData(parameters,UNIX_UTC_start,parameters['UNIX_UTC_end_date'],prev_sample_factor_particle,prev_sample_factor_time,cnx,cursor)


