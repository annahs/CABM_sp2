import sys
import os
from datetime import datetime
from pprint import pprint
from datetime import timedelta
import calendar
import SP2_configuration
import SP2_utilities

Alert = 1
ETL = 2
Egbert = 3
Resolute = 4
Whistler = 5

instrument_locn = Alert


parameters = {
'instr_owner' 				: 'ECCC',
'instr_number'				: 58,
'timezone'					: 0,	#check notes for this field for SP2#17. (note: PST = UTC - 8)	
'instrument_locn' 			: instrument_locn,
'files_in_single_folder'	: False,
'UNIX_UTC_end_date'			: calendar.timegm((datetime(2016,3,16)).utctimetuple()),
'data_dir' 					: '/Volumes/"LaCie"/ALT/', 
'config_table'				: 'sp2_config_parameters',
'database'					: 'CABM_SP2',
}





	
def parseConfigFiles(prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time):
	for file in os.listdir('.'):	
		if file.endswith('.ini'):
			ini_date,sample_factor_particle,sample_factor_time = SP2_configuration.getConfigData(file)
			print ini_date,sample_factor_particle,sample_factor_time
			if ini_date != prev_ini_date:
				UNIX_UTC_start = calendar.timegm(prev_ini_date.utctimetuple()) 
				UNIX_UTC_end = calendar.timegm(ini_date.utctimetuple())

				SP2_configuration.writeConfigData(parameters,UNIX_UTC_start,UNIX_UTC_end,prev_sample_factor_particle,prev_sample_factor_time)
				prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time = ini_date,sample_factor_particle,sample_factor_time

	return prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time






####script
os.chdir(parameters['data_dir'])
prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time = datetime(1970,1,1),1,0
parameters['instr_ID'] = SP2_utilities.getInstrID(parameters['instr_owner'],parameters['instr_number'],parameters['database'])


if parameters['files_in_single_folder'] == True:
	prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time = parseConfigFiles(prev_ini_date,prev_sample_factor_particle,prev_sample_factor_time)
	UNIX_UTC_start = calendar.timegm(prev_ini_date.utctimetuple())
	SP2_configuration.writeConfigData(parameters,UNIX_UTC_start,parameters['UNIX_UTC_end_date'],prev_sample_factor_particle,prev_sample_factor_time)

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

	UNIX_UTC_start = calendar.timegm(prev_ini_date.utctimetuple())
	SP2_configuration.writeConfigData(parameters,UNIX_UTC_start,parameters['UNIX_UTC_end_date'],prev_sample_factor_particle,prev_sample_factor_time)


