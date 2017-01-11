import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import mysql.connector
import pickle
import math
import calendar
import os.path
import SP2_housekeeping
from mysql_db_connection import dbConnection


Alert = 1
ETL = 2
Egbert = 3
Resolute = 4
Whistler = 5

instrument_locn = ETL

parameters = {
'instr_owner' 				: 'ECCC',
'instr_number'				: 17,
'timezone'					: 0,	#check notes for this field for SP2#17. (note: PST = - 8)	
'instrument_locn' 			: instrument_locn,
'files_in_single_folder'	: False,
'hk_interval' 			  	: 1,    #seconds

'seconds_past_midnight_col' : 0,
'sample_flow_col'  		  	: 2,
'yag_power_col'    			: 3,
'sheath_flow_col'  			: 6,
'yag_xtal_temp_col'			: 7,

'data_dir' 					: '/Volumes/"LaCie"/ETL/', 
'hk_table'					: 'sp2_hk_data_locn' + str(instrument_locn),
'database'					: 'CABM_SP2',
}

#create db connection and cursor
database_connection = dbConnection(database_name)
cnx = database_connection.db_connection
cursor = database_connection.db_cur

#create db insert statement
add_interval = SP2_housekeeping.defineHKInsertStatement(parameters['hk_table'])

##add instr_ID to dictionary
parameters['instr_ID'] = SP2_utilities.getInstrID(parameters['instr_owner'],parameters['instr_number'],parameters['database'])

##script
os.chdir(parameters['data_dir'])
last_ts = np.nan

if parameters['files_in_single_folder'] == True:
	for file in os.listdir('.'):
		if file.endswith('.hk'):
			parameters['file_date'] = datetime.strptime(file[0:8], '%Y%m%d')
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
					parameters['file_date'] = datetime.strptime(file[0:8], '%Y%m%d')
					print file, parameters['file_date']

					with open(folder_path + '/' + file, 'r') as hk_file:
						multiple_records, last_ts = HKfileToDatabase(hk_file,add_interval,parameters,last_ts,cnx,cursor)
									
					#bulk insert of remaining records to db
					if multiple_records != []:
						cursor.executemany(add_interval, multiple_records)
						cnx.commit()
					
			os.chdir(parameters['data_dir'])
	
cnx.close()
	


