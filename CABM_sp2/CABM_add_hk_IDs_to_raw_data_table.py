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
import argparse
from mysql_db_connection import dbConnection
import SP2_housekeeping
import SP2_utilities
import CABM_utilities

#set arguments
parser = argparse.ArgumentParser(description='''
	Adds housekeeping IDS to the SP2 single_particle_data tables.
	''')
parser.add_argument('analysis_start', 		help='Date to start on - format YYYY-MM-DD ',type=SP2_utilities.valid_date)
parser.add_argument('analysis_end', 		help='Date to finish on - format YYYY-MM-DD ',type=SP2_utilities.valid_date)
parser.add_argument('location', 			help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
args = parser.parse_args()


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur

#setup
instr_location_ID 	= CABM_utilities.getLocationID(args.location)
instr_ID 			= CABM_utilities.getInstrID(args.instr_number)
start 				= args.analysis_start
end 				= args.analysis_end 

#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur

timestep = 5. #days
while start <= end:
	print start

	UNIX_start = calendar.timegm(start.utctimetuple())
	UNIX_end = UNIX_start + 86400*timestep

	parameters = {
	'UNIX_start'	:UNIX_start,
	'UNIX_end'		:UNIX_end,
	'hk_table'		:'sp2_hk_data_locn' + str(instr_location_ID),
	'raw_data_table':'sp2_single_particle_data_locn' + str(instr_location_ID),
	'instr_ID'		:instr_ID,
	}

	SP2_housekeeping.addHKKeysToRawDataTable(parameters,cnx,cursor)

	start += timedelta(days = timestep)