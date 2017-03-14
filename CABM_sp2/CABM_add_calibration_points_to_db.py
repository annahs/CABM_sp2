#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
import numpy as np
from pprint import pprint
import math
from mysql_db_connection import dbConnection
from datetime import datetime
import calendar
import dateutil
import SP2_calibration
import argparse
import SP2_utilities
import CABM_utilities


#set arguments
parser = argparse.ArgumentParser(description='''
	Reads calibration data points from file and enters them into the database. 
	The full file path must be given as well as the calibration date, instrument, and calibrated channel.
	The calibration points file should be tab-delimited and have a single line header. Rows below the header should have mobility diameter followed by the corresponding signal:
	''')
parser.add_argument('calibration_date', help='Date of calibration - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('calibrated_channel', help='Channel that was calibrated. Options: BBHG_incand or BBLG_incand',type=str)
parser.add_argument('filepath', help='full path for file containing calibration data',type=str)
args = parser.parse_args()


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


#set inputs
instr_ID 			= CABM_utilities.getInstrID(args.instr_number)
UNIX_date			= calendar.timegm(args.calibration_date.utctimetuple())
calib_id 			= CABM_utilities.retrieveCalibrationID(instr_ID,UNIX_date,args.calibrated_channel,cursor)
calibration_data 	= []
with open(args.filepath) as f:
	f.readline()
	for line in f:
		newline = line.split()
		mobdia = float(newline[0])
		signal = float(newline[1])
		calibration_data.append([mobdia,signal])


#define database query
add_data = ('''INSERT INTO sp2_calibration_points							  
			  (calibration_ID,
			  mobility_diameter,
			  incand_pk_ht
			  )
			  VALUES (
			  %(calibration_ID)s,
			  %(mobility_diameter)s,
			  %(incand_pk_ht)s
			  )''')
					
#script
for row in calibration_data:
	mobility_diameter = row[0]
	incand_pk_ht = row[1]

	single_record ={
	'calibration_ID':calib_id,
	'mobility_diameter':mobility_diameter,
	'incand_pk_ht':incand_pk_ht 
	}

	cursor.execute(add_data, single_record)
	cnx.commit()

