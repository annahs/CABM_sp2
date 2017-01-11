import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import mysql.connector
import math
import calendar
import SP2_calibration


database_name = 'CABM_SP2'

#database connection
cnx = mysql.connector.connect(user='root', password='', host='localhost', database=database_name)
cursor = cnx.cursor()

#cursor.execute('''
#	SELECT 
#		id
#	FROM
#		sp2_calibrations  
#	''')
#
#calibration_ids = cursor.fetchall()
#print calibration_ids
#
#
#for id_tuple in calibration_ids:
#	calibration_ID = id_tuple[0]
#	calib_points = SP2_calibration.getCalibrationPoints(database_name, calibration_ID)
#	popt,perr,r_squared =  SP2_calibration.fitLinear(calib_points)
#	#popt,perr, r_squared =  SP2_calibration.fitQuadratic(calib_points)
#	print popt,perr, r_squared
#	
#	SP2_calibration.plotCalib(calib_points,popt)
#
#	SP2_calibration.writeFitToDatabase(database_name,calibration_ID,popt,perr)


calibration_ID = 1
calib_points = SP2_calibration.getCalibrationPoints(database_name, calibration_ID)
popt,perr,r_squared =  SP2_calibration.fitLinear(calib_points)
#popt,perr, r_squared =  SP2_calibration.fitQuadratic(calib_points)
print popt,perr, r_squared

SP2_calibration.plotCalib(calib_points,popt)

SP2_calibration.writeFitToDatabase(database_name,calibration_ID,popt,perr)


