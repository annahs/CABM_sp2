#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import mysql.connector
import math
import calendar
import os.path
import mysql.connector
from scipy.optimize import curve_fit
import inspect

"""
this module is a collection of widely used generic methods specific to the CABM sites
"""

def getLocationID(location_name):
	instr_location_IDs = {'Alert':1,'East Trout Lake':2,'Egbert':3,'Resolute':4,'Whistler':5}
 	instr_location_ID = instr_location_IDs[location_name]
 	return instr_location_ID
 	
def getInstrID(instr_number):
	instr_IDs = {17:1,44:2,58:3}
 	instr_ID = instr_IDs[instr_number]
 	return instr_ID


def retrieveCalibrationIDs(instr_location_ID,instr_ID,date,cursor):
	#Only a single calibration is available for WHI and Egbert, done in 2010, so we must use this even for 2009 values
	if instr_location_ID in [3,5]:
		date = 1267747200 

	#many of the CABM calibrations were done in the lab, so we can't select them using the location ID but will simply use the most recent one.
	calib_id_list = []
	for channel in ['BBHG_incand','BBLG_incand']:
		cursor.execute('''
		SELECT 
			id	
		FROM
			sp2_calibrations
		WHERE
			instr_ID = %s
			AND calibrated_channel = %s
			AND calibration_date <= %s
			ORDER BY calibration_date DESC LIMIT 1
		''',
		(instr_ID,channel,date))
		calib_id = cursor.fetchall()
		try:
			calib_id_list.append(calib_id[0][0])
		except:
			continue

	return calib_id_list


def retrieveCalibrationID(instr_ID,date,channel,cursor):
	cursor.execute('''
	SELECT 
		id	
	FROM
		sp2_calibrations
	WHERE
		instr_ID = %s
		AND calibrated_channel = %s
		AND calibration_date = %s
	''',
	(instr_ID,channel,date))

	calib_id = cursor.fetchall()

	return calib_id[0][0]
