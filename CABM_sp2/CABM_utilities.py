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
	instr_location_IDs = {'Alert':1,'ETL':2,'Egbert':3,'Resolute':4,'Whistler':5}
 	instr_location_ID = instr_location_IDs[location_name]
 	return instr_location_ID
 	
def getInstrID(instr_number):
	instr_IDs = {17:1,44:2,58:3}
 	instr_ID = instr_IDs[instr_number]
 	return instr_ID


def retrieveCalibrationIDs(instr_location_ID,instr_ID,date,cursor):
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


def retrieveCalibrationDate(calibration_id,cursor):
	cursor.execute('''
	SELECT 
		calibration_date	
	FROM
		sp2_calibrations
	WHERE
		id = %s
		AND id > %s
	''',
	(calibration_id,0))

	calib_date_list = cursor.fetchall()

	return calib_date_list[0][0]

