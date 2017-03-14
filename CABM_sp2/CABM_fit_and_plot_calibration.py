#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
import calendar
import matplotlib.pyplot as plt
import dateutil
import SP2_calibration
import SP2_utilities
import CABM_utilities
from mysql_db_connection import dbConnection
import argparse


#set arguments
parser = argparse.ArgumentParser(description='''
	Plots the calibration points for both high and low gain incandescence channels for a given instrument, location, and date.  
	The date can be any date during which the instrument was in use at that location and the appropriate calibration will be plotted.
	A fit is also plotted and can be specified as a linear or quadratic function.  
	An optional argument allows the database to be updated with this fit, so that it is used in future analysis
	''')
parser.add_argument('date', help='Date of interest- format flexible',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('fit_type', help='linear or quadratic',type=str)
parser.add_argument('-u','--update_fit', help='update the database with this fit', action='store_true')
args = parser.parse_args()


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


#set inputs
instr_location_ID 	= CABM_utilities.getLocationID(args.location)
instr_ID 			= CABM_utilities.getInstrID(args.instr_number)
UNIX_date			= calendar.timegm(args.date.utctimetuple())
calib_ids 			= CABM_utilities.retrieveCalibrationIDs(instr_location_ID,instr_ID,UNIX_date,cursor)
calib_date 			= CABM_utilities.retrieveCalibrationDate(calib_ids[0],cursor)
fit_type 			= args.fit_type
print 'calib_ids', calib_ids

#do plotting
fig = plt.figure()
ax = fig.add_subplot(111)
i=0
for calibration_ID in calib_ids:
	calibration_pts = SP2_calibration.getCalibrationPoints(calibration_ID,cnx,cursor)
	incand_pk_hts, rBC_masses = SP2_calibration.getPeakHeightAndMassLists(calibration_pts)
	fit_rBC_masses = []
	if fit_type == 'linear':
		popt,perr = SP2_utilities.fitFunction(SP2_utilities.linear,incand_pk_hts, rBC_masses)
		incand_pk_hts, rBC_masses = SP2_calibration.getPeakHeightAndMassLists(calibration_pts)
		r2 = SP2_calibration.calcRSquared(incand_pk_hts, rBC_masses,popt,SP2_utilities.linear)
		for pk_ht in incand_pk_hts:
			fit_rBC_mass = SP2_utilities.linear(pk_ht,popt[0],popt[1])
			fit_rBC_masses.append(fit_rBC_mass)

	if fit_type == 'quadratic':
		popt,perr = SP2_utilities.fitFunction(SP2_utilities.quadratic,incand_pk_hts, rBC_masses)
		incand_pk_hts, rBC_masses = SP2_calibration.getPeakHeightAndMassLists(calibration_pts)
		r2 = np.nan
		for pk_ht in incand_pk_hts:
			fit_rBC_mass = SP2_utilities.quadratic(pk_ht,popt[0],popt[1],popt[2])
			fit_rBC_masses.append(fit_rBC_mass)

	if i == 0:
		plot_label = 'High Gain Channel, $R^2$ ' + str(round(r2,3))
		plot_color = 'r'
	else:
		plot_label = 'Low Gain Channel, $R^2$ ' + str(round(r2,3))
		plot_color = 'b'

	ax.scatter(incand_pk_hts,rBC_masses,color=plot_color)
	ax.plot(incand_pk_hts,fit_rBC_masses,color=plot_color, label = plot_label)

	#optional database update
	if args.update_fit:
		print 'updating database'
		SP2_calibration.writeFitToDatabase(calibration_ID,popt,perr,cnx,cursor)
	i+=1



ax.grid(True)
plt.legend(loc=4)
plt.xlabel('Incandescent peak height (a.u.)')
plt.ylabel('rBC mass (fg)')
plt.text(0.05, 1.025,'SP2#' + str(args.instr_number) + ' at ' + args.location + ', calibrated on ' + datetime.utcfromtimestamp(calib_date).strftime('%Y/%m/%d') +  ' - ' + args.fit_type + ' fit', fontsize = 14,transform=ax.transAxes)
plt.show()


