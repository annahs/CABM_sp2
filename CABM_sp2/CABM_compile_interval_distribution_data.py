#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
import math
from datetime import datetime
import calendar
import SP2_utilities
import CABM_utilities
import argparse
import CABM_distribution
from CABM_SP2_time_interval import CABMTimeInterval
from mysql_db_connection import dbConnection


#set arguments
parser = argparse.ArgumentParser(description='''
	This script is used to compile the mass distribution over a given interval (default is 24 hours).
	The distributions can be plotted using the optional -p argument.
	It can also update the appropriate sp2_time_intervals table with a correction factor for mass outside the detection range using the -u optional argument.
	''')
parser.add_argument('start_time', help='beginning of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('end_time', help='end of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('-p','--plot_distributions', help='plot the distributions', action='store_true')
parser.add_argument('-u','--update_correction_factor', help='update the correction factor in sp2_intervals table', action='store_true')
parser.add_argument('-i','--interval_length', help='sets the time interval over which to compile the distribution (in hours), default is 24',default=24, type= int)
args = parser.parse_args()

#set inputs
start_analysis				= calendar.timegm(args.start_time.utctimetuple())
end_analysis				= calendar.timegm(args.end_time.utctimetuple())
instr_location_ID	 		= CABM_utilities.getLocationID(args.location)
instr_ID					= CABM_utilities.getInstrID(args.instr_number)
interval_length				= args.interval_length*3600				


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


i = 0
multiple_records = []
distr_interval_start = start_analysis
while distr_interval_start < end_analysis:
	distr_interval_end = distr_interval_start + interval_length
 	print datetime.utcfromtimestamp(distr_interval_start),datetime.utcfromtimestamp(distr_interval_end)

 	#select all intervals data within this larger distribution period and do a QC double check
 	int_data = CABM_distribution.retrieveQCdIntervalData(instr_ID,instr_location_ID,distr_interval_start,distr_interval_end,cursor,cnx)
 	if int_data == []:
 		distr_interval_start += interval_length
 		continue

 	#sort interval data into dictionary
 	binned_mass_data_dict,binned_numb_data_dict,interval_id_set,total_volume = CABM_distribution.compileIntervalData(int_data)
	
	#get total bin mass concs for the distribution interval
	binned_mass_data_list = CABM_distribution.getOverallDistr(binned_mass_data_dict,total_volume)

	#fit a single lognormal to the overall distribution
	bin_mid_vals   = [row[0] for row in binned_mass_data_list]
	bin_mass_concs = [row[1] for row in binned_mass_data_list]
	popt,perr = SP2_utilities.fitFunction(SP2_utilities.lognorm,bin_mid_vals,bin_mass_concs,p0=(100,180,0.5))

	#get binning increment, check that it is constant, and exit if not
	bin_differences = list(np.diff(bin_mid_vals))
	if not bin_differences[1:] == bin_differences[:-1]:
		print 'Bin widths are not all equal'
		print 'exiting'
		sys.exit()
	else:
		binning_increment = int(np.mean(bin_differences))
	

	#calculate the fraction of the mass distribution sampled
	meas_mass = np.sum(bin_mass_concs)
	fit_masses = []
	for fit_bin in range(10,1000,binning_increment):
		fit_bin_mass = SP2_utilities.lognorm(fit_bin, popt[0], popt[1], popt[2])
		fit_masses.append(fit_bin_mass)
	fit_mass = np.sum(fit_masses)
	fraction_meas = meas_mass/fit_mass
	print 'fraction of mass in detection range = ', fraction_meas

	#calculate the uncertainty in the fraction of the mass distribution sampled
	fit_masses_ul = []
	for fit_bin in range(10,1000,binning_increment):
		fit_bin_mass = SP2_utilities.lognorm(fit_bin, (popt[0]+perr[0]), (popt[1]+perr[1]), (popt[2]+perr[2]))
		fit_masses_ul.append(fit_bin_mass)
	fit_mass_ul = np.sum(fit_masses_ul)
	fraction_meas_ll = meas_mass/fit_mass_ul
	fraction_meas_err = fraction_meas - fraction_meas_ll

	#update the interval table
	if args.update_correction_factor:
		CABM_distribution.updateIntervalTable(fraction_meas,fraction_meas_err,interval_id_set,instr_location_ID,cursor,cnx)

	#plot if desired
	if args.plot_distributions:
		CABM_distribution.plot_distr(binned_mass_data_list,binning_increment)

	distr_interval_start += interval_length



