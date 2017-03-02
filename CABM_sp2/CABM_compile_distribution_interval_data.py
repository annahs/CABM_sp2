# -*- coding: UTF-8 -*-
#this script is used to add individual particle incandescence information to the database

import sys
import os
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
from struct import *
import math
import mysql.connector
from datetime import datetime
import calendar
import SP2_utilities
import SP2_plotting
from CABM_SP2_time_interval import CABMTimeInterval
from mysql_db_connection import dbConnection



#locations
Alert = 1
ETL = 2
Egbert = 3
Resolute = 4
Whistler = 5
#instruments
sp2_17 = 1
sp2_44 = 2
sp2_58 = 3

#setup
instr_ID 					= sp2_17
instr_location_ID 			= ETL
start_analysis 				= datetime(2013,6,1)
end_analysis	 			= datetime(2013,6,1,3)
binning_increment			= 10						#nm
interval_length				= 3600*3				#in seconds
database_name 				= 'CABM_SP2'
intervals_table_name 		= 'sp2_interval_data_locn'+str(instr_location_ID)
distrs_table_name 			= 'sp2_distribution_data_locn'+str(instr_location_ID)


def plot_distr(binned_data_list):
	norm_concs = []
	bin_mids = []
	for row in binned_data_list:
		bin_mid = row[0]
		bin_conc = row[1]
		bin_conc_norm = bin_conc/(math.log(bin_mid+binning_increment/2.,10)-math.log(bin_mid-binning_increment/2.,10))
		norm_concs.append(bin_conc_norm)
		bin_mids.append(bin_mid)


	popt,perr = SP2_utilities.fitFunction(SP2_utilities.lognorm,bin_mids,norm_concs,p0=(100,180,0.5))

	fit_bin_mids = np.arange(10,1000,5)
	fit_concs = []
	for fit_bin_mid in fit_bin_mids:
		fit_conc_norm  = SP2_utilities.lognorm(fit_bin_mid, popt[0], popt[1], popt[2])
		fit_concs.append(fit_conc_norm)
	
	
	ticks = [10,20,30,40,50,60,80,120,200,300,400,600,1000]
	fig = plt.figure()
	ax1 = fig.add_subplot(111)
	ax1.plot(fit_bin_mids,fit_concs)
	ax1.scatter(bin_mids,norm_concs)
	ax1.xaxis.set_major_formatter(plt.FormatStrFormatter('%d'))
	ax1.xaxis.set_major_locator(plt.FixedLocator(ticks))
	ax1.set_xscale('log')
	ax1.set_xlim(10,1000)
	ax1.set_ylabel('dM/dlogD (ng/m3)')
	ax1.set_xlabel('rBC VED (nm)')
	plt.show()



def retrieveQCdIntervalData(instr_ID,instr_location_ID,distr_interval_start,distr_interval_end):
	cursor.execute('''
		SELECT 
			distrd.bin_ll,
			distrd.bin_ul,
			distrd.bin_mass,
			intd.total_interval_volume,
			intd.id
		FROM  
			sp2_interval_data_locn''' + str(instr_location_ID) + ''' intd
			JOIN
			sp2_distribution_data_locn''' + str(instr_location_ID) + ''' distrd on distrd.interval_ID = intd.id
			LEFT JOIN 
			sp2_qc_intervals_locn''' + str(instr_location_ID) + ''' qc on ((intd.UNIX_UTC_ts_int_start < qc.UNIX_UTC_ts_int_end) and (qc.UNIX_UTC_ts_int_start < intd.UNIX_UTC_ts_int_end))		
		WHERE 
			intd.instr_ID = %s
			AND qc.UNIX_UTC_ts_int_start IS NULL
			AND (intd.UNIX_UTC_ts_int_start BETWEEN %s AND %s)
			''',
		(instr_ID,distr_interval_start,distr_interval_end))
	int_data = cursor.fetchall()

	return int_data

def compileIntervalData(int_data):
	binned_data_dict = {}
	interval_id_set = set()
 	for row in int_data:
 		bin_ll 			= row[0]
 		bin_ul 			= row[1]
 		bin_mid 		= bin_ll + (bin_ul-bin_ll)/2 
 		bin_mass 		= row[2]
 		int_volume 		= row[3]
 		bin_mass_conc 	= (bin_mass/int_volume)
 		int_id 			= row[4]

 		interval_id_set.add(int_id)
 		
 		if bin_mid in binned_data_dict:
 			binned_data_dict[bin_mid].append(bin_mass_conc)
		else:
			binned_data_dict[bin_mid] = [bin_mass_conc]

	return binned_data_dict, interval_id_set


def getOverallDistr(binned_data_dict):
	binned_data_list = []
	for bin_mid in binned_data_dict:
		total_mass_conc = np.mean(binned_data_dict[bin_mid])
		binned_data_list.append([bin_mid,total_mass_conc])
	binned_data_list.sort()

	return binned_data_list

def updateIntervalTable(fraction_meas, fraction_meas_err, interval_id_set):
	for interval_id in interval_id_set:
		cursor.execute('''
			UPDATE 
				sp2_interval_data_locn''' + str(instr_location_ID) + '''
			SET 
				fraction_of_mass_sampled = %s,
				fraction_of_mass_sampled_uncertainty = %s
			WHERE
				id = %s

			''',
		(float(fraction_meas),float(fraction_meas_err),interval_id))
		cnx.commit()


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


i = 0
multiple_records = []
distr_interval_start 	= calendar.timegm(start_analysis.utctimetuple())
while distr_interval_start < calendar.timegm(end_analysis.utctimetuple()):
	distr_interval_end = distr_interval_start + interval_length
 	print datetime.utcfromtimestamp(distr_interval_start),datetime.utcfromtimestamp(distr_interval_end)

 	#select all intervals data within this larger distribution period and do a QC double check
 	int_data = retrieveQCdIntervalData(instr_ID,instr_location_ID,distr_interval_start,distr_interval_end)

 	# short interval data into dictionary
 	binned_data_dict,interval_id_set = compileIntervalData(int_data)
	
	#get total bin mass concs for the distribution interval
	binned_data_list = getOverallDistr(binned_data_dict)

	#fit a single lognormal to the overall distribution
	bin_mid_vals   = [row[0] for row in binned_data_list]
	bin_mass_concs = [row[1] for row in binned_data_list]
	popt,perr = SP2_utilities.fitFunction(SP2_utilities.lognorm,bin_mid_vals,bin_mass_concs,p0=(100,180,0.5))

	#calculate the fraction of the mass distribution sampled
	meas_mass = np.sum(bin_mass_concs)
	fit_masses = []
	for fit_bin in range(10,1000,binning_increment):
		fit_bin_mass = SP2_utilities.lognorm(fit_bin, popt[0], popt[1], popt[2])
		fit_masses.append(fit_bin_mass)
	fit_mass = np.sum(fit_masses)
	fraction_meas = meas_mass/fit_mass
	print fraction_meas

	#calculate the uncertainty in the fraction of the mass distribution sampled
	fit_masses_ul = []
	for fit_bin in range(10,1000,binning_increment):
		fit_bin_mass = SP2_utilities.lognorm(fit_bin, (popt[0]+perr[0]), (popt[1]+perr[1]), (popt[2]+perr[2]))
		fit_masses_ul.append(fit_bin_mass)
	fit_mass_ul = np.sum(fit_masses_ul)
	fraction_meas_ll = meas_mass/fit_mass_ul
	fraction_meas_err = fraction_meas - fraction_meas_ll

	#update the interval table
	updateIntervalTable(fraction_meas, fraction_meas_err, interval_id_set)

	#plot if desired
	plot_distr(binned_data_list)

	distr_interval_start += interval_length



