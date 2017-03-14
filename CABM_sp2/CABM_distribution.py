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
from CABM_SP2_time_interval import CABMTimeInterval
from mysql_db_connection import dbConnection


def plot_distr(binned_data_list,binning_increment):
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



def retrieveQCdIntervalData(instr_ID,instr_location_ID,distr_interval_start,distr_interval_end,cursor,cnx):
	cursor.execute('''
		SELECT 
			distrd.bin_ll,
			distrd.bin_ul,
			distrd.bin_mass,
			distrd.bin_number,
			intd.total_interval_volume,
			intd.id
		FROM  
			sp2_time_intervals_locn''' + str(instr_location_ID) + ''' intd
			JOIN
			sp2_time_interval_binned_data_locn''' + str(instr_location_ID) + ''' distrd on distrd.interval_ID = intd.id
			LEFT JOIN 
			sp2_qc_intervals_locn''' + str(instr_location_ID) + ''' qc on ((intd.UNIX_UTC_ts_int_start < qc.UNIX_UTC_ts_int_end) and (qc.UNIX_UTC_ts_int_start < intd.UNIX_UTC_ts_int_end))		
		WHERE 
			intd.instr_ID = %s
			AND qc.UNIX_UTC_ts_int_start IS NULL
			AND intd.UNIX_UTC_ts_int_start >= %s 
			AND intd.UNIX_UTC_ts_int_start < %s
			''',
		(instr_ID,distr_interval_start,distr_interval_end))
	int_data = cursor.fetchall()

	return int_data

def compileIntervalData(int_data):
	binned_mass_data_dict 	= {}
	binned_numb_data_dict 	= {}
	total_volume 			= 0
	interval_id_set 		= set()
	prev_int_id 			= np.nan
 	for row in int_data:
 		bin_ll 			= row[0]
 		bin_ul 			= row[1]
 		bin_mid 		= bin_ll + (bin_ul-bin_ll)/2 
 		bin_mass 		= row[2]
 		bin_number 		= row[3]
 		int_volume 		= row[4]
 		int_id 			= row[5]
 		if int_id != prev_int_id:
 			total_volume += int_volume
		prev_int_id = int_id
 		interval_id_set.add(int_id)
 		
 		if bin_mid in binned_mass_data_dict:
 			binned_mass_data_dict[bin_mid].append(bin_mass)
 			binned_numb_data_dict[bin_mid].append(bin_number)
		else:
			binned_mass_data_dict[bin_mid] = [bin_mass]
			binned_numb_data_dict[bin_mid] = [bin_number]

	return binned_mass_data_dict,binned_numb_data_dict,interval_id_set,total_volume


def getOverallDistr(binned_data_dict,total_volume):
	binned_data_list = []
	for bin_mid in binned_data_dict:
		total_mass_conc = np.sum(binned_data_dict[bin_mid])/total_volume
		binned_data_list.append([bin_mid,total_mass_conc])
	binned_data_list.sort()
	binned_data_list.pop()
	return binned_data_list

def updateIntervalTable(fraction_meas,fraction_meas_err,interval_id_set,instr_location_ID,cursor,cnx):
	print 'updating database'
	for interval_id in interval_id_set:
		cursor.execute('''
			UPDATE 
				sp2_time_intervals_locn''' + str(instr_location_ID) + '''
			SET 
				fraction_of_mass_sampled = %s,
				fraction_of_mass_sampled_uncertainty = %s
			WHERE
				id = %s

			''',
		(float(fraction_meas),float(fraction_meas_err),interval_id))
		cnx.commit()

