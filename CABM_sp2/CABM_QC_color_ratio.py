#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
import math
from datetime import datetime
from datetime import timedelta
from matplotlib import dates
import calendar
import argparse
import dateutil
from CABM_SP2_time_interval import CABMTimeInterval
from mysql_db_connection import dbConnection
import SP2_utilities
import CABM_utilities


#set arguments
parser = argparse.ArgumentParser(description='''
	Plots the color ratio (high-gain broad-band/high-gain narrow-band incandesce amplitude) as a function or rBC particle size over time.
	''')
parser.add_argument('start_time', help='beginning of interval to plot - format YYYY-MM-DD ',type=SP2_utilities.valid_date)
parser.add_argument('end_time', help='end of interval to plot - format YYYY-MM-DD ',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('-bi','--binning_interval', help='binning interval length in hours (default is 6)',default=6,type=int)
args = parser.parse_args()


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


#set inputs
start_analysis		= args.start_time
end_analysis		= args.end_time
instr_location_ID 	= CABM_utilities.getLocationID(args.location)
instr_ID			= CABM_utilities.getInstrID(args.instr_number)
min_VED				= 80							# in nm
max_VED				= 220							# in nm
binning_increment	= 20 							# in nm
interval_length		= args.binning_interval*3600	# in seconds
database_name 		= 'CABM_SP2'



#functions
def makeBinDict(min_VED,max_VED,binning_increment):
	new_dict = {}
	for bin in range(min_VED,(max_VED+binning_increment),binning_increment):
		new_dict[bin] = [bin,(bin+binning_increment)]
	return new_dict


#script
plot_data = []
interval_start 	= calendar.timegm(start_analysis.utctimetuple())
while (interval_start + interval_length) <= calendar.timegm(end_analysis.utctimetuple()):
	interval_end = interval_start + interval_length
	print datetime.utcfromtimestamp(interval_start),datetime.utcfromtimestamp(interval_end)

	#instantiate a time interval object
	time_interval = CABMTimeInterval(database_name,instr_location_ID, instr_ID, interval_start, interval_end)
	
	#retreive single particle data for the interval
	time_interval.retrieveSingleParticleData()

	interval_dict = makeBinDict(min_VED,max_VED,binning_increment)

	#process data
	print 'number of particles:', len(time_interval.single_particle_data)
	if len(time_interval.single_particle_data) > 0 :
		for row in time_interval.single_particle_data:
			ind_start_time 	= row[0] 	#UNIX UTC timestamp
			ind_end_time 	= row[1]	#UNIX UTC timestamp
			BB_incand_HG 	= row[2]  	#in arbitrary units
			BB_incand_LG 	= row[3]  	#in arbitrary units
			sample_flow 	= row[4]  	#in vccm
			NB_incand_HG	= row[6]	#in arbitrary units
			try:
				color_ratio 	= BB_incand_HG/NB_incand_HG
			except:
				continue
			
			try:
				rBC_mass,rBC_mass_uncertainty = time_interval.calculateMass(BB_incand_HG,BB_incand_LG,ind_end_time)
			except:
				print interval_start
				print time_interval.calibration_info
				print BB_incand_HG, BB_incand_LG
				sys.exit()

			VED = SP2_utilities.calculateVED(time_interval.rBC_density,rBC_mass)

			for point in interval_dict:
				LL_bin = interval_dict[point][0]
				UL_bin = interval_dict[point][1]
				
				if len(interval_dict[point]) < 3:
					interval_dict[point].append([])
				if (LL_bin <= VED < UL_bin):
					interval_dict[point][2].append(color_ratio)

		for point in interval_dict:		
			avg_ratio = float(np.mean(interval_dict[point][2]))
			if point == 80:
				r_80_100 = avg_ratio
			if point == 100:
				r_100_120 = avg_ratio
			if point == 120:
				r_120_140 = avg_ratio
			if point == 140:
				r_140_160 = avg_ratio
			if point == 160:
				r_160_180 = avg_ratio
			if point == 180:
				r_180_200 = avg_ratio
			if point == 200:
				r_200_220 = avg_ratio
		
		plot_data.append([interval_start, interval_end, r_80_100,r_100_120,r_120_140,r_140_160,r_160_180,r_180_200,r_200_220])
	
	interval_start += 86400


#get hk - yag power data
cursor.execute('''
	SELECT 
		UNIX_UTC_ts_int_start,
		yag_power
	FROM  sp2_hk_data_locn''' + str(instr_location_ID) + ''' 
	WHERE 
		instr_ID = %s
		AND (UNIX_UTC_ts_int_start BETWEEN %s AND %s)
		AND id % 60 = %s
		''',
	(instr_ID,calendar.timegm(start_analysis.utctimetuple()),calendar.timegm(end_analysis.utctimetuple()),0))
hk_data = cursor.fetchall()

hk_datetime	= [dates.date2num(datetime.utcfromtimestamp(row[0])) for row in hk_data]
yag_power 	= [row[1] for row in hk_data]


#plot data
plot_datetime	= [dates.date2num(datetime.utcfromtimestamp(row[0])) for row in plot_data]
r80_100 		= [row[2] for row in plot_data]
r100_120 		= [row[3] for row in plot_data]
r120_140 		= [row[4] for row in plot_data]
r140_160 		= [row[5] for row in plot_data]
r160_180 		= [row[6] for row in plot_data]
r180_200 		= [row[7] for row in plot_data]
r200_220 		= [row[8] for row in plot_data]

hfmt = dates.DateFormatter('%Y-%m-%d')

fig = plt.figure(figsize=(12,10))
ax1 = fig.add_subplot(211)
ax1.plot(plot_datetime,r80_100 ,label = '80-100nm')
ax1.plot(plot_datetime,r100_120,label = '100-120nm')
ax1.plot(plot_datetime,r120_140,label = '120-140nm')
ax1.plot(plot_datetime,r140_160,label = '140-160nm')
ax1.plot(plot_datetime,r160_180,label = '160-180nm')
ax1.plot(plot_datetime,r180_200,label = '180-200nm')
ax1.plot(plot_datetime,r200_220,label = '200-220nm')
ax1.set_ylabel('BB/NB ratio')
ax1.set_xticklabels([])
ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 1.20),ncol=4)
ax1.set_ylim(1,3)
ax1.set_xlim(dates.date2num(start_analysis),dates.date2num(end_analysis))

ax2 = fig.add_subplot(212)
ax2.plot(hk_datetime,yag_power, color='grey')
ax2.set_ylabel('yag power (hk)', color='grey')
ax2.xaxis.set_major_formatter(hfmt)
ax2.set_xlabel('Date')
ax2.set_ylim(2,8)
ax2.set_xlim(dates.date2num(start_analysis),dates.date2num(end_analysis))


plt.subplots_adjust(wspace=0, hspace=0)

plt.text(0.05, 0.9,'SP2#' + str(args.instr_number) + ' at ' + args.location + ' ' + start_analysis.strftime('%Y/%m/%d') + '-' + end_analysis.strftime('%Y/%m/%d') , fontsize = 14,transform=ax1.transAxes)


os.chdir('/Users/mcallister/projects/CABM_sp2/docs/')
plt.savefig('SP2#' + str(args.instr_number) + ' at ' + args.location + ' ' + start_analysis.strftime('%Y%m%d') + '-' + end_analysis.strftime('%Y%m%d')  +  ' - BB to NB color ratio.png', bbox_inches='tight') 

plt.show()

