import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
from matplotlib import dates
import math
import calendar
import SP2_utilities
import dateutil.parser
from scipy import stats
import CABM_utilities
import argparse
import CABM_distribution
from CABM_SP2_time_interval import CABMTimeInterval
from mysql_db_connection import dbConnection



#set arguments
parser = argparse.ArgumentParser(description='''
	This script plots the data vs results from the PSI toolkit
	''')
parser.add_argument('start_time', help='beginning of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('end_time', help='end of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('file_name', help='PSI data file name',type=str)
parser.add_argument('-i','--interval_length', help='sets the time interval over which to compile the distribution (in hours), default is 24',default=24, type= int)
args = parser.parse_args()

print args.start_time
print args.end_time	


#set inputs
start_analysis				= calendar.timegm(args.start_time.utctimetuple())
end_analysis				= calendar.timegm(args.end_time.utctimetuple())
instr_location_ID	 		= CABM_utilities.getLocationID(args.location)
instr_ID					= CABM_utilities.getInstrID(args.instr_number)
file 						= args.file_name
interval_length				= args.interval_length*3600		
binning_increment			= 10						
title 						= 'SP2 #' + str(args.instr_number) +' at '+ args.location + ' - ' + str(args.start_time.date())


#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


#get PSI data
PSI_data_fit = []
PSI_data_meas = []
file = '/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/'+ file
with open(file) as f:
	f.readline()
	for line in f:
		newline = line.split()
		fit_bin_mids = float(newline[0])
		fit_norm_mass = float(newline[1])
		fit_norm_numb = float(newline[2])
		try:
			meas_bin_mids = float(newline[3])
			meas_norm_mass = float(newline[4])
			meas_norm_numb = float(newline[5])
		except:
			print 'no meas'


		PSI_data_fit.append([fit_bin_mids,fit_norm_mass,fit_norm_numb])
		PSI_data_meas.append([meas_bin_mids,meas_norm_mass,meas_norm_numb])



#get UBC points
#select all intervals data within this larger distribution period and do a QC double check
int_data = CABM_distribution.retrieveQCdIntervalData(instr_ID,instr_location_ID,start_analysis,end_analysis,cursor,cnx)

#sort interval data into dictionary
binned_mass_data_dict,binned_numb_data_dict,interval_id_set,total_volume = CABM_distribution.compileIntervalData(int_data)

#get total bin mass concs for the distribution interval
binned_mass_data_list = CABM_distribution.getOverallDistr(binned_mass_data_dict,total_volume)
binned_numb_data_list = CABM_distribution.getOverallDistr(binned_numb_data_dict,total_volume)

#fit a single lognormal to the overall distribution
bin_mid_vals 	= []
bin_mass_concs	= []
bin_numb_concs	= []
i=0
for row in binned_mass_data_list:
	bin_mid 	  = row[0] 
	bin_mass_conc = row[1] 
	bin_numb_conc = binned_numb_data_list[i][1]
	bin_mass_conc_norm = bin_mass_conc/(math.log(bin_mid+binning_increment/2.,10)-math.log(bin_mid-binning_increment/2.,10))
	bin_numb_conc_norm = bin_numb_conc/(math.log(bin_mid+binning_increment/2.,10)-math.log(bin_mid-binning_increment/2.,10))
	bin_mid_vals.append(bin_mid)
	bin_mass_concs.append(bin_mass_conc_norm)
	bin_numb_concs.append(bin_numb_conc_norm)
	i+=1

popt,perr = SP2_utilities.fitFunction(SP2_utilities.lognorm,[row[0] for row in binned_mass_data_list],[row[1] for row in binned_mass_data_list],p0=(100,180,0.5))
fit_masses = []
for fit_bin in range(10,1000,binning_increment):
	fit_bin_mass = SP2_utilities.lognorm(fit_bin, popt[0], popt[1], popt[2])
	fit_masses.append(fit_bin_mass/(math.log(fit_bin+binning_increment/2.,10)-math.log(fit_bin-binning_increment/2.,10)))


#plotting

fig = plt.figure(figsize=(8,10))
ax1 = plt.subplot2grid((2,1), (0,0))
ax2 = plt.subplot2grid((2,1), (1,0))
ticks = [10,20,30,40,50,60,80,120,200,300,400,600,1000]

PSI_bins 		= np.array([row[0] for row in PSI_data_meas])
PSI_mass 		= np.array([row[1]*1000 for row in PSI_data_meas])
PSI_numb 		= np.array([row[2] for row in PSI_data_meas])
PSI_fit_bins 	= np.array([row[0] for row in PSI_data_fit])
PSI_fit_mass 	= np.array([row[1]*1000 for row in PSI_data_fit])

UBC_bins 		= bin_mid_vals
UBC_mass 		= bin_mass_concs
UBC_numb 		= bin_numb_concs
UBC_fit_bins 	= range(10,1000,binning_increment)
UBC_fit_mass 	= fit_masses


ax1.scatter(PSI_bins,PSI_mass, color ='r',label='PSI toolkit')
ax1.plot(PSI_fit_bins,PSI_fit_mass, color ='r')
ax1.scatter(UBC_bins,UBC_mass, color ='b',label='UBC toolkit')
ax1.plot(UBC_fit_bins,UBC_fit_mass, color ='b')
ax1.xaxis.set_major_formatter(plt.FormatStrFormatter('%s'))
ax1.xaxis.set_major_locator(plt.FixedLocator(ticks))
ax1.set_xscale('log')
ax1.set_xlabel('rBC VED (nm)')
ax1.set_ylabel('dM/dlogD (ng/m3)')
ax1.set_xlim(10,1000)
ax1.set_ylim(0,10)
ax1.legend()


ax2.scatter(PSI_bins,PSI_numb, color ='r',label='PSI toolkit')
#ax2.plot(PSI_fit_bins,PSI_fit_numb, color ='r')
ax2.scatter(UBC_bins,UBC_numb, color ='b',label='UBC toolkit')
#ax2.plot(UBC_fit_bins,UBC_fit_numb, color ='b')
ax2.xaxis.set_major_formatter(plt.FormatStrFormatter('%s'))
ax2.xaxis.set_major_locator(plt.FixedLocator(ticks))
ax2.set_xscale('log')
ax2.set_xlabel('rBC VED (nm)')
ax2.set_ylabel('dN/dlogD (#/cm3)')
ax2.set_xlim(10,1000)
ax2.set_ylim(0,10)

fig.suptitle(title, fontsize=14)
plt.savefig('/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/'+title+ ' distributions.png',bbox_inches = 'tight')

plt.show()
cnx.close()

