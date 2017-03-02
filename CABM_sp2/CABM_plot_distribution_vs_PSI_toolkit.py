import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import matplotlib.pyplot as plt
from matplotlib import dates
import mysql.connector
import math
import calendar
import SP2_utilities
import dateutil.parser
from scipy import stats


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
instr_ID 				= sp2_17
instr_location_ID 		= ETL
start_analysis 			= datetime(2013,6,1)
end_analysis	 		= datetime(2013,6,1,3,11)


start_analysis_UTC 		= calendar.timegm(start_analysis.utctimetuple())
end_analysis_UTC 		= calendar.timegm(end_analysis.utctimetuple())

#database connection
cnx = mysql.connector.connect(user='root', password='', host='localhost', database='CABM_SP2')
cursor = cnx.cursor()

#get PSI data
PSI_data_fit = []
PSI_data_meas = []
file = '/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/ETL20130601-distributions.txt'
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


		PSI_data_fit.append([fit_bin_mids,fit_norm_mass])
		PSI_data_meas.append([meas_bin_mids,meas_norm_mass])



#get UBC points\
#meas data
UBC_data_meas = []
cursor.execute(('''SELECT 
	dd.bin_ll,
	dd.bin_ul,
    dd.bin_mass,
    dd.bin_number,
	di.total_interval_volume 
FROM sp2_distribution_interval_data_locn2 di
		JOIN
	sp2_distribution_data_locn2 dd on dd.interval_ID = di.id
WHERE 	
	di.UNIX_UTC_ts_int_start = %s 
	and di.UNIX_UTC_ts_int_end = %s'''),
(start_analysis_UTC,end_analysis_UTC))	
data = cursor.fetchall()

for row in data:
	bin_ll = row[0]
	bin_ul = row[1]
	bin_mass = row[2]
	total_vol = row[4]
	bin_mid = bin_ll+(bin_ul-bin_ll)/2
	bin_mass_conc = bin_mass/total_vol
	bin_mass_conc_norm = bin_mass_conc/(math.log(bin_ul,10)-math.log(bin_ll,10))
	UBC_data_meas.append([bin_mid,bin_mass_conc_norm])


#fit data
UBC_data_fit = []
cursor.execute(('''SELECT 
	df.mass_fit_coeff_amp,
	df.mass_fit_coeff_mean,
    df.mass_fit_coeff_sd,
	di.total_interval_volume 
FROM sp2_distribution_interval_data_locn2 di
		JOIN
	sp2_distribution_fit_data_locn2 df on df.interval_ID = di.id
WHERE 	
	di.UNIX_UTC_ts_int_start = %s 
	and di.UNIX_UTC_ts_int_end = %s'''),
(start_analysis_UTC,end_analysis_UTC))	
fit_data = cursor.fetchall()

amp = fit_data[0][0]
mean = fit_data[0][1]
sd = fit_data[0][2]

print amp, mean,sd

UBC_fit_vals = []
for value in range(10,1000,5):
	fit_val = SP2_utilities.lognorm(value,amp,mean,sd)
	norm_factor = (math.log((value+2.5),10)-math.log((value-2.5),10))
	UBC_fit_vals.append([value,fit_val/(total_vol*norm_factor)])

PSI_bins = np.array([row[0] for row in PSI_data_meas])
PSI_mass = np.array([row[1]*1000 for row in PSI_data_meas])
PSI_fit_bins = np.array([row[0] for row in PSI_data_fit])
PSI_fit_mass = np.array([row[1]*1000 for row in PSI_data_fit])
UBC_bins = np.array([row[0] for row in UBC_data_meas])
UBC_mass = np.array([row[1] for row in UBC_data_meas])
UBC_fit_bins = np.array([row[0] for row in UBC_fit_vals])
UBC_fit_mass = np.array([row[1] for row in UBC_fit_vals])

#plotting
ticks = [10,20,30,40,50,60,80,120,200,300,400,600,1000]
fig = plt.figure()
ax1 = fig.add_subplot(111)
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
ax1.set_ylim(0,100)
plt.legend()
plt.savefig('/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/ETL20130601-mass distribution.png',bbox_inches = 'tight')

plt.show()
cnx.close()

