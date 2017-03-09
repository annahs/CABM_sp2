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
import SP2_calibration
import dateutil.parser
from scipy import stats
import CABM_utilities
import SP2_utilities
import argparse
from mysql_db_connection import dbConnection


#set arguments
parser = argparse.ArgumentParser(description='''
	This script plots the timeseries data vs results from the PSI toolkit
	''')
parser.add_argument('start_time', help='beginning of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('end_time', help='end of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
parser.add_argument('file_name', help='PSI data file name',type=str)
args = parser.parse_args()

print args.start_time
print args.end_time	


#set inputs
start_analysis				= args.start_time
end_analysis				= args.end_time
instr_location_ID	 		= CABM_utilities.getLocationID(args.location)
instr_ID					= CABM_utilities.getInstrID(args.instr_number)
file 						= args.file_name
title 						= 'SP2 #' + str(args.instr_number) +' at '+ args.location + ' - ' + str(start_analysis.date())

#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur

#get PSI and UBC data
PSI_data = []
all_data = []
file = '/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/' + file
with open(file) as f:
	f.readline()
	for line in f:
		newline = line.split()
		date_time = dateutil.parser.parse(newline[0]+' '+newline[1])

		if start_analysis <= date_time < end_analysis:
			UNIX_datetime = calendar.timegm(date_time.utctimetuple())

			try:
				number_conc = float(newline[2])
				mass_conc = float(newline[3])*1000
			except:
				number_conc = np.nan
				mass_conc = np.nan
			
			#get UBC points
			cursor.execute(('''SELECT 
					total_interval_mass,
					total_interval_number,
					total_interval_volume,
					fraction_of_mass_sampled
				FROM sp2_time_intervals_locn'''+ str(instr_location_ID)+'''  
				WHERE UNIX_UTC_ts_int_start <= %s 
					and UNIX_UTC_ts_int_end > %s
					and instr_ID = %s
				LIMIT 1'''),
			(UNIX_datetime,UNIX_datetime,instr_ID))	

			data = cursor.fetchall()
			for row in data:
				fraction_of_mass_sampled = np.float32(row[3])
				UBC_mass_conc = np.float32(row[0])/(np.float32(row[2]))
				UBC_numb_conc = np.float32(row[1])/np.float32(row[2])
			#except:
			#	print datetime.utcfromtimestamp(UNIX_datetime),row,"***"

			#if UBC_mass_conc < 1000:
			PSI_data.append([date_time,number_conc,mass_conc])
			all_data.append([date_time,number_conc,mass_conc,UBC_numb_conc,UBC_mass_conc])


time = [dates.date2num(row[0]) for row in all_data]
PSI_numb = np.array([row[1] for row in all_data])
PSI_mass = np.array([row[2] for row in all_data])
UBC_numb = np.array([row[3] for row in all_data])
UBC_mass = np.array([row[4] for row in all_data])

#plotting
hfmt = dates.DateFormatter('%H:%M')
fig = plt.figure(figsize=(12,10))

ax1 = plt.subplot2grid((2,2), (0,0))
ax2 = plt.subplot2grid((2,2), (0,1))
ax3 = plt.subplot2grid((2,2), (1,0))
ax4 = plt.subplot2grid((2,2), (1,1))

ax1.scatter(time,PSI_mass, color = 'r', label='PSI toolkit')
ax1.scatter(time,UBC_mass, color = 'b', label='UBC toolkit')
ax1.xaxis.set_major_formatter(hfmt)
ax1.set_ylabel('rBC mass concentration (ng/m3)')
ax1.set_xlabel('Time')
ymin, ymax = ax1.get_ylim()
ax1.set_ylim(0,ymax)
ax1.legend()


mask = np.isfinite(PSI_mass) & np.isfinite(UBC_mass)
slope, intercept, r_value, p_value, std_err = stats.linregress(PSI_mass[mask], UBC_mass[mask])
print "r-squared:", r_value**2

fit_y_vals = []
for value in [0,ymax]:
	y_val = slope*value + intercept
	fit_y_vals.append(y_val)

ax2.scatter(PSI_mass,UBC_mass, color = 'grey')
ax2.set_ylabel('UBC mass concentration (ng/m3)')
ax2.set_xlabel('PSI mass concentration (ng/m3)')
ax2.set_ylim(0,ymax)
ax2.set_xlim(0,ymax)
ax2.plot([0,ymax],[0,ymax],color = 'k', label ='1:1 line')
ax2.plot([0,ymax],fit_y_vals,color = 'r', label = 'linear fit')
ax2.text(0.75, 0.35,'r-squared: ' + str(round(r_value**2,2)), ha='center', va='center', transform=ax2.transAxes)
ax2.legend()


ax3.scatter(time,PSI_numb, color = 'r', linestyle = '-')
ax3.scatter(time,UBC_numb, color = 'b', linestyle = '-')
ax3.xaxis.set_major_formatter(hfmt)
ax3.set_ylabel('rBC number concentration (#/cm3)')
ymin, ymax = ax3.get_ylim()
ax3.set_ylim(0,ymax)
ax3.set_xlabel('Time')

mask = np.isfinite(PSI_numb) & np.isfinite(UBC_numb)
slope, intercept, r_value, p_value, std_err = stats.linregress(PSI_numb[mask], UBC_numb[mask])
print "r-squared:", r_value**2

fit_y_vals = []
for value in [0,ymax]:
	y_val = slope*value + intercept
	fit_y_vals.append(y_val)


ax4.scatter(PSI_numb,UBC_numb, color = 'grey')
ax4.set_ylabel('UBC number concentration (#/cm3)')
ax4.set_xlabel('PSI number concentration (#/cm3)')
ax4.set_ylim(0,ymax)
ax4.set_xlim(0,ymax)
ax4.plot([0,ymax],[0,ymax],color = 'k',)
ax4.plot([0,ymax],fit_y_vals,color = 'r')
ax4.text(0.75, 0.35,'r-squared: ' + str(round(r_value**2,2)), ha='center', va='center', transform=ax4.transAxes)

fig.suptitle(title, fontsize=14)


#plt.savefig('/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/'+title+ ' timeseries.png',bbox_inches = 'tight')
cnx.close()

plt.show()



