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
	This script plots the timeseries data.  Note that the mass concentrations are corrected for the fraction of mass within the instrument detection limits 
	(determined from the mass distribution), but no such correction is applied to the number concentrations.
	''')
parser.add_argument('start_time', help='beginning of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('end_time', help='end of intervals - format flexible ',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
args = parser.parse_args()

print args.start_time
print args.end_time	


#set inputs
start_analysis				= args.start_time
end_analysis				= args.end_time
UNIX_start_analysis			= calendar.timegm(start_analysis.utctimetuple())
UNIX_end_analysis			= calendar.timegm(end_analysis.utctimetuple())
instr_location_ID	 		= CABM_utilities.getLocationID(args.location)
instr_ID					= CABM_utilities.getInstrID(args.instr_number)
title 						= 'SP2 #' + str(args.instr_number) +' at '+ args.location + ' - ' + str(start_analysis.date())

#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur

timeseries_data = []
#get UBC points
cursor.execute(('''SELECT 
		UNIX_UTC_ts_int_start,
		UNIX_UTC_ts_int_end,	
		total_interval_mass,
		total_interval_mass_uncertainty,
		total_interval_number,
		total_interval_volume,
		fraction_of_mass_sampled,
		fraction_of_mass_sampled_uncertainty
	FROM sp2_time_intervals_locn'''+ str(instr_location_ID)+'''  
	WHERE UNIX_UTC_ts_int_start >= %s 
		and UNIX_UTC_ts_int_start < %s
		and instr_ID = %s'''),
	(UNIX_start_analysis,UNIX_end_analysis,instr_ID))	
	

data = cursor.fetchall()
for row in data:
	interval_start 				= row[0]
	interval_end 				= row[1]
	interval_mid 				= interval_start + (interval_end-interval_start)/2
	fraction_of_mass_sampled 	= np.float32(row[6])
	fraction_of_mass_sampled_err= np.float32(row[7])
	mass_conc 					= np.float32(row[2])/(np.float32(row[5])*fraction_of_mass_sampled)
	mass_conc_err 				= np.float32(row[3])/(np.float32(row[5])*fraction_of_mass_sampled)
	numb_conc 					= np.float32(row[4])/(np.float32(row[5]))

	timeseries_data.append([interval_mid,mass_conc,numb_conc])

time 		= [dates.date2num(datetime.utcfromtimestamp(row[0])) for row in timeseries_data]
mass_concs 	= [row[1] for row in timeseries_data]
numb_concs 	= [row[2] for row in timeseries_data]

#plotting
hfmt = dates.DateFormatter('%Y%m%d %H:%M')
fig = plt.figure(figsize=(12,10))

ax1 = plt.subplot2grid((2,1), (0,0))
ax2 = plt.subplot2grid((2,1), (1,0))

ax1.scatter(time,mass_concs, color = 'b', marker = 'o')
ax1.xaxis.set_major_formatter(hfmt)
ax1.set_ylabel('rBC mass concentration (ng/m3)')
ax1.set_xlabel('Time')
ymin, ymax = ax1.get_ylim()
ax1.set_ylim(0,ymax)

ax2.scatter(time,numb_concs, color = 'g')
ax2.xaxis.set_major_formatter(hfmt)
ax2.set_ylabel('rBC number concentration (#/m3)')
ax2.set_xlabel('Time')
ymin, ymax = ax2.get_ylim()
ax2.set_ylim(0,ymax)

fig.suptitle(title, fontsize=14)

plt.show()



