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
import SP2_calibration
import dateutil.parser

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
instr_location_ID 		= Whistler
start_analysis 			= datetime(2009,7,1)
end_analysis	 		= datetime(2009,7,2)


start_analysis_UTC 		= calendar.timegm(start_analysis.utctimetuple())
end_analysis_UTC 		= calendar.timegm(end_analysis.utctimetuple())

#database connection
cnx = mysql.connector.connect(user='root', password='', host='localhost', database='CABM_SP2')
cursor = cnx.cursor()

#get PSI data
PSI_data = []
file = '/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/PSI_toolkit_timeseries-WHI2009.txt'
with open(file) as f:
	f.readline()
	for line in f:
		newline = line.split()
		date_time = dateutil.parser.parse(newline[0]+' '+newline[1])
		try:
			number_conc = float(newline[2])
			mass_conc = float(newline[3])*1000
		except:
			number_conc = np.nan
			mass_conc = np.nan
		PSI_data.append([date_time,number_conc,mass_conc])



#get UBC data
cursor.execute(('''SELECT 
				UNIX_UTC_ts_int_start,
				UNIX_UTC_ts_int_end,
				total_interval_mass,
				total_interval_mass_uncertainty,
				total_interval_number,
				total_interval_volume 
			FROM sp2_interval_data_locn5 
			WHERE UNIX_UTC_ts_int_start >= %s 
				and UNIX_UTC_ts_int_end < %s
				and instr_ID = %s
				and instr_location_ID = %s'''),
		(start_analysis_UTC,end_analysis_UTC,instr_ID,instr_location_ID))	
data = cursor.fetchall()
print len(data)

UBC_data = []
for row in data:
	int_start 						= datetime.utcfromtimestamp(row[0])
	int_end 						= datetime.utcfromtimestamp(row[1])
	int_mid 						= int_start + (int_end-int_start)/2
	total_interval_mass 			= row[2]
	total_interval_mass_uncertainty = row[3]
	total_interval_number 			= row[4]
	total_interval_volume 			= row[5]
	
	mass_conc = total_interval_mass/total_interval_volume
	number_conc = total_interval_number/total_interval_volume


	UBC_data.append([int_mid,number_conc,mass_conc])

PSI_time = [dates.date2num(row[0]) for row in PSI_data]
PSI_numb = [row[1] for row in PSI_data]
PSI_mass = [row[2] for row in PSI_data]

UBC_time = [dates.date2num(row[0]) for row in UBC_data]
UBC_numb = [row[1] for row in UBC_data]
UBC_mass = [row[2] for row in UBC_data]

#plotting
hfmt = dates.DateFormatter('%d/%Y %H:%M')


fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(PSI_time,PSI_mass, color = 'r', linestyle = '-')
ax.plot(UBC_time,UBC_mass, color = 'b', linestyle = '-')
ax.xaxis.set_major_formatter(hfmt)


plt.show()


cnx.close()

