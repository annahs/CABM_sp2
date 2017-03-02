#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import math
import calendar
import os.path
import matplotlib.pyplot as plt
from matplotlib import dates
import dateutil
import argparse
from datetime import datetime
from pprint import pprint
from mysql_db_connection import dbConnection
import SP2_housekeeping
import SP2_utilities
import CABM_utilities


#set arguments
parser = argparse.ArgumentParser(description='''
	Plots the housekeeping data over a specified date range for a particular location and instrument
	''')
parser.add_argument('start_time', help='beginning of interval to plot - format YYYY-MM-DD ',type=SP2_utilities.valid_date)
parser.add_argument('end_time', help='end of interval to plot - format YYYY-MM-DD ',type=SP2_utilities.valid_date)
parser.add_argument('location', help='CABM site name. Options: Alert, ETL, Egbert, Resolute, Whistler ',type=str)
parser.add_argument('instr_number', help='SP2 number. Options: 17, 44, 58 ',type=int)
args = parser.parse_args()

#create db connection and cursor
database_connection = dbConnection('CABM_SP2')
cnx = database_connection.db_connection
cursor = database_connection.db_cur


#set inputs
start_time			= calendar.timegm(args.start_time.utctimetuple())
end_time			= calendar.timegm(args.end_time.utctimetuple())
instrument_locn 	= CABM_utilities.getLocationID(args.location)
instr_ID			= CABM_utilities.getInstrID(args.instr_number)


#get data
cursor.execute('''
	SELECT 
		UNIX_UTC_ts_int_start,
		sample_flow, 
		yag_power,
		sheath_flow,
		yag_xtal_temp
	FROM  sp2_hk_data_locn''' + str(instrument_locn) + ''' 
	WHERE 
		instr_ID = %s
		AND (UNIX_UTC_ts_int_start BETWEEN %s AND %s)
		AND id % 300 = %s
		''',
	(instr_ID,start_time,end_time,0))
hk_data = cursor.fetchall()


#plot data
plot_datetime	= [dates.date2num(datetime.utcfromtimestamp(row[0])) for row in hk_data]
sample_flow 	= [row[1] for row in hk_data]
yag_power 		= [row[2] for row in hk_data]
sheath_flow 	= [row[3] for row in hk_data]
yag_xtal_temp 	= [row[4] for row in hk_data]

hfmt = dates.DateFormatter('%Y-%m-%d')

fig = plt.figure(figsize=(12,10))
ax1 = fig.add_subplot(311)
ax1.plot(plot_datetime,yag_power, color = 'r')
ax1.xaxis.set_major_formatter(hfmt)
ax1.axhline(3.75, color = 'r', linestyle = '--')
ax1.axhline(9, color = 'r', linestyle = '--')
ax1.set_ylim(0,10)
ax1.set_ylabel('Yag power (V)')

ax2 = fig.add_subplot(312)
ax2.plot(plot_datetime,sample_flow)
ax2.xaxis.set_major_formatter(hfmt)
ax2.axhline(105, color = 'b', linestyle = '--')
ax2.axhline(135, color = 'b', linestyle = '--')
ax2.set_ylim(90,150)
ax2.set_ylabel('Sample flow (sccm)')

ax3 = fig.add_subplot(313)
ax3.plot(plot_datetime,sheath_flow, color = 'g')
ax3.xaxis.set_major_formatter(hfmt)
ax3.axhline(950, color = 'g', linestyle = '--')
ax3.axhline(1050, color = 'g', linestyle = '--')
ax3.set_ylim(900,1100)
ax3.set_ylabel('Sheath flow (sccm)')

plt.text(0.05, 1.025,'SP2#' + str(args.instr_number) + ' at ' + args.location + ' ' + datetime.utcfromtimestamp(start_time).strftime('%Y/%m/%d') + '-' + datetime.utcfromtimestamp(end_time).strftime('%Y/%m/%d') , fontsize = 14,transform=ax1.transAxes)

plt.show()

