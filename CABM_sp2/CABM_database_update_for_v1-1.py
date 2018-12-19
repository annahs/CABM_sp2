#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os
import argparse
import SP2_utilities
import CABM_utilities
from mysql_db_connection import dbConnection


#set arguments
parser = argparse.ArgumentParser(description='''
	This script should be run once to modify the database when updating the CABM_sp2 code from version 1.0 to version 1.1
	This script will add columns "chamber_temp" and "chamber_pressure" to the housekeeping data tables.
	''')
args = parser.parse_args()
db_name = 'CABM_SP2'

#create db connection and cursor
database_connection = dbConnection(db_name)
cnx = database_connection.db_connection
cursor = database_connection.db_cur


def tableExists(location):
	table_name = 'sp2_hk_data_locn' + str(location)
	cursor.execute('''SELECT TABLE_NAME 
		FROM information_schema.tables
		WHERE
		    TABLE_SCHEMA = %s 
		AND TABLE_NAME = %s''',(db_name,'sp2_hk_data_locn' + str(location)))
	table_exists = cursor.fetchall()

	if table_exists == []:
		return False
	else:
		return True


def columnExists(location,column_name):
	cursor.execute('''SELECT * 
		FROM information_schema.COLUMNS 
		WHERE 
		    TABLE_SCHEMA = %s 
		AND TABLE_NAME = %s 
		AND COLUMN_NAME = %s''',(db_name,'sp2_hk_data_locn' + str(location),column_name))
	columns = cursor.fetchall()

	if columns == []:
		return False
	else:
		return True


def addColumn(location, column_name):
	table_name = 'sp2_hk_data_locn' + str(location)
	cursor.execute('''ALTER TABLE ''' + table_name + ''' ADD COLUMN ''' + column_name + ''' double''')




#script
cursor.execute('''SELECT id FROM sp2_locations''')
locations = cursor.fetchall()

for locn in locations:
	location = (locn[0])
	
	if tableExists(location):

		if columnExists(location,'chamber_pressure') == False:
			addColumn(location,'chamber_pressure')
			print 'added chamber_pressure to sp2_hk_data_locn', str(location) 

		if columnExists(location,'chamber_temp') == False:
			addColumn(location,'chamber_temp')
			print 'added chamber_temp to sp2_hk_data_locn', str(location) 

