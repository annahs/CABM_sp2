# -*- coding: UTF-8 -*-
import sys
import os
import numpy as np
from pprint import pprint
import math
import mysql.connector
from datetime import datetime
import calendar
import SP2_calibrations



calibration_date = datetime(2015,12,1)
calibrated_channel = 'BBLG_incand'
instrument_ID 	 = 3 #SP2_17_id = 1 SP2_44_id = 2 SP2_58_id = 3

#calibration points: mobility diameter,signal
calibration_data = [
[150,520.09	],
[175,1041.6	],
[200,1718	],
[225,2456.4	],
[250,3291.3	],
[269,3944.9	],
[300,5074.7],
[350,7104.2],
[400,9242.2],
[450,11531],
]


#database connection
cnx = mysql.connector.connect(user='root', password='', host='localhost', database='CABM_SP2')
cursor = cnx.cursor()

#database query
add_data = ('''INSERT INTO sp2_calibration_points							  
			  (calibration_ID,
			  mobility_diameter,
			  rBC_mass, 
			  incand_pk_ht
			  )
			  VALUES (
			  %(calibration_ID)s,
			  %(mobility_diameter)s,
			  %(rBC_mass)s,
			  %(incand_pk_ht)s
			  )''')
					


#script
calibration_date_UNIX = calendar.timegm(calibration_date.utctimetuple())
cursor.execute('''
	SELECT 
		id
	FROM
		sp2_calibrations 
	WHERE
		instr_ID = %s
		AND calibration_date = %s
		AND calibrated_channel = %s
	''',
	(instrument_ID,calibration_date_UNIX,calibrated_channel))

calibration_ID = cursor.fetchall()[0][0]

for row in calibration_data:
	mobility_diameter = row[0]
	incand_pk_ht = row[1]
	rBC_mass = SP2_calibrations.calcrBCMass(mobility_diameter)

	single_record ={
	'calibration_ID':calibration_ID,
	'mobility_diameter':mobility_diameter,
	'rBC_mass':rBC_mass,
	'incand_pk_ht':incand_pk_ht 
	}

	cursor.execute(add_data, single_record)
	cnx.commit()

cnx.close()