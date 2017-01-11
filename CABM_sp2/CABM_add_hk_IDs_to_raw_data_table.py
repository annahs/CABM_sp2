import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import mysql.connector
import math
import calendar
import SP2_housekeeping

Alert = 1
ETL = 2
Egbert = 3
Resolute = 4
Whistler = 5

instrument_locn = Whistler
database_name = 'CABM_sp2'

start = datetime(2009,6,1)
end =   datetime(2010,9,1)
UNIX_start = calendar.timegm(start.utctimetuple())
UNIX_end = calendar.timegm(end.utctimetuple())


timestep = 5. #days
while start <= end:
	
	print start

	UNIX_start = calendar.timegm(start.utctimetuple())
	UNIX_end = UNIX_start + 86400*timestep


	parameters = {
	'UNIX_start':UNIX_start,
	'UNIX_end':UNIX_end,
	'hk_table':  'sp2_hk_data_locn' + str(instrument_locn),
	'raw_data_table': 'sp2_single_particle_data_locn' + str(instrument_locn),
	'database_name': database_name,
	}

	SP2_housekeeping.addHKKeysToRawDataTable(parameters)

	start += timedelta(days = timestep)