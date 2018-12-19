# -*- coding: UTF-8 -*-
#subclass of SP2 size distribution class for CABM data
#timestamps are in UTC

import sys
import os
import numpy as np
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import math
import matplotlib.pyplot as plt
import calendar
from scipy.optimize import curve_fit
import SP2_utilities
from SP2_time_interval import TimeInterval

class CABMTimeInterval(TimeInterval):
	
	"""

	This is a subclass of TimeInterval with methods modified to suit data from the ECCC CABM sites.

	"""
	
	
	def retrieveHousekeepingLimits(self):
		"""
		Get the housekeeping limits from the database.
		This is subclassed to allow different tables for each CABM location
		"""
		self.db_cur.execute('''
		SELECT 
			yag_min,
			yag_max,
			sample_flow_min,
			sample_flow_max
		FROM
			sp2_hk_limits_locn''' + str(self.instr_location_ID) + ''' 
		WHERE
			instr_ID = %s
			AND instr_location_ID = %s
			AND UNIX_UTC_ts_int_start <= %s
			AND UNIX_UTC_ts_int_end > %s
		LIMIT 1
		''',
		(self.instr_ID, self.instr_location_ID,self.interval_start,self.interval_end))
		hk_limits = self.db_cur.fetchall()

		self.yag_min 		 = hk_limits[0][0]
		self.yag_max 		 = hk_limits[0][1]
		self.sample_flow_min = hk_limits[0][2]
		self.sample_flow_max = hk_limits[0][3]


	def retrieveSingleParticleData(self):
		"""
		Get the single particle data for this interval. Use housekeeping information to exclude periods of poor instrument performance.
		This is subclassed to allow different tables for each location and because SP2 #17 at Whistler has days-long gaps in the housekeeping data, so we can't automatically exclude periods with no hk values.  
		This is also subclassed to exclude periods identified as having QC issues.
		"""
		#whistler case
		if self.instr_location_ID == 5 and self.instr_ID == 1:
			self.db_cur.execute('''
			SELECT 
				sp.UNIX_UTC_ts_int_start,
				sp.UNIX_UTC_ts_int_end,
				sp.BB_incand_HG_pkht,
				sp.BB_incand_LG_pkht,
				hk.sample_flow,
				hk.yag_power,
				sp.NB_incand_HG_pkht,
				hk.chamber_temp,
				hk.chamber_pressure
			FROM
				sp2_single_particle_data_locn''' + str(self.instr_location_ID) + ''' sp
					LEFT JOIN
				sp2_hk_data_locn''' + str(self.instr_location_ID) + ''' hk ON sp.HK_id = hk.id
			WHERE
				sp.UNIX_UTC_ts_int_end BETWEEN %s AND %s
			''',
			(self.interval_start,self.interval_end))
			
			all_particle_data = self.db_cur.fetchall()
			hk_filtered_particle_data = []
			for row in all_particle_data:
				sample_flow = row[4]
				yag_power 	= row[5]
				if sample_flow == None and yag_power == None:
					new_row = (row[0],row[1],row[2],row[3],120,4,row[6])
					hk_filtered_particle_data.append(new_row)
				elif (self.sample_flow_min <= sample_flow < self.sample_flow_max) and (self.yag_min <= yag_power < self.yag_max):
					hk_filtered_particle_data.append(row)

			self.single_particle_data = hk_filtered_particle_data


		#all other cases 
		else:
			self.db_cur.execute('''
			SELECT 
				sp.UNIX_UTC_ts_int_start,
				sp.UNIX_UTC_ts_int_end,
				sp.BB_incand_HG_pkht,
				sp.BB_incand_LG_pkht,
				hk.sample_flow,
				hk.yag_power,
				sp.NB_incand_HG_pkht,
				hk.chamber_temp,
				hk.chamber_pressure
			FROM
				sp2_single_particle_data_locn''' + str(self.instr_location_ID) + ''' sp
					JOIN
				sp2_hk_data_locn''' + str(self.instr_location_ID) + ''' hk ON sp.HK_id = hk.id
			WHERE
				sp.UNIX_UTC_ts_int_end BETWEEN %s AND %s
				AND hk.yag_power BETWEEN %s AND %s
				AND hk.sample_flow BETWEEN %s AND %s
			''',
			(self.interval_start,self.interval_end,self.yag_min,self.yag_max,self.sample_flow_min,self.sample_flow_max))
			
			self.single_particle_data = self.db_cur.fetchall()


	def retrieveCalibrationData(self):
		"""
		Get the relevant calibration data from the database.
		This is subclassed because many of the CABM calibrations were done in the lab, so we can't select them using the location ID but will simply use the most recent one. 
		"""

		calib_time = self.interval_start

		#get data from db
		calibration_data = {}
		for channel in ['BBHG_incand','BBLG_incand']:
			self.db_cur.execute('''
			SELECT 
				0_term,
				1_term,
				2_term,
				0_term_err,
				1_term_err,
				2_term_err,
				calibration_material,
				id	
			FROM
				sp2_calibrations
			WHERE
				instr_ID = %s
				AND calibrated_channel = %s
				AND calibration_date <= %s
				ORDER BY calibration_date DESC LIMIT 1
				
			''',
			(self.instr_ID,channel,calib_time))

			calib_coeffs = self.db_cur.fetchall()
			if calib_coeffs == []:
				calib_coeffs_np = [[np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,'nan',np.nan]]
			else:
				calib_coeffs_np = np.array(calib_coeffs, dtype=[('term0', 'f4'),('term1', 'f4'),('term2', 'f4'),('term0err', 'f4'),('term1err', 'f4'),('term2err', 'f4'),('mat', 'S7'),('ID', 'f4'),])  #converts Nones to nans for calculations

			#Aqudag correction
			for row in calib_coeffs_np:
				calib_material 	= row[6]
				calib_ID 		= row[7]
				calib_0 		= row[0]
				calib_0_err		= row[3]
				if calib_material == 'Aquadag':
					calib_1 	= row[1]/0.7
					calib_1_err = row[4]/0.7
					calib_2 	= row[2]/0.7
					calib_2_err = row[5]/0.7
			
			#set calibration ids		
			if channel == 'BBHG_incand':
				self.HG_calibration_ID = float(calib_ID)
			if channel == 'BBLG_incand':
				self.LG_calibration_ID = float(calib_ID)

			#get the signal limits for calculating mass
			if self.extrapolate_calibration == False:
				pkht_ll, pkht_ul = self._retrieveCalibrationLimits(calib_ID)
			else:
				pkht_ll = self.min_detectable_signal
				pkht_ul = self.saturation_limit
				#an electrical issue on SP2 #17 prior to 2012 gave anomalous signals at masses above ~240nm, this only applies to calibration #1, so we limit the mass range in this case
				if calib_ID == 1:
					pkht_ul = 1410


			calibration_data[channel] = [pkht_ll, pkht_ul, calib_0, calib_1, calib_2, calib_0_err, calib_1_err, calib_2_err]
		
		self.calibration_info = calibration_data	


	def checkForQCInterval(self):
		QC_issues = False
		self.db_cur.execute('''
			SELECT 
				count(*)
			FROM
				sp2_qc_intervals_locn''' + str(self.instr_location_ID) + '''
			WHERE
				((%s < UNIX_UTC_ts_int_end) and (UNIX_UTC_ts_int_start < %s))
				and instr_ID = %s
			''',
			(self.interval_start,self.interval_end,self.instr_ID))
		QC_codes = self.db_cur.fetchall()[0][0]

		if QC_codes > 0:
			QC_issues = True

		return QC_issues




	#database methods specific for the CABM sites

	#for intervals
	def assembleIntervalData(self):
		"""
		Assemble the interval data.  
		This is stored as a dictionary with: the total interval rBC mass, the uncertainty in total rBC mass, rBC particle number, total sampled volume, and a list of the diameters of detected particles.
		This is subclassed to allow for earlier data where the chamber temperature and pressure weren't stored
		"""
		interval_data_dict = {}
		interval_sampled_volume = 0
		interval_mass = 0
		interval_mass_uncertainty = 0
		ved_list = []

		for row in self.single_particle_data:
			ind_start_time 	= row[0] 	#UNIX UTC timestamp
			ind_end_time 	= row[1]	#UNIX UTC timestamp
			BB_incand_HG 	= row[2]  	#in arbitrary units
			BB_incand_LG 	= row[3]  	#in arbitrary units
			sample_flow 	= row[4]  	#in vccm
			chamber_temp 	= row[7] 	#in deg C
			chamber_pressure= row[8]  	#in Pa

			if sample_flow == None: #ignore particles if we can't calculate a volume
				continue
			if (ind_end_time-ind_start_time) > self.interval_max  or (ind_end_time-ind_start_time) < 0:  #ignore particles with a huge sample interval (this arises when the SP2 was set to sample only from 1 of every x minutes)
				continue
			
			#get appropriate sample factor
			sample_factor = self.getParticleSampleFactor(ind_end_time)
			STP_correction_factor = self.getSTPCorrectionFactor(chamber_pressure,chamber_temp) 

			particle_sample_vol =  sample_flow*(ind_end_time-ind_start_time)*STP_correction_factor/(60*sample_factor)   #factor of 60 needed because flow is in sccm and time is in seconds
			interval_sampled_volume += particle_sample_vol

			rBC_mass,rBC_mass_uncertainty = self.calculateMass(BB_incand_HG,BB_incand_LG,ind_end_time)
			VED = SP2_utilities.calculateVED(self.rBC_density,rBC_mass)

			if self.min_VED <= VED <= self.max_VED:  #we limit mass and number concentrations to within the set size limits
				interval_mass += rBC_mass
				interval_mass_uncertainty += rBC_mass_uncertainty
				ved_list.append(VED)
				
		interval_data_dict['VED list'] = ved_list
		interval_data_dict['total mass'] = interval_mass
		interval_data_dict['total number'] = len(ved_list)
		interval_data_dict['total mass uncertainty'] = interval_mass_uncertainty
		interval_data_dict['sampled volume'] = interval_sampled_volume

		self.assembled_interval_data = interval_data_dict


	def getSTPCorrectionFactor(self,chamber_pressure,chamber_temp):
		if chamber_pressure is None:
			chamber_pressure = self.pressure

		if chamber_temp is None:
			chamber_temperature = self.temperature
		else:
			chamber_temperature = chamber_temp + 273.15 	#in deg C -> K


		STP_correction_factor = (chamber_pressure/101325)*(273.15/chamber_temperature) 

		return STP_correction_factor


	def createIntervalInsertStatement(self, table_name):
		
		add_data = ('''INSERT INTO '''+ table_name + '''							  
		  (UNIX_UTC_ts_int_start,
		  UNIX_UTC_ts_int_end,
		  instr_ID,
		  instr_location_ID,
		  calibration_HG_ID,
		  calibration_LG_ID,
		  total_interval_mass,
		  total_interval_mass_uncertainty,
		  total_interval_number,
		  total_interval_volume,
		  fraction_of_mass_sampled,
		  calib_extrapolated)
		  VALUES (
		  %(UNIX_UTC_ts_int_start)s,
		  %(UNIX_UTC_ts_int_end)s,
		  %(instr_ID)s,
		  %(instr_location_ID)s,
		  %(calibration_HG_ID)s,
		  %(calibration_LG_ID)s,
		  %(total_interval_mass)s,
		  %(total_interval_mass_uncertainty)s,
		  %(total_interval_number)s,
		  %(total_interval_volume)s,
		  %(fraction_of_mass_sampled)s,
		  %(calib_extrapolated)s)''')

		return add_data


	def createIntervalRecord(self):
		interval_record ={
		'UNIX_UTC_ts_int_start':self.interval_start,
		'UNIX_UTC_ts_int_end':self.interval_end,
		'instr_ID':self.instr_ID,
		'instr_location_ID':self.instr_location_ID,
		'calibration_HG_ID':self.HG_calibration_ID,
		'calibration_LG_ID':self.LG_calibration_ID,
		'total_interval_mass':float(self.assembled_interval_data['total mass']),
		'total_interval_mass_uncertainty':float(self.assembled_interval_data['total mass uncertainty']),
		'total_interval_number':float(self.assembled_interval_data['total number']),
		'total_interval_volume':float(self.assembled_interval_data['sampled volume']),
		'fraction_of_mass_sampled': 1.,
		'calib_extrapolated': self.extrapolate_calibration,
		}

		return interval_record

	
	def deleteExistingIntervalRecord(self, table_name):

		self.db_cur.execute('''DELETE FROM '''+ table_name + ''' 
			WHERE UNIX_UTC_ts_int_start = %s
			AND UNIX_UTC_ts_int_end = %s
			AND instr_ID = %s
			AND instr_location_ID = %s
			''',
			(self.interval_start,self.interval_end,self.instr_ID,self.instr_location_ID))
		self.db_connection.commit()


	def insertSingleRecord(self, insert_statment,interval_record):
		self.db_cur.execute(insert_statment,interval_record)
		self.db_connection.commit()
		
		interval_id = self.db_cur.lastrowid
		self.interval_db_id = interval_id



	#for distributions
	def createBinnedDataInsertStatement(self, table_name):
		add_data = ('''INSERT INTO '''+ table_name + '''							  
		  (bin_ll,
		  bin_ul,
		  bin_mass,
		  bin_number,
		  interval_ID)
		  VALUES (
		  %(bin_ll)s,
		  %(bin_ul)s,
		  %(bin_mass)s,
		  %(bin_number)s,
		  %(interval_ID)s)''')

		return add_data


	def insertBinnedData(self, insert_statment):
	 
		multiple_records = []
		for point in self.binned_data:	
			single_record ={
			'bin_ll':		float(self.binned_data[point][0]),
			'bin_ul':		float(self.binned_data[point][1]),
			'bin_mass':		float(self.binned_data[point][2]),
			'bin_number':	float(self.binned_data[point][3]),
			'interval_ID':self.interval_db_id
			}
			
			multiple_records.append((single_record))
  
		self.db_cur.executemany(insert_statment, multiple_records)
		self.db_connection.commit()

