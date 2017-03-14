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


data_1h = []
data_1m = []
file = '/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/Alert 20120601 44 compare interval methods.txt'
with open(file, 'r') as f:
	f.readline()
	for line in f:
		newline = line.split()
		extrap_1h 		= float(newline[0])
		unextrap_1h 	= float(newline[1])
		extrap_1min		= float(newline[2])
		unextrap_1min 	= float(newline[3])
		data_1h.append([extrap_1h,unextrap_1h])
		data_1m.append([extrap_1min,unextrap_1min])


extrap_1h_l = [row[0] for row in data_1h]
unextrap_1h_l = [row[1] for row in data_1h]
extrap_1m_l = [row[0] for row in data_1m]
unextrap_1m_l = [row[1] for row in data_1m]

#plotting
fig = plt.figure(figsize=(8,10))

ax1 = plt.subplot2grid((2,1), (0,0))
ax2 = plt.subplot2grid((2,1), (1,0))


ax1.scatter(unextrap_1h_l,extrap_1h_l, color = 'b')
ax1.set_ylabel('mass concencetration (ng/m3) - with extrapolation')
ax1.set_xlabel('mass concencetration (ng/m3) - without extrapolation')
ymin, ymax = ax1.get_ylim()
ax1.set_ylim(0,ymax)
xmin, xmax = ax1.get_xlim()
ax1.set_xlim(0,ymax)
ax1.plot([0,ymax],[0,ymax],color = 'k')
ax1.text(0.75, 0.35,'1 hour intervals', transform=ax1.transAxes)



ax2.scatter(unextrap_1m_l,extrap_1m_l, color = 'g')
ax2.set_ylabel('mass concencetration (ng/m3) - with extrapolation')
ax2.set_xlabel('mass concencetration (ng/m3) - without extrapolation')
ymin, ymax = ax2.get_ylim()
ax2.set_ylim(0,ymax)
xmin, xmax = ax2.get_xlim()
ax2.set_xlim(0,ymax)
ax2.plot([0,ymax],[0,ymax],color = 'k')
ax2.text(0.75, 0.35,'1 minute intervals', transform=ax2.transAxes)


fig.suptitle('Alert SP2#44 20120601', fontsize=14)


plt.savefig('/Users/mcallister/projects/CABM_sp2/docs/PSI_toolkit_results/compare calibration extrapolation over different time intervals.png',bbox_inches = 'tight')


plt.show()



