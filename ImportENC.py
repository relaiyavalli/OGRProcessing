#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 2012-10-29 13:54:20 

###############################################################################
# Copyright (c) 2012 SourceLogix
# Author: Ramesh Elaiyavalli
#
#  The above copyright notice and this permission notice shall be included
#  in all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
#  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.
###############################################################################

# Use this script to import any ENC file collection of format S-57 *.000 files
# 1) ENC.ini drives all settings
# 2) Downloaded and Uncompress S57 data
# 3) Run from anywhere
# 3) If you need to run a subset of files: ImportS57ToDB All_ENC_08022012 US1 (Or US2, US3 etc)
# 3) The Script will recurse through sub-directories and find S-57 files *.000
# 4) Such S-57 files will be passed on to myOgr2Ogr utility
# 5) myOgr2Ogr will import the S-57 layers into the database
# 6) This process usually takes over 1 hour
# ***Need a clean instance of PostGIS database with "geometry_column" table only
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Usage~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# dropdb ramesh && createdb -T template_postgis_20 ramesh
# python ImportENC.py
# start /high pythonw ImportENC.py
# dropdb -p 5433 rameshN && createdb -p5433 -T template_postgis_20 rameshN


# Import essential libraries
import sys, os, re
from time import gmtime, strftime
import time, datetime

# Set appropriate environment variables
os.environ["GDAL"] = "B:\Program Files (x86)\GDAL"
os.environ["PATH"] = os.environ["PATH"] + ";" + os.environ["GDAL"]
os.environ["PGCLIENTENCODING"] = "LATIN1"
os.environ["PG_USE_COPY"] = "YES"
os.environ["GDAL_DATA"] = os.environ["GDAL"] + "\gdal-data"
os.environ["GDAL_DRIVER_PATH"] = os.environ["GDAL"] + "\gdalplugins"
os.environ["PROJ_LIB"] = os.environ["GDAL"] + "\projlib"

#Import GDAL and OGR
from osgeo import gdal, ogr, osr

#Import SourceLogix Files
import Utils
import DBAccess
import myOgr2Ogr

#Read settings from config files
from configobj import ConfigObj
config = ConfigObj("ENC.ini")

#create logger
import logging, logging.config
logging.config.fileConfig('logging.ini')
logger = logging.getLogger('ImportENC')

# Turning off GC improves performance
import gc
gc.disable()



@Utils.entryExit
def Initialize():
	"This is more a placeholder for future use"
	os.environ["PGCLIENTENCODING"] = "LATIN1"
	logger.info("GDAL and OGR Version: " + gdal.__version__)
	#Dump environment variables
	logger.info("\n".join(['%s:: %s' % (key, value) for (key, value) in os.environ.items()]))

@Utils.entryExit
def gatherFilesToProcess(directories, filemasks = []):
	"This function retrieves the files from specified directories and filters them based on filemasks if provided"
	S57Files = []
	if filemasks:
		pattern = re.compile("|".join(filemasks))
	else:
		pattern = re.compile("US*")
	for item in directories:
		for root, dirnames, filenames in os.walk(item):
			for filename in filenames:
				if pattern.match(filename) and filename.endswith(".000"):
					S57Files.append(os.path.join(root, filename))
	logger.info("****Gathered: %s" % len(S57Files) + " Files****" )
	logger.info("\n" + "\n".join(['%s' % item for item in S57Files]))
	return S57Files


@Utils.entryExit
def main():
	"The main method that gets invoked at command line. This method validates command line arguments and calls the ImportS57ToDB method"
	# Gather Start Time
	logger.info("******************** Process Started ***********************************")
	StartTime = time.time()
	FmtStartTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

	#Workflow steps based on settings
	section5 = config['Workflow']

	#Begin import Process passing the options
	Initialize()
	try:
		File = ""
		if "Yes" in section5['downloadFile']:
			File = Utils.downloadFile()

		inputDirectory = ""
		if "Yes" in section5['extract']:
			inputDirectory = Utils.extract(File).split()

		S57Files = []
		if "Yes" in section5['gatherFilesToProcess']:
			section3 = config['S57']
			if not inputDirectory:
				inputDirectory = section3['directory'].split()
			filemasks = section3['filemasks'].split()
			S57Files = gatherFilesToProcess(inputDirectory, filemasks)

		if "Yes" in section5['dropDB']:
			DBAccess.dropDB()

		if "Yes" in section5['createDB']:
			DBAccess.createDB()

		if "Yes" in section5['createAndImportTables']:
			DBAccess.createAndImportTables(S57Files, len(S57Files))

		if "Yes" in section5['prepareDB']:
			DBAccess.prepareDB()

		# Print End and Elapse Time
		ElapsedTime = time.time()- StartTime
		logger.info('Total time taken in HH:MM:SS.ms:  %s', str(datetime.timedelta(seconds=ElapsedTime)))

		# Send email
		if section5['sendemail'] == "Yes":
			logger.info("Sending notification  email")
			message = {'Start DateTime for Processing': FmtStartTime , \
			'End DateTime for Processing': strftime("%Y-%m-%d %H:%M:%S", time.localtime()), \
			'Total Time Taken to Process in HH:MM:SS.ms': str(datetime.timedelta(seconds=ElapsedTime)), \
			'Total Number of files Processed': len(S57Files), \
			'Link to Log file': 'https://srclogix.dlinkddns.com/logs/vic.txt'}
			Utils.noticeEMail(message)
		
		logger.info("******************** Process Finished ***********************************")
		return True
	except Exception, e:
		logger.error(e)
		logger.error("******************** Process Errored ***********************************")
		return False


if __name__=='__main__':
	"Entry point to main from command line"
	main()
