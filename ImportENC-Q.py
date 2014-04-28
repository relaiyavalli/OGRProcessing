#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

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
# 1) Run anyhere S-57 files are downloaded and uncompressed OR 
# 2) Run from anywhere specifying directory name where S57 is, as argument: ImportS57ToDB All_ENC_08022012
# 3) If you need to run a subset of files: ImportS57ToDB All_ENC_08022012 US1 (Or US2, US3 etc)
# 3) The Script will recurse through sub-directories and find S-57 files *.000
# 4) Such S-57 files will be passed on to ogr2ogr utility
# 5) ogr2ogr will import the S-57 layers into the database
# 6) This process usually takes over 3 hours
# ***Need a clean instance of PostGIS database with "geometry_column" table only
# ***Already copied OpenCPN CSV's to FWTools area
# ***ogr2ogr in FWTools works - For some reason the latest GDAL version gives errors
#
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Usage~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Run the batch on command prompt like >>python ImportENC.py -d ..\Vector\ENCData -f US1 >vic 2>&1 && type vic
# For Each S-57 file - Could take in a parameter like US1 or US2 to run a subset or no option will run full
# dropdb ramesh && createdb -T template_postgis_20 ramesh
# python ImportENC-Q.py -d ..\VectorData\All_ENC_10292012
# dropdb -p 5433 rameshN && createdb -p5433 -T template_postgis_20 rameshN

# Import appropriate libraries
import sys
import os
from time import gmtime, strftime
import time, datetime
import argparse
import re

from osgeo import gdal, ogr, osr
import osgeo.gdal
import myOgr2Ogr

import EmailNotification

#Could improve performance disabling garbage collection
import gc
gc.disable()


#create logger
import logging, logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('ImportENC')

#MultiProcessing
from multiprocessing import Process, Queue
import multiprocessing

# File Level Variables
filecount=0
#nProcs = multiprocessing.cpu_count()
nProcs = 2
pszDestDataSource = 'PG:dbname=rameshN host=127.0.0.1 port=5433 user=postgres password=postgres'


def Initialize():
	"This function sets up the environment for processing S57 files"
	# Set appropriate environment variables
	#print("Setting environment for using GDAL/OGR and Python.") 
	#OrigPath = os.environ["PATH"]
	#os.environ["PATH"] = "B:\Program Files (x86)\FWTools2.4.7\bin;" + os.environ["PATH"]
	#os.environ["GDAL_DATA"] = "B:\Program Files (x86)\FWTools2.4.7\data"
	#os.environ["S57_CSV"] = "B:\Program Files (x86)\FWTools2.4.7\data"
	os.environ["PGCLIENTENCODING"] = "LATIN1"
	logger.info("GDAL and OGR Version: " + osgeo.gdal.__version__)


def GatherFilesToProcess(directories, filemasks):
	"This function retrieves the files from all specified directories and filters them based on filemasks if provided"
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
	# Chunk up the files for multiprocessing
	# Chunk them up into roughly equal sizes for each processor
	filecount = len(S57Files)
	chunk = filecount/nProcs
	ChunkedS57Files = [S57Files[i:i+chunk] for i in range(0, filecount, chunk)]
	#print "\n".join(item[1] for item in ChunkedS57Files)
	#print ChunkedS57Files
	return ChunkedS57Files

def ImportS57ToDB(directories, filemasks):
	"This function serves as the workflow to coordinate all import related activities"
	Initialize()
	ChunkedS57Files = GatherFilesToProcess(directories, filemasks)

	#Create the database schema
	logger.info("Creating S57 database -- started")
	#Flatten the individual lists
	AllS57Files = sum(ChunkedS57Files, [])
	# Create the schema objects - tables, columns
	CreateDB(AllS57Files, filecount)
	logger.info("S57 database creation finished")
	# Single processor data loader
	if (nProcs < 2):
		ImportToDB(AllS57Files, nProcs, filecount)
		return filecount

	# Multi processor data loader
	jobs = []
	for i in range(nProcs):
		queue = Queue()
		process = multiprocessing.Process(target=ImportToDB, args=(ChunkedS57Files[i],i,filecount))
		jobs.append(process)
		process.start()
	for job in jobs:
		job.join()

	return filecount


def CreateDB(S57Files, nFileCount):
	# Open the source and destination data source
	poODS = ogr.Open( pszDestDataSource, True )

	# Process each file
	for index, item in enumerate(S57Files):
		#Open data source
		poDS = ogr.Open(S57Files[index], False)

		#Process the Layer in the File
		#This would create the table(s) for each layer
		nLayerCount = poDS.GetLayerCount()
		for iLayer in range(nLayerCount):
			poLayer = poDS.GetLayer(iLayer)
			psInfo = myOgr2Ogr.SetupTargetLayer( poDS, poLayer, poODS)
		poDS.Destroy()
	poODS.Destroy()

def ImportToDB(S57Files, nProcCount, nFileCount):
	# Open the source and destination data source
	pszDestDataSource = 'PG:dbname=rameshN host=127.0.0.1 port=5433 user=postgres password=postgres'
	poODS = ogr.Open( pszDestDataSource, True )
	gdal.PushErrorHandler('CPLQuietErrorHandler')
	
	# Process each layer
	for index, item in enumerate(S57Files):
		# logger.info("Processing file: " + os.path.basename(item) + \
				# '\t' + "running on CPU: " + str(nProcCount) + "   " + \
				# str(os.path.getsize(item)/(1024)) + "KB")
		#Open data source
		poDS = ogr.Open(S57Files[index], False)
		nLayerCount = poDS.GetLayerCount()
		#papoLayers = [None for i in range(nLayerCount)]
		panLayerCountFeatures = [0 for i in range(nLayerCount)]

		#Second pass to do the real job */
		for iLayer in range(nLayerCount):
			poLayer = poDS.GetLayer(iLayer)
			#Create PSInfo Object
			psInfo = myOgr2Ogr.TargetLayerInfo()
			psInfo.poDstLayer = poODS.GetLayerByName(poLayer.GetName().lower())
			if psInfo.poDstLayer is None:
				logger.error("The following table/ layer is not found: " + poLayer.GetName().lower())
				return False
			poSrcFDefn = poLayer.GetLayerDefn()
			nSrcFieldCount = poSrcFDefn.GetFieldCount()
			panMap = [ -1 for i in range(nSrcFieldCount) ]
			psInfo.panMap = panMap
			psInfo.iSrcZField = -1

			poLayer.ResetReading()
			if (psInfo is None or \
				not myOgr2Ogr.newTranslateLayer( psInfo, poDS, poODS, poLayer) \
				and not bSkipFailures):
				logger.error("Terminating translation prematurely after failed\n" + \
					"translation of layer " + poLayer.GetLayerDefn().GetName() + " (use -skipfailures to skip errors)")

				return False

		poDS.Destroy()

	poODS.Destroy()

def main():
	"The main method that gets invoked at command line. This method validates command line arguments and calls the ImportS57ToDB method"
	# Gather Start Time
	logger.info("******************** Process Started ***********************************")
	StartTime = time.time()
	FmtStartTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

	# Parse command line arguments
	logger.info("Validating command line arguments")
	parser = argparse.ArgumentParser(
		description='Identifies S57 files by recursing through directories and imports them into Postgres .')
	parser.add_argument('-d', '--directory', nargs='+',
		help='specify one or more top level directories that contain S57 data', required=False)
	parser.add_argument('-f', '--filemask', nargs='+', 
		help='specify one or more file masks to filter for importing', required=False)
	args = parser.parse_args()

	#Check if the given directory (or directories) are valid
	if not args.directory:
		raise argparse.ArgumentTypeError("Please supply a valid directory")
	for item in args.directory:
		if not os.path.isdir(item):
			logger.error("%s is not a valid directory" %item)
			raise argparse.ArgumentTypeError("%s is not a valid directory" %item)	
	#Check if the given filemask(s) are valid
	validlist = ['US1', 'US2', 'US3', 'US4', 'US5', 'US6']
	if args.filemask:
		for item in args.filemask:
			if item not in validlist:
				logger.error("%s is not a valid filemask" %item)
				raise argparse.ArgumentTypeError("%s is not a valid filemask" % item)	

	#Begin import Process passing the options
	logger.info("Starting the import process")
	filecount = ImportS57ToDB(args.directory, args.filemask)

	# Print End and Elapse Time
	logger.info("Finished import process")
	ElapsedTime = time.time()- StartTime
	logger.info('Total time taken in HH:MM:SS.ms:  %s', str(datetime.timedelta(seconds=ElapsedTime)))
	# Send email
	logger.info("Sending notification  email")
	message = {'Start DateTime for Processing': FmtStartTime , \
			   'End DateTime for Processing': strftime("%Y-%m-%d %H:%M:%S", time.localtime()), \
			   'Total Time Taken to Process in HH:MM:SS.ms': str(datetime.timedelta(seconds=ElapsedTime)), \
			   'Total Number of files Processed': filecount, \
			   'Link to Log file': 'https://srclogix.dlinkddns.com/logs/vic.txt'}
	EmailNotification.noticeEMail(message)
	
	logger.info("******************** Process Finished ***********************************")


if __name__=='__main__':
	"Entry point to main from command line"
	main()
import cProfile
cProfile.run('main()')
# To Do
# Fix the default directory issue
# Change to use wildcard patterns - US1* US1G* *M etc
# Explore possibility of multi - processing
# Progress Bar
# Logging
# Why os.environ does not work?
# Create module/ package
# CM
# Paralell processing