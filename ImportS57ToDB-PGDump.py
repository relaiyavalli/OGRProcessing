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
# 1) Run anywhere S-57 files are downloaded & uncompressed OR 
# 2) Run from anywhere specifying directory name(s) where S57 is, as argument: ImportS57ToDB All_ENC_08022012
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
# Run the batch on command prompt like >>pythonw ImportS57ToDB.py >vic 2>&1 && type vic 
# Pythonw will run in background and the text output/ errors will be writted vic
# For Each S-57 file - Could take in a parameter like US1 or US2 to run a subset

# Import appropriate libraries
import os
import sys
import time, datetime
import logging
import fnmatch
from time import gmtime, strftime
import subprocess
import argparse
import re
import smtplib
import math
import myOgr2Ogr
logging.basicConfig(filename='example.log',level=logging.DEBUG)
StartTime = time.time()
import EmailNotification

def Initialize():
	"This function sets up the environment for processing S57 files"
	# Set appropriate environment variables
	#print("Setting environment for using GDAL/OGR and Python.") 
	#OrigPath = os.environ["PATH"]
	#os.environ["PATH"] = "B:\Program Files (x86)\FWTools2.4.7\bin;" + os.environ["PATH"]
	#os.environ["GDAL_DATA"] = "B:\Program Files (x86)\FWTools2.4.7\data"
	#os.environ["S57_CSV"] = "B:\Program Files (x86)\FWTools2.4.7\data"
	#os.environ["PGCLIENTENCODING"] = "LATIN1"
	
	# Print Begin Time
	print ("\n **********Starting the import process of S-57 ENC to PostGres:" \
			+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "\n")
	
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
	return S57Files


def ImportS57ToDB(directories, filemasks) :
	"This function serves as the workflow to coordinate all import related activities"
	Initialize()
	S57Files = GatherFilesToProcess(directories, filemasks)
	filecount = len(S57Files)

	for index, item in enumerate(S57Files):
		print str(index+1) + "/" + str(filecount) + '\t' "Processing file: " + os.path.basename(item) + '\t' + str(os.path.getsize(item)/(1024)) + "KB"
		myOgr2Ogr.main(['myOgr2Ogr ', '--config', 'PG_USE_COPY YES', '-f', "PGDump", 'abc.sql', S57Files[0]])
	

def main():
	"The main method that gets invoked at command line. This method receives and validates command line arguments"
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
			raise argparse.ArgumentTypeError("%s is not a valid directory" %item)	
	
	#Check if the given filemask(s) are valid
	validlist = ['US1', 'US2', 'US3', 'US4', 'US5', 'US6']
	if args.filemask:		
		for item in args.filemask:
			if item not in validlist:
				raise argparse.ArgumentTypeError("%s is not a valid filemask" % item)	
	
	#Begin import Process passing the options
	ImportS57ToDB(args.directory, args.filemask)

	# Print End  Time
	print ("\n **********Finishing the import process at: " \
			+ strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "\n")

	# Print Elapsed Time
	ElapsedTime = time.time()- StartTime
	print "**************Total time taken in HH:MM:SS: ", datetime.timedelta(seconds=ElapsedTime)
	
	#send notification Email
	#noticeEMail('gowadzee', 'cloudmon123', 'gowadzee@gmail.com', 'ramesh@srclogix.com')

if __name__=='__main__':
	"Entry point to main from command line"
	main()

# To Do
# Fix the default directory issue
# Change to use plain patterns and not regex
# Do multi - processing
# Get FileMask as constant
# Get Email Notification
# Progress Bar
# Logging