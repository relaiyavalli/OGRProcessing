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

# Use this script to call methods that relate to postgres database access


# Import appropriate libraries
import os, sys
import time, datetime
import psycopg2
from osgeo import ogr, gdal

#Import SourceLogix Files
import myOgr2Ogr
import Utils

#Set up logger for this file
import logging, logging.config
logging.config.fileConfig('logging.ini')
logger = logging.getLogger('DBAccess')

#Set up config reader for this file
from configobj import ConfigObj
config = ConfigObj("ENC.ini")

@Utils.entryExit
def createAndImportTables(S57Files, nFileCount = 1, nProcCount=1):
	# Open the source and destination data source
	section2 = config['Database']
	# To formulate a string like this from config file
	#'PG:dbname=rameshN host=127.0.0.1 port=5433 user=postgres password=postgres'
	connectionString = "PG:"
	for k, v in section2.items():
		connectionString += k + "=" + v + " "
	poODS = ogr.Open( connectionString, True )
	gdal.PushErrorHandler('CPLQuietErrorHandler')

	# Process each layer
	for index, item in enumerate(S57Files):
		#Open data source
		s57FileHandle = ogr.Open(S57Files[index], False)

		#Second pass to do the real job */
		nLayerCount = s57FileHandle.GetLayerCount()
		logger.info(str(index+1) + "/" + str(nFileCount) + \
				'\t' "Processing file: " + os.path.basename(item) + \
				" having %s" % nLayerCount + " layers" + \
				' ' + "on CPU: " + str(nProcCount) + "   " + \
				str(os.path.getsize(item)/(1024)) + "KB" )
		for iLayer in range(nLayerCount):
			poLayer = s57FileHandle.GetLayer(iLayer)
			# Create tables
			psInfo = myOgr2Ogr.newSetupTargetLayer( poLayer, poODS)
			poLayer.ResetReading()
			# Import Data
			myOgr2Ogr.newTranslateLayer( psInfo, poODS, poLayer)

		s57FileHandle.Destroy()

	poODS.Destroy()
	return True

@Utils.entryExit
def createDBObjects(S57Files):
	# Open the source and destination data source
	section2 = config['Database']
	# To formulate a string like this from config file
	#'PG:dbname=rameshN host=127.0.0.1 port=5433 user=postgres password=postgres'
	connectionString = "PG:"
	for k, v in section2.items():
		connectionString += k + "=" + v + " "
	poODS = ogr.Open( connectionString, True )
	gdal.PushErrorHandler('CPLQuietErrorHandler')

	# Process each file
	for index, item in enumerate(S57Files):
		#Open data source
		s57FileHandle = ogr.Open(S57Files[index], False)

		#Process the Layer in the File
		#This would create the table(s) for each layer
		nLayerCount = s57FileHandle.GetLayerCount()
		for iLayer in range(nLayerCount):
			poLayer = s57FileHandle.GetLayer(iLayer)
			psInfo = myOgr2Ogr.newSetupTargetLayer( poLayer, poODS)
		s57FileHandle.Destroy()
	poODS.Destroy()

#Not adding decorator as this could cause issues
def importData(S57Files, nProcCount):
	# Open the source and destination data source
	section2 = config['Database']
	# To formulate a string like this from config file
	#'PG:dbname=rameshN host=127.0.0.1 port=5433 user=postgres password=postgres'
	connectionString = "PG:"
	for k, v in section2.items():
		connectionString += k + "=" + v + " "
	poODS = ogr.Open( connectionString, True )
	gdal.PushErrorHandler('CPLQuietErrorHandler')
	
	# Process each File
	for index, item in enumerate(S57Files):
		#Open S57 Source
		s57FileHandle = ogr.Open(S57Files[index], False)
		nLayerCount = s57FileHandle.GetLayerCount()

		logger.info("Processing: " + os.path.basename(item) + \
					" having %s" % nLayerCount + " layers" + \
					" on CPU: %s" % (nProcCount + 1) + \
					" size %s" % (os.path.getsize(item)/(1024)) + "KB")

		#papoLayers = [None for i in range(nLayerCount)]
		#panLayerCountFeatures = [0 for i in range(nLayerCount)]

		#Process Each Layer in Source
		for iLayer in range(nLayerCount):
			poLayer = s57FileHandle.GetLayer(iLayer)
			#Create PSInfo Object
			psInfo = myOgr2Ogr.TargetLayerInfo()
			psInfo.poDstLayer = poODS.GetLayerByName(poLayer.GetName().lower())
			if psInfo.poDstLayer is None:
				logger.error("The following table/ layer is not found: " + poLayer.GetName().lower())
				return False
			poSrcFDefn = poLayer.GetLayerDefn()
			nSrcFieldCount = poSrcFDefn.GetFieldCount()
			psInfo.panMap = [ -1 for i in range(nSrcFieldCount) ]
			poDstFDefn = psInfo.poDstLayer.GetLayerDefn()
			for iField in range(nSrcFieldCount):
				poSrcFieldDefn = poSrcFDefn.GetFieldDefn(iField)
				iDstField = poDstFDefn.GetFieldIndex(poSrcFieldDefn.GetNameRef())
				if iDstField >= 0:
					psInfo.panMap[iField] = iDstField
			poLayer.ResetReading()
			myOgr2Ogr.newTranslateLayer( psInfo, poODS, poLayer)

		s57FileHandle.Destroy()

	poODS.Destroy()


@Utils.entryExit
def prepareDB():
	section2 = config['Database']
	connectionString = ""
	for k, v in section2.items():	
		connectionString += k + "=" + v + " "
	logger.info("Starting database Vacuum on: " + connectionString)
	conn = psycopg2.connect(connectionString)
	conn.set_isolation_level(0)
	cur = conn.cursor()
	cur.execute('VACUUM ANALYZE')
	cur.close()
	conn.close()
	return True

@Utils.entryExit
def createDB ():
	section2 = config['Database']
	dbname = section2['dbname']
	conn = psycopg2.connect("host=" + section2['host'] \
							+ " port=" + section2['port'] \
							+ " user=" + section2['user'] \
							+ " password=" + section2['password'])
	#'host=127.0.0.1 port=5433 user=postgres password=postgres')
	conn.set_isolation_level(0)
	cur = conn.cursor()
	cur.execute("CREATE DATABASE " + '"' + dbname + '"' + " TEMPLATE template_postgis_20 ")
	logger.info("Database Succesfully created DBName: " + dbname)
	cur.close()
	conn.close()

@Utils.entryExit
def dropDB ():
	section2 = config['Database']
	dbname = section2['dbname']
	conn = psycopg2.connect("host=" + section2['host'] \
							+ " port=" + section2['port'] \
							+ " user=" + section2['user'] \
							+ " password=" + section2['password'])
	#'host=127.0.0.1 port=5433 user=postgres password=postgres')
	conn.set_isolation_level(0)
	cur = conn.cursor()
	logger.info("Database to be dropped DBName: " + dbname)
	try:
		cur.execute("DROP DATABASE " + '"' + dbname + '"')
	except psycopg2.ProgrammingError, e:
		logger.error("Error Code: " + e.pgcode)
		logger.error("Error Description: " + e.pgerror)
		pass
	cur.close()
	conn.close()


@Utils.entryExit
def main():
	"""
	The main method that gets invoked at command line.
	"""
	# Gather Start Time

	createDB()
	section3 = config['S57']
	S57Files = []
	S57Files.append(section3['testfile'])
	createAndImportTables(S57Files,len(S57Files))
	prepareDB()
	dropDB()

	return True

if __name__=='__main__':
	"Entry point to main from command line"
	main()
