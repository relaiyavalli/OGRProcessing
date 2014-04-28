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

# This script is included for entry, exit decorations

# Import appropriate libraries
import os, sys, math
import time, datetime
import urllib2, smtplib, zipfile

#Set up config reader for this file
from configobj import ConfigObj
config = ConfigObj("ENC.ini")

#Set up logger for this file
import logging, logging.config
logging.config.fileConfig('logging.ini')
logger = logging.getLogger('Utils')

def entryExit(f):

	def new_f(*args):
		StartTime = time.time()
		logger.info("Function " + f.__name__ + "() " + "Started")
		new_f = f(*args)
		ElapsedTime = time.time()- StartTime
		logger.info("Function " + f.__name__ + \
		"() Completed: Duration HH:MM:SS.ms %s", str(datetime.timedelta(seconds=ElapsedTime)))
		return new_f
	return new_f

@entryExit
def noticeEMail(message = []):
	"""
	The Email method takes the message body as a list, and takes the 
	rest of the email settings from config file.
	"""
	# Initialize SMTP server
	section4 = config['Email']
	server = smtplib.SMTP(section4['smtp'])
	server.starttls()
	server.login(section4['login'], section4['psw'])

	# Send email
	subject = section4['subject']
	usr = section4['usr']
	toaddr = section4['toaddr']
	m = "Date: %s\r\nFrom: %s\r\nTo: %s\r\nSubject: %s\r\nX-Mailer: My-Mail\r\n\r\n" \
		% (time.strftime("%m%d%Y", time.localtime()), usr , toaddr, subject)
	msg = "The batch process succesfully completed"
	# Collate the message list
	msgdetails = ""
	if message:
		msgdetails = "\n".join(['%s:: %s' % (key, value) for (key, value) in message.items()])
	server.sendmail(usr, toaddr, m + "\n" + "\n" + msg + "\n" + msgdetails)
	server.quit()

@entryExit
def downloadFile():
	"""
	
	Helper to download large files
	the only arg is a url
	this file will go to a temp directory
	the file will also be downloaded
	in chunks and print out how much remains
	
	"""
	# Create the destination file name and path
	section1 = config['Download']
	url = section1['URL']
	logger.info("Source File: %s" % url)
	downloadPath = section1['DownloadPath']
	baseFileName = os.path.basename(url)
	baseName, extension = os.path.splitext(baseFileName)
	targetFileName = downloadPath + baseName + "_" + time.strftime("%m%d%Y", time.localtime()) + extension
	logger.info("Destination File: %s" % targetFileName)

	try:
		req = urllib2.urlopen(url)
		total_size = int(req.info().getheader('Content-Length').strip())
		logger.info ("Downloading started -- File Size: %s" % (total_size/(1024*1024)) + "MB")
		downloaded = 0
		CHUNK = total_size/100
		with open(targetFileName, 'wb') as fp:
			while True:
				chunk = req.read(CHUNK)
				downloaded += len(chunk)
				logger.info("Downloaded " + \
						str(int(math.ceil((float(downloaded) / float(total_size)) * 100))) + \
						"%")
				if not chunk: break
				fp.write(chunk)
	except urllib2.HTTPError, e:
		print "HTTP Error:",e.code , url
		return False
	except urllib2.URLError, e:
		print "URL Error:",e.reason , url
		return False
	return targetFileName

@entryExit
def extract(targetFileName, a=0):
	logger.info("Extract File: %s" % targetFileName)
	
	# Get the File to UnZip
	section1 = config['Download']
	downloadPath = section1['DownloadPath']
	dirName, fileExtension = os.path.splitext(targetFileName)

	# Unzip the file to destination
	zip = zipfile.ZipFile(targetFileName)
	zip.extractall(dirName)
	logger.info("File Extraction Completed")
	zip.close()
	

	# Delete the now unnessary zip file
	logger.info ("Deleting File: %s" % targetFileName)
	os.remove(targetFileName)
	return dirName


@entryExit
def main():
	"""
	The main method that gets invoked at command line.
	"""
	targetFileName = downloadFile()
	extract(targetFileName)


	section4 = config['Email']
	message = {'Start DateTime for Processing': '10AM', \
		   'End DateTime for Processing': '10PM', \
		   'Total Time Taken to Process in HH:MM:SS.ms': '12HR', \
		   'Total Number of files Processed': 900, \
		   'Link to Log file': 'https://srclogix.dlinkddns.com/logs/vic.txt'}
	noticeEMail(message)
	return True

if __name__=='__main__':
	"Entry point to main from command line"
	main()