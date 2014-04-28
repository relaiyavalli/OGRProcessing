OGRProcessing
=============

OGRProcessing is a set of Python utilities to process vector data in the form of S57 files. The input S57 data (*.000) can exist in a target location of your choice. The processor will recurse through the location, pick up all S57 files, create "chunks" to organize them in batches and then execute ogr2ogr in a multi process machine. The ogr2ogr wrapper would read the S57 objects and insert/ update into PostgreSQL DB that is configurable. Sufficient error checking is done. You will be notified by email when job is done.


Here are the highlights...

* Run anyhere S-57 files are downloaded and uncompressed OR 
* Run from anywhere specifying directory name where S57 is, as argument: "ImportENC-Q All_ENC_08022012"
  If you need to run a subset of files: ImportENC-Q All_ENC_08022012 US1 (Or US2, US3 etc)
* The Script will recurse through sub-directories and find S-57 files *.000
* Such S-57 files will be passed on to ogr2ogr utility
* ogr2ogr will import the S-57 layers into the database
* This process usually takes over 3-4 hours for NOAA data. Multi-Processor option will speed this up significantly. For     instance on a 6 core machine, the jobs completed in 36 minutes. Mileage varies based on resources.

Need a clean instance of PostGIS database with "geometry_column" table only

~~Run the batch on command prompt like >>python ImportENC-Q.py -d ..\Vector\ENCData -f US1 >ENC.log 2>&1 && type ENC.log

~~Any questions/ comments - please comment on GitHub. Have fun by being productive!

Cheers,
Ramesh
