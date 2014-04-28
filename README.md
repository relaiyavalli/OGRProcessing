OGRProcessing
=============

OGRProcessing is a set of utilities to process vector data in the form of S57 files. The input S57 data (*.000) can exist in a target location of your choice. The processor will recurse through the location, pick up all S57 files, create "chunks" to organize them in batches and then execute ogr2ogr in a multi process machine. The ogr2ogr wrapper would read the S57 objects and insert/ update into PostgreSQL DB that is configurable. Sufficient error checking is done. The process would notify by email when the job is done.



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
