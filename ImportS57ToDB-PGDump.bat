@ ECHO OFF
REM Use this script to import any ENC file collection of format S-57 *.000 files
REM The script can be run from the parent folder where S-57 files are
REM The Script will recurse through sub-directories and find S-57 files
REM Such S-57 files will be read using ogr2ogr utility
REM The OGR utility will import the S-57 layers into the database
REM ***Need a clean instance of PostGIS database with "geometry_column" table only
REM Already copied OpenCPN CSV's to FWTools area
REM ogr2ogr in FWTools works - For some reason the latest GDAL version gives errors
REM Copyright - SourceLogix Inc.

REM ----------Usage-------------
REM Run the batch on command prompt like >>ImportS57ToDB >vic 2>&1 && type vic --this will output console and error
REM For Each S-57 file - Could take in a parameter like US1 or US2 to run a subset

REM Set appropriate environment variables
ECHO Setting environment for using GDAL/OGR and Python.
SET PGCLIENTENCODING=LATIN1
SET PG_USE_COPY=YES

ECHO.
ECHO **********Starting the import process of S-57 ENC to PostGres at %time%
ECHO.


REM Recurse through all *.000 in directories
FOR /R %1 %%s in (%2*.000) DO (

ECHO Processing %%~ns Size: %%~zs bytes

REM Run the Ogr2ogr import to Postgres DB
ogr2ogr -update -append -gt "65536" -f "PGDump" db.sql %%s
)

ECHO.
ECHO **********Process Finished at %time%
ECHO.




REM -h '127.0.0.1' -p 5432 -U 'postgres' -W 'Post!23'