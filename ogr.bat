ECHO Setting environment for using GDAL/OGR and Python.
SET ORIGPATH=%PATH%
SET PATH=B:\Program Files (x86)\FWTools2.4.7\bin;%PATH%
SET GDAL_DATA=B:\Program Files (x86)\FWTools2.4.7\data
SET S57_CSV=B:\Program Files (x86)\FWTools2.4.7\data
SET PGCLIENTENCODING=LATIN1

ogr2ogr -update -append -gt "65536" -f "PostgreSQL" PG:"dbname='PyTest' host='127.0.0.1' port='5432' user='postgres' password='postgres'" %1
