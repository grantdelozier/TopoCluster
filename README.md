TopoCluster
===========

TopoCluster uses a geostatistical based approach to describe lexical variation over geography and perform Toponym Resolution.

This application is built with Python 2.7.x in mind. This readme assumes that it has been installed and that the python executable had been added to one's PATH.

Most modes of this application require that you have PostgreSQL 9.x or later installed, along with PostGIS extensions. One should have created a database and added the postgis extension to the database prior to working with this application.

This application also has package dependencies. It is assumed that psycopg2 is installed https://pypi.python.org/pypi/psycopg2

Modes
=====

## Load Database with Document Geographies
```
python TopoCluster.py -mode loadDB -tf /directory/wikipedia_training.data.txt -traintype wiki -dtbl wikipedia_geography -conn "dbname=mydbname user=myusername host='localhost' password=' '" 
```

Argument Explanation:
* -tf (accepts a string that points to a training file, it should have schema like the sample training data provided with this repository)
* -traintype (accepts wiki or twitter as arguments. these datasets have slightly different schema so this is necessary)
* -dtbl (name of the table that will be created in ones DB. upper case letters and spaces should not be used. if the table name already exists in the DB its contents will be erased prior to row insertion)
* -conn (a string corresponding to psycopg2 connection information. In the future this argument will be removed altogether in favor of a .config file with this information)

