TopoCluster
===========

TopoCluster uses a geostatistical based approach to describe lexical variation over geography and perform Toponym Resolution.

This application is built with Python 2.7.x in mind. This readme assumes that it has been installed and that the python executable had been added to one's PATH.

Most modes of this application require that you have PostgreSQL 9.x or later installed, along with PostGIS extensions. One should have created a database and added the postgis extension to the database prior to working with this application.

This application also has package dependencies. It is assumed that psycopg2 is installed https://pypi.python.org/pypi/psycopg2

Modes
=====

### Load Database with Document Geographies

This TopoCluster mode takes document training files and outputs a table with point geographies in PostgreSQL. Right now these scripts assume that the lat/long contained in the training files is in an unprojected WGS 84 Datum.

```
python TopoCluster.py -mode loadDB -tf /directory/wikipedia_training.data.txt -traintype wiki -dtbl wikipedia_geography -conn "dbname=mydbname user=myusername host='localhost' password=' '" 
```

Argument Explanation:
* -tf (accepts a string that points to a training file, it should have schema like the sample training data provided with this repository)
* -traintype (accepts wiki or twitter as arguments. these datasets have slightly different schema so this is necessary)
* -dtbl (name of the table that will be created in ones DB. upper case letters and spaces should not be used. if the table name already exists in the DB its contents will be erased prior to row insertion)
* -conn (a string corresponding to psycopg2 connection information. In the future this argument will be removed altogether in favor of a .config file with this information)

### Calculate Moran's Coefficient

Use of this mode requires installation of Python's Numpy/Scipy packages.

```
python TopoCluster.py 
-mode morans_calc 
-tf /directory/wikipedia_training.data.txt 
-traintype wiki 
-dtbl wikipedia_geography
-gtbl globalgrid_5_clip
-conn "dbname=mydbname user=myusername host='localhost' password=' '"
-agg_dist 40000
-kern_dist 90000
-kern_type uniform
-write_agg_lm False
-grid_freq_min 5
-outf MoransScores_Uniform_40kmAgg_90kmNeighbor_wikipedia_training.data.txt
-cores 2
```

Argument Explanation:
* -tf (accepts a string that points to a training file, it should have schema like the sample training data provided with this repository)
* -traintype (accepts wiki or twitter as arguments. these datasets have slightly different schema so this is necessary)
* -dtbl (name of the table containing -tf document geographies)
* -gtbl (name of the grid table from which aggregations will be made)
* -conn (a string corresponding to psycopg2 connection information. In the future this argument will be removed altogether in favor of a .config file with this information)
* -agg_dist (the distance (in meters) around a grid point from which documents will be aggregated.)
* -kern_dist (the distance (in meters) that defines the bandwidth of the kernel function)
* -kern_type (type of kernel function to be used for target to neighbor comparisons. Currently supports only uniform for Moran's Calculations)
* -write_agg_lm (if set to True it will write the aggregated document language models to a file.)
* -grid_freq_min (a minimum number of different aggregated documents a word must be inside before its Moran's Coef is bothered to be calculated)
* -outf (a text file that moran's coeficients will be written to)
* -cores (the number of cores to utilize for Moran's calculations. The moran's calculations are capable of utilizing Python multiprocessing. Appropriate use of the cores argument requires some knowledge of the base memory consumption of the process on your training data. Generally the bottlenecks are in memory and not in the number of cpus.)


