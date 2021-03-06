TopoCluster
===========

TopoCluster uses a geostatistical based approach to describe lexical variation over geography and perform Toponym Resolution.

Installation
============

This application is built with Python 2.7.x in mind. This readme assumes that it has been installed and that the python executable had been added to one's PATH.

Most modes of this application require that you have PostgreSQL 9.x or later installed, along with PostGIS extensions. One should have created a database and added the postgis extension to the database prior to working with this application. Examples of how to do the installation and setup on a Ubuntu system is demonstrated below.

Setting up the files necessary to complete installation takes about 30 GBs of hard drive space.

### Ubuntu Postgres Intallation


```
sudo apt-get install postgresql-9.3
sudo apt-get install postgresql-9.3-postgis-2.1
sudo -u postgres psql postgres
\password postgres
(Enter non blank password)
create database testdb;
\connect testdb
CREATE EXTENSION postgis;
\q
```

This application also has package dependencies. It is assumed that psycopg2 is installed (https://pypi.python.org/pypi/psycopg2). Some modes require additional packages (e.g. the morans calculation mode requires Numpy and significance testing in this mode requires Scipy).

### Ubuntu Psycopg2 Installation 
```
sudo apt-get install python-psycopg2

```

###Create Global Grid Tables

Navigate to the data folder inside the TopoCluster main directory. Extract the tar files
```
tar -xzvf globalgrid_5_clip.txt.tar.gz

```
Create the Global Grid table from the text file
```
python TopoCluster.py -mode loadDB \ 
-tf data/globalgrid_5_clip.txt \
-traintype wiki \
-dtbl globalgrid_5_clip_geog \
-conn "dbname=testdb user=postgres host='localhost' password=' '" 
```

### Download Geowikipedia Gi* Statistics and Gazetteer tables
Navigate to TopoCluster main directory. Run the shell script to begin downloads of tables.
```
bash download_gazets.sh
```
This downloads about 20 GB worth of files and could take a while.


### Restore Gazetteer Tables to DB
```
pg_restore --host localhost --port 5432 --username postgres --dbname testdb --schema public --verbose countries_2012.backup
pg_restore --host localhost --port 5432 --username postgres --dbname testdb --schema public --verbose geonames_all.backup
pg_restore --host localhost --port 5432 --username postgres --dbname testdb --schema public --verbose regions_2012.backup
pg_restore --host localhost --port 5432 --username postgres --dbname testdb --schema public --verbose states_2012.backup
```

### Restore Gi* Statistics Tables to DB
```
pg_restore --host localhost --port 5432 --username postgres --dbname testdb --schema public --verbose enwiki20130102_ner_final_other.backup
pg_restore --host localhost --port 5432 --username postgres --dbname testdb --schema public --verbose enwiki20130102_ner_final_ttoz.backup
pg_restore --host localhost --port 5432 --username postgres --dbname testdb --schema public --verbose enwiki20130102_ner_final_jtos.backup
pg_restore --host localhost --port 5432 --username postgres --dbname testdb --schema public --verbose enwiki20130102_ner_final_atoi.backup
```
Some of these tables are very large and include indexes. Restoring them could take several hours.

Documentation of all modes and arguments is still somewhat incomplete. Users are encouraged to view the project's main class (TopoCluster.py) for information on all modes and options.

Modes
=====

### Load Database with Document Geographies

This TopoCluster mode takes document or grid files and outputs a table with point geographies in PostgreSQL. Right now these scripts assume that the lat/long contained in the training files is in an unprojected WGS 84 Datum.

Load Document Geographies

```
python TopoCluster.py 
-mode loadDB 
-tf data/wikipedia_sample.txt 
-traintype wiki 
-dtbl wikipedia_sample_geography 
-conn "dbname=mydbname user=myusername host='localhost' password='pass'" 
```

Load Grid Table (if not already done)

```
python TopoCluster.py -mode loadDB \ 
-tf data/globalgrid_5_clip.txt \
-traintype wiki \
-dtbl globalgrid_5_clip_geog \
-conn "dbname=mydbname user=myusername host='localhost' password=' '" 
```

Argument Explanation:
* -tf (accepts a string that points to a training file, it should have schema like the sample training data provided with this repository)
* -traintype (accepts wiki or twitter as arguments. these datasets have slightly different schema so this is necessary)
* -dtbl (name of the table that will be created in ones DB. upper case letters and spaces should not be used. if the table name already exists in the DB its contents will be erased prior to row insertion)
* -conn (a string corresponding to psycopg2 connection information. In the future this argument will be removed altogether in favor of a .config file with this information)

### Calculate Moran's Coefficient

Use of this mode requires installation of Python's Numpy/Scipy packages.

(Remove returns between arguments)
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

### Calculate Local Spatial Statistics

Use of this mode requires Numpy to be installed. This mode takes in a document training file and outputs local spatial statistics to a PostgreSQL table.

(Remove returns between arguments)
```
python TopoCluster.py 
-mode morans_calc 
-tf /directory/wikipedia_training.data.txt 
-traintype wiki 
-dtbl wikipedia_geography
-gtbl globalgrid_5_clip
-conn "dbname=mydbname user=myusername host='localhost' password=' '"
-kern_dist 100000
-kern_type epanech
-include_zero False
-statistic gi
-out_tbl wikipedia_training_epanech100km
-cores 2
```

Argument Explanation:
* -tf (accepts a string that points to a training file, it should have schema like the sample training data provided with this repository)
* -traintype (accepts wiki or twitter as arguments. these datasets have slightly different schema so this is necessary)
* -dtbl (name of the table containing -tf document geographies)
* -gtbl (name of the grid table from which aggregations will be made)
* -conn (a string corresponding to psycopg2 connection information. In the future this argument will be removed altogether in favor of a .config file with this information)
* -kern_dist (the distance (in meters) that defines the bandwidth of the kernel function)
* -kern_type (type of kernel function to use for weighting of local statistic calculations. Currently excepts uniform or epanech as arguments)
* -include_zero (accepts True or False as arguments. True will write local stat values of 0 to the table. False will only write values > 0. It is strongly recommended to leave this set as False.)
* -statistic (type of local spatial statistic to calculate. Currently accepts zavg and gi as arguments)
* -out_tbl (table to which local statistic results will be written to. If a table already exists then it will have its previous contents deleted prior to insertion. Right now there is a limit to the character length of words that can be inserted (50 charvar).)
* -cores (the number of cores to utilize for local statistic calculations. The local statistic calculations are capable of utilizing Python multiprocessing. Appropriate use of the cores argument requires some knowledge of the base memory consumption of the process on your training data. Generally the bottlenecks are in memory and not in the number of cpus. It is recommended to leave 1/2 the number of cores open so that they can be devoted to postgreSQL processes that will be spawned.)

### Resolve Toponyms (Plain Text with NER)

This mode will Stanford NER parse all plain text files in a directory and perform a disambiguation based on Gi* Vectors derived from the GeoWikipedia Corpus.

Example command:
```
python TopoCluster.py -mode plain_topo_resolve -outdomain_stat_tbl enwiki20130102_train_kernel100k_grid5_epanech_allwords_ner_fina -tstf /home/yourUser/plaintextfiles -conn "dbname=mydbname user=myusername host='localhost' password=' '" -gtbl globalgrid_5_clip_geog -percentile 1.0 -window 15 -main_topo_weight 40.0 -other_topo_weight 5.0 -other_word_weight 1.0 -country_tbl countries_2012 -region_tbl regions_2012 -state_tbl states_2012 -geonames_tbl geonames_all -out_domain_lambda 1.0 -stan_path /home/yourUser/etc/TopoCluster/stanford-ner-path-containing-jar -results_file /home/yourUser/plaintextfiles_results.txt
```

### Test Resolver (Tr-CoNLL xml format, gold NER)

Tests Topocluster against lat/longs found in xml file. Uses gold NER labels. 

```
python TopoCluster.py -mode xml_topo_resolve -tstf /home/yourUsers/xml_directory -conn "dbname=mydbname user=myusername host='localhost' password=' '" -gtbl globalgrid_5_clip_geog -percentile 1.0 -window 15 -main_topo_weight 40.0 -other_topo_weight 5.0 -other_word_weight 0.5 -country_tbl countries_2012 -region_tbl regions_2012 -state_tbl states_2012 -geonames_tbl geonames_all -out_domain_lambda 1.0 -results_file /home/yourUser/results.txt
```

This same mode can be used to test TopoCluster against your own datasets. The format of the xml should be the same as the the file found in data/wotr-topo-train-106-170.xml