#!/bin/sh

wget https://s3.amazonaws.com/intellilease-docs/topocluster/enwiki20130102_ner_final_atoi.backup -O data/enwiki20130102_ner_final_atoi.backup
wget https://s3.amazonaws.com/intellilease-docs/topocluster/enwiki20130102_ner_final_jtos.backup -O data/enwiki20130102_ner_final_jtos.backup
wget https://s3.amazonaws.com/intellilease-docs/topocluster/enwiki20130102_ner_final_ttoz.backup -O data/enwiki20130102_ner_final_ttoz.backup
wget https://s3.amazonaws.com/intellilease-docs/topocluster/enwiki20130102_ner_final_other.backup -O data/enwiki20130102_ner_final_other.backup

wget https://s3.amazonaws.com/intellilease-docs/topocluster/geonames_all.backup -O data/geonames_all.backup
wget https://s3.amazonaws.com/intellilease-docs/topocluster/countries_2012.backup -O data/countries_2012.backup
wget https://s3.amazonaws.com/intellilease-docs/topocluster/regions_2012.backup -O data/regions_2012.backup
wget https://s3.amazonaws.com/intellilease-docs/topocluster/states_2012.backup -O data/states_2012.backup
