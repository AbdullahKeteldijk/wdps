#!/usr/bin/env bash

ATT=${1:-"WARC-Record-ID"}
INFILE=${2:-"hdfs:///user/bbkruit/sample.warc.gz"}
STANFORD=${3:-"C:\Users\ruben\Documents\WDPS_2018\stanford-ner-2018-10-16.zip"}

PYSPARK_PYTHON=/home/wdps1813/bin/python ~/spark-2.1.2-bin-without-hadoop/bin/spark-submit --master yarn mainfile.py $ATT $INFILE $STANFORD
