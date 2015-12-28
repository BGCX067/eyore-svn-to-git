#!/bin/bash

#
# Script that executes the client
# 
# ARG 1) file path of job jar on local file system
# ARG 2) path to HDFS input directory for job - gets written into JobSpecification for job
# ARG 3) path to HDFS output directory for job - gets written into JobSpecification for job
#

JOB_JAR=$1
HDFS_INPUT_DIR=$2
HDFS_OUTPUT_DIR=$3

java -classpath lib/eyore-client.jar:lib/eyore-common.jar:lib/hadoop-common-0.21.0.jar:lib/hadoop-hdfs-0.21.0.jar:lib/commons-logging-api-1.1.jar edu.colorado.eyore.client.Client "$JOB_JAR" "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR" conf/client.properties conf/client.log.properties
