#!/bin/bash

#
# Script that runs the job server
#

java -classpath lib/eyore-jobserver.jar:lib/eyore-common.jar:lib/hadoop-common-0.21.0.jar:lib/hadoop-hdfs-0.21.0.jar:lib/commons-logging-api-1.1.jar edu.colorado.eyore.jserver.JobServer conf/jserver.properties conf/jserver.log.properties
