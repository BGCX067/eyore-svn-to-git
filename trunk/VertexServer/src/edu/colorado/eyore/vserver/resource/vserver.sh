#!/bin/bash

#
# Script that runs the vertex server process
#

java -classpath lib/eyore-vertexserver.jar:lib/eyore-common.jar:lib/hadoop-common-0.21.0.jar:lib/hadoop-hdfs-0.21.0.jar:lib/commons-logging-api-1.1.jar edu.colorado.eyore.vserver.VertexServer conf/vserver.properties conf/vserver.log.properties
