#!/bin/bash

PATH_TO_CODE_BASE=`pwd`

MAIN_CLASS="ar.edu.itba.pod.client.Query1Client"

java $* $JAVA_OPTS -cp 'lib/jars/*' $MAIN_CLASS