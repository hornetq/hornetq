#!/bin/sh


reldir=`dirname $0`

#export ANT_OPTS="-Xmx1024m -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=ant"
export ANT_OPTS=-Xmx1024m
$ANT_HOME/bin/ant -lib $reldir/../thirdparty/junit/lib/junit.jar "$@"

