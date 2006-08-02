#!/bin/sh


reldir=`dirname $0`

#export ANT_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=rmiserver"
$ANT_HOME/bin/ant -lib $reldir/../thirdparty/junit/lib/junit.jar "$@"

