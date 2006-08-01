#!/bin/sh


reldir=`dirname $0`

$ANT_HOME/bin/ant -lib $reldir/../thirdparty/junit/lib/junit.jar "$@" 

