#!/bin/sh

export OVERRIDE_ANT_HOME=../../../tools/ant

if [ -f "../../../src/bin/build.sh" ]; then
   # running from TRUNK
   ../../../src/bin/build.sh "$@"
else
   # running from the distro
   ../../../bin/build.sh "$@"
fi



