@echo off

set "OVERRIDE_ANT_HOME=../../../tools/ant"

if exist "../../../src/bin/build.sh" (
   rem running from TRUNK
   call ../../../src/bin/build.sh %*
) else (
   rem running from the distro
   call ../../../bin/build.sh %*
)

