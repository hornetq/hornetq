@echo off
REM
REM $Id: build.bat 71 2004-10-04 20:13:23Z andd $
REM
set ANT_OPTS=-Xmx1024m
ant -lib ../thirdparty/junit/lib/junit.jar %1 %2 %3 %4 %5 %6 %7 %8 %9

