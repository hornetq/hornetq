@echo off

rem Copyright 2009 Red Hat, Inc.
rem Red Hat licenses this file to you under the Apache License, version
rem 2.0 (the "License"); you may not use this file except in compliance
rem with the License.  You may obtain a copy of the License at
rem http://www.apache.org/licenses/LICENSE-2.0
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
rem implied.  See the License for the specific language governing
rem permissions and limitations under the License.  

rem -------------------------------------------------------------------------
rem HornetQ Build Script for Windows
rem -------------------------------------------------------------------------

rem $Id: $

rem This build script will ensure a correct ant version is used.
rem Users can also pass in options on the command line.

rem First check for the existence of JAVA_HOME. It doesn't have to be set, 
rem but it may be an indication that javac is not available on the command line.

if "x%JAVA_HOME%" == "x" (
  echo WARNING: JAVA_HOME is not set. Build may fail.
  echo Set JAVA_HOME to the directory of your local JDK to avoid this message.
) else (
  if exist "%JAVA_HOME%\bin\javac.exe" (
    echo Build script found javac.
) else (
  echo WARNING: javac was not found. Make sure it is available in the path.
))

rem Save off the original ANT_HOME. We don't want to destroy environments.
set "ORIG_ANT_HOME=%ANT_HOME%"

rem Set the new one

if "x%OVERRIDE_ANT_HOME%" == "x" (
  set "ANT_HOME=tools\ant"
) else (
 echo hit alternate
 set "ANT_HOME=%OVERRIDE_ANT_HOME%" 
)

rem build HornetQ

echo Using the following ant version from %ANT_HOME%:

echo calling %ANT_HOME%\bin\ant.bat -version
call %ANT_HOME%\bin\ant.bat -version

echo calling %ANT_HOME%\bin\ant.bat %* -Dhornetq.run_script=true
call %ANT_HOME%\bin\ant.bat %* -Dhornetq.run_script=true

rem Restore the original ANT_HOME values
set "ANT_HOME=%ORIG_ANT_HOME%"

echo Done
