@echo off

setlocal

if NOT "%HORNETQ_HOME%"=="" goto CHECK_HORNETQ_HOME
PUSHD .
CD %~dp0..
set HORNETQ_HOME=%CD%
POPD

:CHECK_HORNETQ_HOME
if exist "%HORNETQ_HOME%\bin\hornetq.cmd" goto CHECK_JAVA

:NO_HOME
echo HORNETQ_HOME environment variable is set incorrectly. Please set HORNETQ_HOME.
goto END

:CHECK_JAVA
set _JAVACMD=%JAVACMD%

if "%JAVA_HOME%" == "" goto NO_JAVA_HOME
if not exist "%JAVA_HOME%\bin\java.exe" goto NO_JAVA_HOME
if "%_JAVACMD%" == "" set _JAVACMD=%JAVA_HOME%\bin\java.exe
goto RUN_JAVA

:NO_JAVA_HOME
if "%_JAVACMD%" == "" set _JAVACMD=java.exe
echo.
echo Warning: JAVA_HOME environment variable is not set.
echo.

:RUN_JAVA

for /R %HORNETQ_HOME%\lib %%A in (*.jar) do (
  set CLASSPATH=!CLASSPATH!;%%A
)

if "%JVM_FLAGS%" == "" set JVM_FLAGS=-XX:+UseParallelGC  -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms512M -Xmx1024M -Djava.util.logging.manager=org.jboss.logmanager.LogManager -Djava.util.logging.config.file=%HORNETQ_HOME%\config\logging.properties -Djava.library.path=.

if "x%HORNETQ_OPTS%" == "x" goto noHORNETQ_OPTS
  set JVM_FLAGS=%JVM_FLAGS% %HORNETQ_OPTS%
:noHORNETQ_OPTS

if "x%HORNETQ_DEBUG%" == "x" goto noDEBUG
  set JVM_FLAGS=%JVM_FLAGS% -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005
:noDEBUG

if "x%HORNETQ_PROFILE%" == "x" goto noPROFILE
  set JVM_FLAGS=-agentlib:yjpagent %JVM_FLAGS%
:noPROFILE

if "%JMX_OPTS%" == "" set JMX_OPTS=-Dcom.sun.management.jmxremote
rem set JMX_OPTS=-Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
set JVM_FLAGS=%JVM_FLAGS% %JMX_OPTS%

set JVM_FLAGS=%JVM_FLAGS% -Dhornetq.home="%HORNETQ_HOME%"
if NOT "x%HORNETQ_BASE%" == "x" set JVM_FLAGS=%JVM_FLAGS% -Dhornetq.base="%HORNETQ_BASE%"
set JVM_FLAGS=%JVM_FLAGS% -classpath "%CLASSPATH%"

"%_JAVACMD%" %JVM_FLAGS% org.hornetq.cli.HornetQ %*

:END
endlocal
GOTO :EOF

:EOF
