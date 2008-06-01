@ echo off
setlocal ENABLEDELAYEDEXPANSION
set JBM_HOME=..
set CLASSPATH=%JBM_HOME%\config\
set JVM_ARGS=-XX:+UseParallelGC -Xms512M -Xmx1024M -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.core.logging.JBMLoggerPlugin -Djava.library.path=.
REM export JVM_ARGS="-Xmx512M -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.core.logging.JBMLoggerPlugin -Djava.library.path=. -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
for /R ..\lib %%A in (*.jar) do (	
SET CLASSPATH=!CLASSPATH!;%%A
)

echo ***********************************************************************************
echo "java %JVM_ARGS% -classpath %CLASSPATH% org.jboss.jms.server.microcontainer.JBMBootstrapServer jbm-standalone-beans.xml"
echo ***********************************************************************************
java %JVM_ARGS% -classpath %CLASSPATH% org.jboss.messaging.microcontainer.JBMBootstrapServer jbm-standalone-beans.xml



