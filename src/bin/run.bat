@ echo off
setlocal ENABLEDELAYEDEXPANSION
set JBM_HOME=..
set CONFIG_DIR=%JBM_HOME%\config\stand-alone\non-clustered
set CLASSPATH=%CONFIG_DIR%;%JBM_HOME%\schemas\
set CLUSTER_PROPS=;
#you can use the following line if you want to run with different ports
#set CLUSTER_PROPS="-Djnp.port=1099 -Djnp.rmiPort=1098 -Djnp.host=localhost -Djbm.remoting.netty.host=localhost -Djbm.remoting.netty.port=5445"
set JVM_ARGS=%CLUSTER_PROPS% -XX:+UseParallelGC  -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms512M -Xmx1024M -Djbm.config.dir=$CONFIG_DIR  -Djava.util.logging.config.file=%CONFIG_DIR%\logging.properties -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.integration.logging.JBMLoggerPlugin -Djava.library.path=.
REM export JVM_ARGS="-Xmx512M -Djava.util.logging.config.file=%CONFIG_DIR%\logging.properties -Djbm.config.dir=$CONFIG_DIR  -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.integration.logging.JBMLoggerPlugin -Djava.library.path=. -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
for /R ..\lib %%A in (*.jar) do (
SET CLASSPATH=!CLASSPATH!;%%A
)
mkdir logs
echo ***********************************************************************************
echo "java %JVM_ARGS% -classpath %CLASSPATH% org.jboss.messaging.integration.bootstrap.JBMBootstrapServer jbm-jboss-beans.xml"
echo ***********************************************************************************
java %JVM_ARGS% -classpath %CLASSPATH% org.jboss.messaging.integration.bootstrap.JBMBootstrapServer jbm-jboss-beans.xml



