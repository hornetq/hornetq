export JBM_HOME=..
if [ a"$1" = a ]; then FILENAME=jbm-standalone-beans.xml; else FILENAME="$1"; fi
export CLASSPATH=$JBM_HOME/config/:$JBM_HOME/schemas/
export JVM_ARGS="-XX:+UseParallelGC -Xms512M -Xmx1024M -Djava.util.logging.config.file=$JBM_HOME/config/logging.properties -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.integration.logging.JBMLoggerPlugin -Djava.library.path=."
#export JVM_ARGS="-Xmx512M -Djava.util.logging.config.file=$JBM_HOME/config/logging.properties -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.integration.logging.JBMLoggerPlugin -Djava.library.path=. -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
for i in `ls $JBM_HOME/lib/*.jar`; do
	CLASSPATH=$i:$CLASSPATH
done
echo ***********************************************************************************
echo "java $JVM_ARGS -classpath $CLASSPATH org.jboss.messaging.integration.bootstrap.JBMBootstrapServer $FILENAME"
echo ***********************************************************************************
java $JVM_ARGS -classpath $CLASSPATH -Dcom.sun.management.jmxremote org.jboss.messaging.integration.bootstrap.JBMBootstrapServer $FILENAME