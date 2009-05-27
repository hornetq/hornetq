export JBM_HOME=..
mkdir logs
# By default, the server is started in the non-clustered standalone configuration

if [ a"$1" = a ]; then CONFIG_DIR=$JBM_HOME/config/stand-alone/non-clustered; else CONFIG_DIR="$1"; fi
if [ a"$2" = a ]; then FILENAME=jbm-jboss-beans.xml; else FILENAME="$2"; fi

export CLASSPATH=$CONFIG_DIR:$JBM_HOME/schemas/
export JVM_ARGS="-XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms512M -Xmx1024M -Djava.util.logging.config.file=$CONFIG_DIR/logging.properties -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.integration.logging.JBMLoggerPlugin -Djava.library.path=."
#export JVM_ARGS="-Xmx512M -Djava.util.logging.config.file=$CONFIG_DIR/logging.properties -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.integration.logging.JBMLoggerPlugin -Djava.library.path=. -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"

for i in `ls $JBM_HOME/lib/*.jar`; do
	CLASSPATH=$i:$CLASSPATH
done

echo ***********************************************************************************
echo "java $JVM_ARGS -classpath $CLASSPATH org.jboss.messaging.integration.bootstrap.JBMBootstrapServer $FILENAME"
echo ***********************************************************************************
java $JVM_ARGS -classpath $CLASSPATH -Dcom.sun.management.jmxremote org.jboss.messaging.integration.bootstrap.JBMBootstrapServer $FILENAME