export JBM_HOME=..
export CLASSPATH=$JBM_HOME/config/
export JVM_ARGS="-Xmx512M -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.core.logging.JBMLoggerPlugin -Djava.library.path=."
#export JVM_ARGS="-Xmx512M -Dorg.jboss.logging.Logger.pluginClass=org.jboss.messaging.core.logging.JBMLoggerPlugin -Djava.library.path=. -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
for i in `ls $JBM_HOME/lib/*.jar`; do
	CLASSPATH=$i:$CLASSPATH
done
echo ***********************************************************************************
echo "java $JVM_ARGS -classpath $CLASSPATH org.jboss.jms.server.microcontainer.JBMBootstrapServer jbm-standalone-beans.xml"
echo ***********************************************************************************
java $JVM_ARGS -classpath $CLASSPATH org.jboss.messaging.microcontainer.JBMBootstrapServer jbm-standalone-beans.xml