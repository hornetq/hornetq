#!/bin/sh

export HORNETQ_HOME=..
mkdir -p ../logs
# By default, the server is started in the non-clustered standalone configuration

if [ a"$1" = a ]; then CONFIG_DIR=$HORNETQ_HOME/config/stand-alone/non-clustered; else CONFIG_DIR="$1"; fi
if [ a"$2" = a ]; then FILENAME=hornetq-beans.xml; else FILENAME="$2"; fi

export CLASSPATH=$CONFIG_DIR:$HORNETQ_HOME/schemas/
#you can use the following line if you want to run with different ports
#export CLUSTER_PROPS="-Djnp.port=1099 -Djnp.rmiPort=1098 -Djnp.host=localhost -Dhornetq.remoting.netty.host=localhost -Dhornetq.remoting.netty.port=5445"
export JVM_ARGS="$CLUSTER_PROPS -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms512M -Xmx1024M -Dhornetq.config.dir=$CONFIG_DIR -Djava.util.logging.config.file=$CONFIG_DIR/logging.properties -Djava.library.path=."
#export JVM_ARGS="-Xmx512M -Djava.util.logging.config.file=$CONFIG_DIR/logging.properties -Dhornetq.config.dir=$CONFIG_DIR -Djava.library.path=. -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"

for i in `ls $HORNETQ_HOME/lib/*.jar`; do
	CLASSPATH=$i:$CLASSPATH
done

echo "***********************************************************************************"
echo "java $JVM_ARGS -classpath $CLASSPATH org.hornetq.integration.bootstrap.HornetQBootstrapServer $FILENAME"
echo "***********************************************************************************"
java $JVM_ARGS -classpath $CLASSPATH -Dcom.sun.management.jmxremote org.hornetq.integration.bootstrap.HornetQBootstrapServer $FILENAME