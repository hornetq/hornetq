#!/bin/sh

export JVM_ARGS="$CLUSTER_PROPS -XX:+UseParallelGC -XX:+AggressiveOpts -XX:+UseFastAccessorMethods -Xms512M -Xmx1024M -Djava.net.preferIPv4Stack=true"


java $JVM_ARGS -classpath netty.jar:hornetq-core-client-2.3.25.x.Final.jar:hornetq-commons-2.3.25.x.Final.jar:jboss-logging-3.1.0.GA.jar org.hornetq.core.client.impl.TrackUDP $@
