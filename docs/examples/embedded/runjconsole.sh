export JAVA_HOME=/usr/lib/java-1.5.0/jdk1.5.0_12
CLASSPATH=$JAVA_HOME/lib/tools.jar:$JAVA_HOME/lib/jconsole.jar:../../../output/lib/jboss-messaging.jar
jconsole -J-Djava.class.path=$CLASSPATH