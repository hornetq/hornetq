CONFIG=$1
echo running Server with config ${CONFIG}
java -classpath config/:config/${CONFIG}:lib/mysql-connector-java-5.0.7-bin.jar:lib/jboss-messaging.jar org.jboss.jms.server.microcontainer.JBMBootstrapServer ${CONFIG}.xml jbm-beans.xml