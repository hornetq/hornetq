#!/bin/sh
#
# Run the performance slave
#

${JAVA_HOME}/bin/java -cp \
"output/lib/jboss-messaging-perf.jar:\
../../thirdparty/apache-log4j/lib/log4j.jar:\
etc:\
../../j2ee/output/lib/jboss-j2ee.jar:\
../../j2se/output/lib/jboss-j2se.jar:\
../../thirdparty/jboss/remoting/lib/jboss-remoting.jar:\
../../common/output/lib/jboss-common.jar:\
../../thirdparty/jboss/remoting/lib/jboss-serialization.jar:\
../../thirdparty/apache-httpclient/lib/commons-httpclient.jar:\
../../thirdparty/sun-servlet/lib/servlet-api.jar:\
../../thirdparty/oswego-concurrent/lib/concurrent.jar" \
org.jboss.test.messaging.jms.perf.framework.Slave $*
