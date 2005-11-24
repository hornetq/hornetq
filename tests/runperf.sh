#!/bin/sh
#
# Run the performance runner
#

${JAVA_HOME}/bin/java -Dperf.jndiProperties=perf-jndi.properties -Dperf.properties=jbossmessaging-perf.properties -cp \
"output/lib/jboss-messaging-perf.jar:\
../../thirdparty/apache-log4j/lib/log4j.jar:\
etc:\
../../j2ee/output/lib/jboss-j2ee.jar:\
../../thirdparty/jboss/remoting/lib/jboss-remoting.jar:\
../../common/output/lib/jboss-common.jar:\
../../thirdparty/jboss/remoting/lib/jboss-serialization.jar:\
../output/lib/jboss-messaging.jar:\
/dev/jboss-head/build/output/jboss-5.0.0alpha/client/jbossall-client.jar:\
../../aop/output/lib/jboss-aop.jar:\
../src/resources/hsqldb.jar" \
org.jboss.test.messaging.jms.perf.framework.PerfRunner






