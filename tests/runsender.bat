java -cp "output/lib/perfrunner.jar;../../common/output/lib/jboss-common.jar;../../thirdparty/apache-log4j/lib/log4j.jar;etc;../../naming/output/lib/jnp-client.jar;../output/lib/jboss-messaging.jar;../../j2ee/output/lib/jboss-j2ee.jar;../../aop/output/lib/jboss-aop.jar;../../thirdparty/jboss/remoting/lib/jboss-remoting.jar;../../server/output/lib/jboss.jar;../../thirdparty/oswego-concurrent/lib/concurrent.jar;../../thirdparty/jboss/remoting/lib/jboss-serialization.jar" org.jboss.test.messaging.jms.perf.PerfRunner sender jnp://terra:1099 1 1 0 6000000 topic/testTopic FALSE 0 FALSE 1024 javax.jms.BytesMessage 1

rem param 1 = sender
rem param 2 = server url
rem param 3 = number of connections
rem param 4 = number of sessions, connections are distributed amongst sessions in round-robin fashion
rem param 5 = ramp delay (ms). I.e. number of ms to wait before starting new thread
rem param 6 = length of run (ms)
rem param 7 = destination jndi name
rem param 8 = transacted? (TRUE/FALSE)
rem param 9 = transaction size
rem param 10 = use anonymous producer? (TRUE/FALSE)
rem param 11 = message size (bytes)
rem param 12= message type
rem param 13 = delivery mode (1=non persistent, 2 = persistent)

     

