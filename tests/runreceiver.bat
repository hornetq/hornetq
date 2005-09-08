java -cp "output/lib/perfrunner.jar;../../common/output/lib/jboss-common.jar;../../thirdparty/apache-log4j/lib/log4j.jar;etc;../../naming/output/lib/jnp-client.jar;../output/lib/jboss-messaging.jar;../../j2ee/output/lib/jboss-j2ee.jar;../../aop/output/lib/jboss-aop.jar;../../thirdparty/jboss/remoting/lib/jboss-remoting.jar;../../server/output/lib/jboss.jar;../../thirdparty/oswego-concurrent/lib/concurrent.jar;../../thirdparty/jboss/remoting/lib/jboss-serialization.jar" org.jboss.test.messaging.jms.perf.PerfRunner receiver jnp://terra:1099 1 1 0 6000000 topic/testTopic FALSE 0 1 NULL NULL FALSE FALSE

rem param 1 = receiver
rem param 2 = server url
rem param 3 = number of connections
rem param 4 = number of sessions, connections are distributed amongst sessions in round-robin fashion
rem param 5 = ramp delay (ms). I.e. number of ms to wait before starting new thread
rem param 6 = length of run (ms)
rem param 7 = destination jndi name
rem param 8 = transacted? (TRUE/FALSE)
rem param 9 = transaction size
rem param 10 = acknowledgement mode, auto=1, client=2, dups_ok=3, session_transacted=0
rem param 11 = durable sub name, or string "NULL" if none
rem param 12 = message selector, or string "NULL" if none
rem param 13 = no local? (TRUE/FALSE)
rem param 14 = use asynch. message receiver? (TRUE/FALSE)

