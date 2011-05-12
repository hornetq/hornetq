#tool used to debug Journal Data
java -cp ../build/jars/hornetq-core.jar:../thirdparty/org/jboss/netty/lib/netty.jar org.hornetq.core.persistence.impl.journal.PrintData $*
