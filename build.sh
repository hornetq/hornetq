#!/bin/sh

src/bin/build.sh "$@"

# the java5 client jars are replacing the regular clients on this branch
#   as agreed with the users of this branch (Customers)
cd build/jars
mv hornetq-jms-client-java5.jar hornetq-jms-client.jar
mv hornetq-core-client-java5.jar hornetq-core-client.jar 
