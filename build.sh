#!/bin/sh

src/bin/build.sh "$@"

# the java5 client jars are replacing the regular clients on this branch
#   as agreed with the users of this branch (Customers)
cd build/jars
mv hornetq-jms-client-java5.jar hornetq-jms-client.jar
mv hornetq-core-client-java5.jar hornetq-core-client.jar 


#generating the dist-rollout.zip
rm -rf dist-oneoff
mkdir dist-oneoff
cp hornetq-core.jar dist-oneoff/
cp hornetq-bootstrap.jar dist-oneoff/
cp hornetq-core-client.jar dist-oneoff/
cp hornetq-jms-client.jar dist-oneoff/
cp hornetq-jms.jar dist-oneoff/
cp hornetq-logging.jar dist-oneoff/
cp hornetq-ra.jar dist-oneoff/
cd dist-oneoff
zip -r dist-rollout *

