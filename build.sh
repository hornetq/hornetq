#!/bin/sh

src/bin/build.sh "$@"

# the java5 client jars are replacing the regular clients on this branch
#   as agreed with the users of this branch (Customers)

#generating the dist-rollout.zip
cd build/jars
rm -rf dist-rollout
mkdir dist-rollout
cp hornetq-core.jar dist-rollout/
cp hornetq-bootstrap.jar dist-rollout/
cp hornetq-core-client-java5.jar dist-rollout/hornetq-core-client.jar
cp hornetq-jms-client-java5.jar dist-rollout/hornetq-jms-client.jar
cp hornetq-jms.jar dist-rollout/
cp hornetq-logging.jar dist-rollout/
cp hornetq-ra.jar dist-rollout/
cp hornetq-jboss-as-integration.jar dist-rollout/
cd dist-rollout
zip -r dist-rollout *

