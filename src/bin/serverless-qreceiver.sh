#!/bin/sh

main_class="org.jboss.jms.serverless.client.CommonInterfaceQueueReceiver"

source `dirname $0`/functions
run $@
