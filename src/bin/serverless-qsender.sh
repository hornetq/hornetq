#!/bin/sh

main_class="org.jboss.jms.serverless.client.CommonInterfaceQueueSender"

source `dirname $0`/functions
run $@
