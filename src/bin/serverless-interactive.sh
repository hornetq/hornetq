#!/bin/sh

main_class="org.clester.Main"

source `dirname $0`/functions
run org.jboss.jms.serverless.client.Interactive $@





