/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.server.management;

import java.util.Map;

import javax.management.MBeanOperationInfo;

import org.hornetq.core.server.management.Operation;
import org.hornetq.core.server.management.Parameter;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface TopicControl extends DestinationControl
{
   // Attributes ----------------------------------------------------

   int getSubscriptionCount();

   int getDurableSubscriptionCount();

   int getNonDurableSubscriptionCount();

   int getDurableMessageCount();

   int getNonDurableMessageCount();

   // Operations ----------------------------------------------------

   @Operation(desc = "List all subscriptions")
   Object[] listAllSubscriptions() throws Exception;

   @Operation(desc = "List all subscriptions")
   String listAllSubscriptionsAsJSON() throws Exception;

   @Operation(desc = "List only the durable subscriptions")
   Object[] listDurableSubscriptions() throws Exception;

   @Operation(desc = "List only the durable subscriptions")
   String listDurableSubscriptionsAsJSON() throws Exception;

   @Operation(desc = "List only the non durable subscriptions")
   Object[] listNonDurableSubscriptions() throws Exception;

   @Operation(desc = "List only the non durable subscriptions")
   String listNonDurableSubscriptionsAsJSON() throws Exception;

   @Operation(desc = "List all the message for the given subscription")
   public Map<String, Object>[] listMessagesForSubscription(@Parameter(name = "queueName", desc = "the name of the queue representing a subscription") String queueName) throws Exception;

   @Operation(desc = "List all the message for the given subscription")
   public String listMessagesForSubscriptionAsJSON(@Parameter(name = "queueName", desc = "the name of the queue representing a subscription") String queueName) throws Exception;

   @Operation(desc = "Count the number of messages matching the filter for the given subscription")
   public int countMessagesForSubscription(@Parameter(name = "clientID", desc = "the client ID") String clientID,
                                           @Parameter(name = "subscriptionName", desc = "the name of the durable subscription") String subscriptionName,
                                           @Parameter(name = "filter", desc = "a JMS filter (can be empty)") String filter) throws Exception;

   @Operation(desc = "Drop a durable subscription", impact = MBeanOperationInfo.ACTION)
   void dropDurableSubscription(@Parameter(name = "clientID", desc = "the client ID") String clientID,
                                @Parameter(name = "subscriptionName", desc = "the name of the durable subscription") String subscriptionName) throws Exception;

   @Operation(desc = "Drop all subscriptions from this topic", impact = MBeanOperationInfo.ACTION)
   void dropAllSubscriptions() throws Exception;
}
