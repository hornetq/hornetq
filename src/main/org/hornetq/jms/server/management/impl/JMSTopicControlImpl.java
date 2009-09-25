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

package org.hornetq.jms.server.management.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.management.StandardMBean;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.AddressControl;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.jms.HornetQTopic;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.SelectorTranslator;
import org.hornetq.jms.server.management.TopicControl;
import org.hornetq.utils.Pair;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSTopicControlImpl extends StandardMBean implements TopicControl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSTopicControlImpl.class);

   // Attributes ----------------------------------------------------

   private final HornetQTopic managedTopic;

   private final String binding;

   private AddressControl addressControl;

   private ManagementService managementService;

   // Static --------------------------------------------------------

   public static String createFilterFromJMSSelector(final String selectorStr) throws HornetQException
   {
      return (selectorStr == null || selectorStr.trim().length() == 0) ? null
                                                                      : SelectorTranslator.convertToHornetQFilterString(selectorStr);
   }

   // Constructors --------------------------------------------------

   public JMSTopicControlImpl(final HornetQTopic topic,
                           final AddressControl addressControl,
                           final String jndiBinding,
                           final ManagementService managementService) throws Exception
   {
      super(TopicControl.class);
      this.managedTopic = topic;
      this.addressControl = addressControl;
      this.binding = jndiBinding;
      this.managementService = managementService;
   }

   // TopicControlMBean implementation ------------------------------

   public String getName()
   {
      return managedTopic.getName();
   }

   public boolean isTemporary()
   {
      return managedTopic.isTemporary();
   }

   public String getAddress()
   {
      return managedTopic.getAddress();
   }

   public String getJNDIBinding()
   {
      return binding;
   }

   public int getMessageCount()
   {
      return getMessageCount(DurabilityType.ALL);
   }

   public int getDurableMessageCount()
   {
      return getMessageCount(DurabilityType.DURABLE);
   }

   public int getNonDurableMessageCount()
   {
      return getMessageCount(DurabilityType.NON_DURABLE);
   }

   public int getSubscriptionCount()
   {
      return getQueues(DurabilityType.ALL).size();
   }

   public int getDurableSubscriptionCount()
   {
      return getQueues(DurabilityType.DURABLE).size();
   }

   public int getNonDurableSubscriptionCount()
   {
      return getQueues(DurabilityType.NON_DURABLE).size();
   }

   public Object[] listAllSubscriptions()
   {
      return listSubscribersInfos(DurabilityType.ALL);
   }

   public String listAllSubscriptionsAsJSON() throws Exception
   {
      return listSubscribersInfosAsJSON(DurabilityType.ALL);
   }

   public Object[] listDurableSubscriptions()
   {
      return listSubscribersInfos(DurabilityType.DURABLE);
   }

   public String listDurableSubscriptionsAsJSON() throws Exception
   {
      return listSubscribersInfosAsJSON(DurabilityType.DURABLE);
   }

   public Object[] listNonDurableSubscriptions()
   {
      return listSubscribersInfos(DurabilityType.NON_DURABLE);
   }

   public String listNonDurableSubscriptionsAsJSON() throws Exception
   {
      return listSubscribersInfosAsJSON(DurabilityType.NON_DURABLE);
   }

   public Map<String, Object>[] listMessagesForSubscription(final String queueName) throws Exception
   {
      QueueControl coreQueueControl = (QueueControl)managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null)
      {
         throw new IllegalArgumentException("No subscriptions with name " + queueName);
      }

      Map<String, Object>[] coreMessages = coreQueueControl.listMessages(null);

      Map<String, Object>[] jmsMessages = new Map[coreMessages.length];

      int i = 0;

      for (Map<String, Object> coreMessage : coreMessages)
      {
         jmsMessages[i++] = HornetQMessage.coreMaptoJMSMap(coreMessage);
      }
      return jmsMessages;
   }

   public String listMessagesForSubscriptionAsJSON(String queueName) throws Exception
   {
      return JMSQueueControlImpl.toJSON(listMessagesForSubscription(queueName));
   }

   public int countMessagesForSubscription(final String clientID, final String subscriptionName, final String filterStr) throws Exception
   {
      String queueName = HornetQTopic.createQueueNameForDurableSubscription(clientID, subscriptionName);
      QueueControl coreQueueControl = (QueueControl)managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null)
      {
         throw new IllegalArgumentException("No subscriptions with name " + queueName + " for clientID " + clientID);
      }
      String filter = createFilterFromJMSSelector(filterStr);
      return coreQueueControl.listMessages(filter).length;
   }

   public int removeMessages(String filterStr) throws Exception
   {
      String filter = createFilterFromJMSSelector(filterStr);
      int count = 0;
      String[] queues = addressControl.getQueueNames();
      for (String queue : queues)
      {
         QueueControl coreQueueControl = (QueueControl)managementService.getResource(ResourceNames.CORE_QUEUE + queue);
         count += coreQueueControl.removeMessages(filter);
      }

      return count;
   }

   public void dropDurableSubscription(String clientID, String subscriptionName) throws Exception
   {
      String queueName = HornetQTopic.createQueueNameForDurableSubscription(clientID, subscriptionName);
      QueueControl coreQueueControl = (QueueControl)managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null)
      {
         throw new IllegalArgumentException("No subscriptions with name " + queueName + " for clientID " + clientID);
      }
      HornetQServerControl serverControl = (HornetQServerControl)managementService.getResource(ResourceNames.CORE_SERVER);
      serverControl.destroyQueue(queueName);
   }

   public void dropAllSubscriptions() throws Exception
   {
      HornetQServerControl serverControl = (HornetQServerControl)managementService.getResource(ResourceNames.CORE_SERVER);
      String[] queues = addressControl.getQueueNames();
      for (String queue : queues)
      {
         serverControl.destroyQueue(queue);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private Object[] listSubscribersInfos(final DurabilityType durability)
   {
      List<QueueControl> queues = getQueues(durability);
      List<Object[]> subInfos = new ArrayList<Object[]>(queues.size());

      for (QueueControl queue : queues)
      {
         String clientID = null;
         String subName = null;

         if (queue.isDurable())
         {
            Pair<String, String> pair = HornetQTopic.decomposeQueueNameForDurableSubscription(queue.getName()
                                                                                                   .toString());
            clientID = pair.a;
            subName = pair.b;
         }

         String filter = queue.getFilter() != null ? queue.getFilter() : null;

         Object[] subscriptionInfo = new Object[6];
         subscriptionInfo[0] = queue.getName();
         subscriptionInfo[1] = clientID;
         subscriptionInfo[2] = subName;
         subscriptionInfo[3] = queue.isDurable();
         subscriptionInfo[4] = queue.getMessageCount();

         subInfos.add(subscriptionInfo);
      }
      return subInfos.toArray(new Object[subInfos.size()]);
   }

   private String listSubscribersInfosAsJSON(final DurabilityType durability) throws Exception
   {
      List<QueueControl> queues = getQueues(durability);
      JSONArray array = new JSONArray();

      for (QueueControl queue : queues)
      {
         String clientID = null;
         String subName = null;

         if (queue.isDurable())
         {
            Pair<String, String> pair = HornetQTopic.decomposeQueueNameForDurableSubscription(queue.getName()
                                                                                                   .toString());
            clientID = pair.a;
            subName = pair.b;
         }

         String filter = queue.getFilter() != null ? queue.getFilter() : null;

         JSONObject info = new JSONObject();
         info.put("queueName", queue.getName());
         info.put("clientID", clientID);
         info.put("selector", filter);
         info.put("name", subName);
         info.put("durable", queue.isDurable());
         info.put("messageCount", queue.getMessageCount());
         array.put(info);
      }

      return array.toString();
   }

   private int getMessageCount(final DurabilityType durability)
   {
      List<QueueControl> queues = getQueues(durability);
      int count = 0;
      for (QueueControl queue : queues)
      {
         count += queue.getMessageCount();
      }
      return count;
   }

   private List<QueueControl> getQueues(final DurabilityType durability)
   {
      try
      {
         List<QueueControl> matchingQueues = new ArrayList<QueueControl>();
         String[] queues = addressControl.getQueueNames();
         for (String queue : queues)
         {
            QueueControl coreQueueControl = (QueueControl)managementService.getResource(ResourceNames.CORE_QUEUE + queue);

            // Ignore the "special" subscription
            if (!coreQueueControl.getName().equals(addressControl.getAddress()))
            {
               if (durability == DurabilityType.ALL || (durability == DurabilityType.DURABLE && coreQueueControl.isDurable()) ||
                   (durability == DurabilityType.NON_DURABLE && !coreQueueControl.isDurable()))
               {
                  matchingQueues.add(coreQueueControl);
               }
            }
         }
         return matchingQueues;
      }
      catch (Exception e)
      {
         return Collections.emptyList();
      }
   }

   // Inner classes -------------------------------------------------

   private enum DurabilityType
   {
      ALL, DURABLE, NON_DURABLE
   }
}
