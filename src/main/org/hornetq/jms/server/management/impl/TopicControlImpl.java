/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.hornetq.jms.server.management.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.AddressControl;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.MessagingServerControl;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.jms.JBossTopic;
import org.hornetq.jms.client.JBossMessage;
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
public class TopicControlImpl implements TopicControl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TopicControlImpl.class);

   // Attributes ----------------------------------------------------

   private final JBossTopic managedTopic;

   private final String binding;

   private AddressControl addressControl;

   private ManagementService managementService;

   // Static --------------------------------------------------------

   public static String createFilterFromJMSSelector(final String selectorStr) throws MessagingException
   {
      return (selectorStr == null || selectorStr.trim().length() == 0) ? null : SelectorTranslator.convertToJBMFilterString(selectorStr);
   }

   // Constructors --------------------------------------------------

   public TopicControlImpl(final JBossTopic topic,
                       final AddressControl addressControl,
                       final String jndiBinding,
                       final ManagementService managementService)
   {
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
         jmsMessages[i++] = JBossMessage.coreMaptoJMSMap(coreMessage);
      }
      return jmsMessages;
   }
   
   public String listMessagesForSubscriptionAsJSON(String queueName) throws Exception
   {
      return JMSQueueControlImpl.toJSON(listMessagesForSubscription(queueName));
   }

   public int countMessagesForSubscription(final String clientID, final String subscriptionName, final String filterStr) throws Exception
   {
      String queueName = JBossTopic.createQueueNameForDurableSubscription(clientID, subscriptionName);
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
      String queueName = JBossTopic.createQueueNameForDurableSubscription(clientID, subscriptionName);
      QueueControl coreQueueControl = (QueueControl)managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null)
      {
         throw new IllegalArgumentException("No subscriptions with name " + queueName + " for clientID " + clientID);
      }
      MessagingServerControl serverControl = (MessagingServerControl)managementService.getResource(ResourceNames.CORE_SERVER);
      serverControl.destroyQueue(queueName);
   }

   public void dropAllSubscriptions() throws Exception
   {
      MessagingServerControl serverControl = (MessagingServerControl)managementService.getResource(ResourceNames.CORE_SERVER);
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
            Pair<String, String> pair = JBossTopic.decomposeQueueNameForDurableSubscription(queue.getName().toString());
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
            Pair<String, String> pair = JBossTopic.decomposeQueueNameForDurableSubscription(queue.getName().toString());
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
