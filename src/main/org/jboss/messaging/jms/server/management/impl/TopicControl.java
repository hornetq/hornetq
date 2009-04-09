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

package org.jboss.messaging.jms.server.management.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.MessageInfo;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.SelectorTranslator;
import org.jboss.messaging.jms.server.management.JMSMessageInfo;
import org.jboss.messaging.jms.server.management.SubscriptionInfo;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.utils.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class TopicControl implements TopicControlMBean
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TopicControl.class);

   // Attributes ----------------------------------------------------

   private final JBossTopic managedTopic;

   private final String binding;

   private AddressControlMBean addressControl;

   private ManagementService managementService;

   // Static --------------------------------------------------------

   public static String createFilterFromJMSSelector(final String selectorStr) throws MessagingException
   {
      return (selectorStr == null) ? null : SelectorTranslator.convertToJBMFilterString(selectorStr);
   }
   
   // Constructors --------------------------------------------------

   public TopicControl(final JBossTopic topic,
                       final AddressControlMBean addressControl,
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

   public int getDurableMessagesCount()
   {
      return getMessageCount(DurabilityType.DURABLE);
   }

   public int getNonDurableMessagesCount()
   {
      return getMessageCount(DurabilityType.NON_DURABLE);
   }

   public int getSubcriptionsCount()
   {
      return getQueues(DurabilityType.ALL).size();
   }

   public int getDurableSubcriptionsCount()
   {
      return getQueues(DurabilityType.DURABLE).size();
   }

   public int getNonDurableSubcriptionsCount()
   {
      return getQueues(DurabilityType.NON_DURABLE).size();
   }

   public TabularData listAllSubscriptions()
   {
      return SubscriptionInfo.toTabularData(listSubscribersInfos(DurabilityType.ALL));
   }

   public TabularData listDurableSubscriptions()
   {
      return SubscriptionInfo.toTabularData(listSubscribersInfos(DurabilityType.DURABLE));
   }

   public TabularData listNonDurableSubscriptions()
   {
      return SubscriptionInfo.toTabularData(listSubscribersInfos(DurabilityType.NON_DURABLE));
   }

   public TabularData listMessagesForSubscription(final String queueName) throws Exception
   {
      QueueControlMBean coreQueueControl = (QueueControlMBean)managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null)
      {
         throw new IllegalArgumentException("No subscriptions with name " + queueName);
      }
      TabularData coreMessages = coreQueueControl.listAllMessages();
      List<JMSMessageInfo> infos = new ArrayList<JMSMessageInfo>(coreMessages.size());
      MessageInfo[] coreMessageInfos = MessageInfo.from(coreMessages);
      for (MessageInfo messageInfo : coreMessageInfos)
      {
         JMSMessageInfo info = JMSMessageInfo.fromCoreMessage(messageInfo);
         infos.add(info);
      }
      return JMSMessageInfo.toTabularData(infos);
   }

   public int countMessagesForSubscription(final String clientID, final String subscriptionName, final String filterStr) throws Exception
   {
      String queueName = JBossTopic.createQueueNameForDurableSubscription(clientID, subscriptionName);
      QueueControlMBean coreQueueControl = (QueueControlMBean)managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null)
      {
         throw new IllegalArgumentException("No subscriptions with name " + queueName + " for clientID " + clientID);
      }
      String filter = createFilterFromJMSSelector(filterStr);
      return coreQueueControl.listMessages(filter).size();
   }

   public int removeAllMessages() throws Exception
   {
      int count = 0;
      String[] queues = addressControl.getQueueNames();
      for (String queue : queues)
      {
         QueueControlMBean coreQueueControl = (QueueControlMBean)managementService.getResource(ResourceNames.CORE_QUEUE + queue);
         count += coreQueueControl.removeAllMessages();
      }

      return count;
   }

   public void dropDurableSubscription(String clientID, String subscriptionName) throws Exception
   {
      String queueName = JBossTopic.createQueueNameForDurableSubscription(clientID, subscriptionName);
      QueueControlMBean coreQueueControl = (QueueControlMBean)managementService.getResource(ResourceNames.CORE_QUEUE + queueName);
      if (coreQueueControl == null)
      {
         throw new IllegalArgumentException("No subscriptions with name " + queueName + " for clientID " + clientID);
      }
      MessagingServerControlMBean serverControl = (MessagingServerControlMBean)managementService.getResource(ResourceNames.CORE_SERVER);
      serverControl.destroyQueue(queueName);
   }

   public void dropAllSubscriptions() throws Exception
   {
      MessagingServerControlMBean serverControl = (MessagingServerControlMBean)managementService.getResource(ResourceNames.CORE_SERVER);
      String[] queues = addressControl.getQueueNames();
      for (String queue : queues)
      {
         serverControl.destroyQueue(queue);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private SubscriptionInfo[] listSubscribersInfos(final DurabilityType durability)
   {
      List<QueueControlMBean> queues = getQueues(durability);
      List<SubscriptionInfo> subInfos = new ArrayList<SubscriptionInfo>(queues.size());

      for (QueueControlMBean queue : queues)
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
         SubscriptionInfo info = new SubscriptionInfo(queue.getName(),
                                                      clientID,
                                                      subName,
                                                      queue.isDurable(),
                                                      filter,
                                                      queue.getMessageCount());
         subInfos.add(info);
      }
      return (SubscriptionInfo[])subInfos.toArray(new SubscriptionInfo[subInfos.size()]);
   }

   private int getMessageCount(final DurabilityType durability)
   {
      List<QueueControlMBean> queues = getQueues(durability);
      int count = 0;
      for (QueueControlMBean queue : queues)
      {
         count += queue.getMessageCount();
      }
      return count;
   }

   private List<QueueControlMBean> getQueues(final DurabilityType durability)
   {
      try
      {
         List<QueueControlMBean> matchingQueues = new ArrayList<QueueControlMBean>();
         String[] queues = addressControl.getQueueNames();
         for (String queue : queues)
         {
            QueueControlMBean coreQueueControl = (QueueControlMBean)managementService.getResource(ResourceNames.CORE_QUEUE + queue);

            //Ignore the "special" subscription
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
