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

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.server.management.JMSMessageInfo;
import org.jboss.messaging.jms.server.management.SubscriptionInfo;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

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

   private final PostOffice postOffice;

   private final StorageManager storageManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicControl(final JBossTopic topic,
                       final String jndiBinding,
                       final PostOffice postOffice,
                       final StorageManager storageManager)
   {
      this.managedTopic = topic;
      this.binding = jndiBinding;
      this.postOffice = postOffice;
      this.storageManager = storageManager;
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
      return SubscriptionInfo.toTabularData(listAllSubscriptionInfos());
   }

   public TabularData listDurableSubscriptions()
   {
      return SubscriptionInfo.toTabularData(listDurableSubscriptionInfos());
   }

   public TabularData listNonDurableSubscriptions()
   {
      return SubscriptionInfo.toTabularData(listNonDurableSubscriptionInfos());
   }

   public SubscriptionInfo[] listAllSubscriptionInfos()
   {
      return listSubscribersInfos(DurabilityType.ALL);
   }

   public SubscriptionInfo[] listDurableSubscriptionInfos()
   {
      return listSubscribersInfos(DurabilityType.DURABLE);
   }

   public SubscriptionInfo[] listNonDurableSubscriptionInfos()
   {
      return listSubscribersInfos(DurabilityType.NON_DURABLE);
   }

   public TabularData listMessagesForSubscription(final String queueName) throws Exception
   {
      SimpleString sAddress = new SimpleString(queueName);
      Binding binding = postOffice.getBinding(sAddress);
      if (binding == null)
      {
         throw new IllegalArgumentException("No queue with name " + sAddress);
      }
      Queue queue = binding.getQueue();
      List<MessageReference> messageRefs = queue.list(null);
      List<JMSMessageInfo> infos = new ArrayList<JMSMessageInfo>(messageRefs.size());

      for (MessageReference messageRef : messageRefs)
      {
         ServerMessage message = messageRef.getMessage();
         JMSMessageInfo info = JMSMessageInfo.fromServerMessage(message);
         infos.add(info);
      }
      return JMSMessageInfo.toTabularData(infos);
   }

   public int removeAllMessages() throws Exception
   {
      int count = 0;
      List<Binding> bindings = postOffice.getBindingsForAddress(managedTopic.getSimpleAddress());

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();
         count += queue.deleteAllReferences(storageManager);
      }

      return count;
   }

   public void dropDurableSubscription(String clientID, String subscriptionName) throws Exception
   {
      String queueName = JBossTopic.createQueueNameForDurableSubscription(clientID, subscriptionName);
      Binding binding = postOffice.getBinding(new SimpleString(queueName));

      if (binding == null)
      {
         throw new IllegalArgumentException("No durable subscription for clientID=" + clientID +
                                            ", subcription=" +
                                            subscriptionName);
      }

      Queue queue = binding.getQueue();

      queue.deleteAllReferences(storageManager);

      postOffice.removeBinding(queue.getName());
   }

   public void dropAllSubscriptions() throws Exception
   {
      List<Binding> bindings = postOffice.getBindingsForAddress(managedTopic.getSimpleAddress());

      for (Binding binding : bindings)
      {
         Queue queue = binding.getQueue();
         queue.deleteAllReferences(storageManager);
         postOffice.removeBinding(queue.getName());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private SubscriptionInfo[] listSubscribersInfos(final DurabilityType durability)
   {
      List<Queue> queues = getQueues(durability);
      List<SubscriptionInfo> subInfos = new ArrayList<SubscriptionInfo>(queues.size());

      for (Queue queue : queues)
      {
         String clientID = null;
         String subName = null;

         if (queue.isDurable())
         {
            Pair<String, String> pair = JBossTopic.decomposeQueueNameForDurableSubscription(queue.getName().toString());
            clientID = pair.a;
            subName = pair.b;
         }

         String filter = queue.getFilter() != null ? queue.getFilter().getFilterString().toString() : null;
         SubscriptionInfo info = new SubscriptionInfo(queue.getName().toString(),
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
      List<Queue> queues = getQueues(durability);
      int count = 0;
      for (Queue queue : queues)
      {
         count += queue.getMessageCount();
      }
      return count;
   }

   private List<Queue> getQueues(final DurabilityType durability)
   {
      try
      {
         List<Binding> bindings = postOffice.getBindingsForAddress(managedTopic.getSimpleAddress());
         List<Queue> matchingQueues = new ArrayList<Queue>();

         for (Binding binding : bindings)
         {
            Queue queue = binding.getQueue();
            if (durability == DurabilityType.ALL || (durability == DurabilityType.DURABLE && queue.isDurable()) ||
                (durability == DurabilityType.NON_DURABLE && !queue.isDurable()))
            {
               matchingQueues.add(queue);
            }
         }
         return matchingQueues;
      }
      catch (Exception e)
      {
         e.printStackTrace();
         return Collections.emptyList();
      }
   }

   // Inner classes -------------------------------------------------

   private enum DurabilityType
   {
      ALL, DURABLE, NON_DURABLE
   }
}
