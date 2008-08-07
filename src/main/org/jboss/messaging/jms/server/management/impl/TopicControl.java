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

import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.management.Operation;
import org.jboss.messaging.core.management.Parameter;
import org.jboss.messaging.core.management.impl.MBeanInfoHelper;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.server.management.JMSMessageInfo;
import org.jboss.messaging.jms.server.management.SubscriberInfo;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class TopicControl extends StandardMBean implements TopicControlMBean
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JBossTopic managedTopic;
   private final MessagingServerManagement server;
   private final String binding;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicControl(final JBossTopic topic,
         final MessagingServerManagement server, final String jndiBinding)
         throws NotCompliantMBeanException
   {
      super(TopicControlMBean.class);
      this.managedTopic = topic;
      this.server = server;
      this.binding = jndiBinding;
   }

   // Public --------------------------------------------------------

   // StandardMBean overrides ---------------------------------------

   /**
    * overrides getMBeanInfo to add operations info using annotations
    * 
    * @see Operation
    * @see Parameter
    */
   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(), info.getDescription(), info
            .getAttributes(), info.getConstructors(), MBeanInfoHelper
            .getMBeanOperationsInfo(TopicControlMBean.class), info
            .getNotifications());
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

   public int getSubcribersCount()
   {
      return getQueues(DurabilityType.ALL).size();
   }

   public int getDurableSubcribersCount()
   {
      return getQueues(DurabilityType.DURABLE).size();
   }

   public int getNonDurableSubcribersCount()
   {
      return getQueues(DurabilityType.NON_DURABLE).size();
   }

   public TabularData listAllSubscribers()
   {
      return SubscriberInfo.toTabularData(listAllSubscriberInfos());
   }

   public TabularData listDurableSubscribers()
   {
      return SubscriberInfo.toTabularData(listDurableSubscriberInfos());
   }

   public TabularData listNonDurableSubscribers()
   {
      return SubscriberInfo.toTabularData(listNonDurableSubscriberInfos());
   }

   public SubscriberInfo[] listAllSubscriberInfos()
   {
      return listSubscribersInfos(DurabilityType.ALL);
   }

   public SubscriberInfo[] listDurableSubscriberInfos()
   {
      return listSubscribersInfos(DurabilityType.DURABLE);
   }

   public SubscriberInfo[] listNonDurableSubscriberInfos()
   {
      return listSubscribersInfos(DurabilityType.NON_DURABLE);
   }

   public TabularData listMessagesForSubscriber(final String subscriberID)
         throws Exception
   {
      Queue queue = server.getQueue(new SimpleString(subscriberID));
      List<MessageReference> messageRefs = queue.list(null);
      List<JMSMessageInfo> infos = new ArrayList<JMSMessageInfo>(messageRefs
            .size());

      for (MessageReference messageRef : messageRefs)
      {
         ServerMessage message = messageRef.getMessage();
         JMSMessageInfo info = JMSMessageInfo.fromServerMessage(message);
         infos.add(info);
      }
      return JMSMessageInfo.toTabularData(infos);
   }

   public void removeAllMessages() throws Exception
   {
      server.removeAllMessagesForAddress(managedTopic.getSimpleAddress());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private SubscriberInfo[] listSubscribersInfos(final DurabilityType durability)
   {
      List<Queue> queues = getQueues(durability);
      List<SubscriberInfo> subInfos = new ArrayList<SubscriberInfo>(queues
            .size());
      for (Queue queue : queues)
      {
         String clientID = null;
         String subName = null;

         if (queue.isDurable())
         {
            Pair<String, String> pair = JBossTopic
                  .decomposeQueueNameForDurableSubscription(queue.getName()
                        .toString());
            clientID = pair.a;
            subName = pair.b;
         }

         String filter = queue.getFilter() != null ? queue.getFilter()
               .getFilterString().toString() : null;
         SubscriberInfo info = new SubscriberInfo(queue.getName().toString(),
               clientID, subName, queue.isDurable(), filter, queue
                     .getMessageCount(), queue.getMaxSizeBytes());
         subInfos.add(info);
      }
      return (SubscriberInfo[]) subInfos.toArray(new SubscriberInfo[subInfos
            .size()]);
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
         List<Queue> queues = server.getQueuesForAddress(managedTopic
               .getSimpleAddress());
         List<Queue> matchingQueues = new ArrayList<Queue>();
         for (Queue queue : queues)
         {
            if (durability == DurabilityType.ALL
                  || (durability == DurabilityType.DURABLE && queue.isDurable())
                  || (durability == DurabilityType.NON_DURABLE && !queue
                        .isDurable()))
            {
               matchingQueues.add(queue);
            }
         }
         return matchingQueues;
      } catch (Exception e)
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
