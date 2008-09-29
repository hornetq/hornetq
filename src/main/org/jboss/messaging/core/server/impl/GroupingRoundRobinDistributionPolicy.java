/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.core.server.impl;

import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.SimpleString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Distributes message based on the message property 'JMSXGroupID'. Once a message has been successfully delivered to a
 * consumer that consumer is then bound to that group. Any message that has the same group id set will always be
 * delivered to the same consumer.
 * The Initial consumer is the first consumer found, using the round robin policy, that hasn't been bound to a group, If
 * there are no consumers left that have not been bound to a group then the next consumer will be bound to 2 groups and
 * so on.
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class GroupingRoundRobinDistributionPolicy extends RoundRobinDistributionPolicy
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Map with GroupID as a key and a Consumer as value.
   private final Map<SimpleString, ConsumerState> consumers = new ConcurrentHashMap<SimpleString, ConsumerState>();

   // we hold the state of each consumer, i.e., is it bound etc
   private final Map<Consumer, ConsumerState> consumerStateMap = new ConcurrentHashMap<Consumer, ConsumerState>();

   // Distributor implementation ------------------------------------

   @Override
   public Consumer select(final ServerMessage message, final boolean redeliver)
   {
      if (message.getProperty(MessageImpl.GROUP_ID) != null)
      {
         final SimpleString groupId = (SimpleString)message.getProperty(MessageImpl.GROUP_ID);
         final ConsumerState consumerState = consumers.get(groupId);
         if (consumerState != null)
         {
            // if this is a redelivery and the group is bound we wait.
            if (redeliver && consumerState.isBound())
            {
               return null;
            }
            // if this is a redelivery and it was its first attempt we can look for another consumer and use that
            else if (redeliver && !consumerState.isBound())
            {
               removeBinding(groupId, consumerState);
               return getNextPositionAndBind(message, redeliver, groupId).getConsumer();
            }
            // we bind after we know that the first message has been successfully consumed
            else if (!consumerState.isBound())
            {
               consumerState.setBound(true);
            }
            consumerState.setAvailable(false);

            return consumerState.getConsumer();
         }
         else
         {
            return getNextPositionAndBind(message, redeliver, groupId).getConsumer();
         }
      }
      else
      {
         return super.select(message, redeliver);
      }
   }

   @Override
   public synchronized void addConsumer(final Consumer consumer)
   {
      super.addConsumer(consumer);
      consumerStateMap.put(consumer, new ConsumerState(consumer));
   }

   @Override
   public synchronized boolean removeConsumer(final Consumer consumer)
   {
      final boolean removed = super.removeConsumer(consumer);
      if (removed)
      {
         final ConsumerState cs = consumerStateMap.remove(consumer);
         for (final SimpleString ss : cs.getGroupIds())
         {
            consumers.remove(ss);
         }

      }
      return removed;
   }

   /**
    * we need to find the next available consumer that doesn't have a binding. If there are no free we use the next
    * available in the normal Round Robin fashion.
    * @param message
    * @param redeliver
    * @param groupId
    * @return
    */
   private ConsumerState getNextPositionAndBind(final ServerMessage message,
                                                final boolean redeliver,
                                                final SimpleString groupId)
   {
      Consumer consumer = super.select(message, redeliver);
      final ConsumerState cs = consumerStateMap.get(consumer);
      // if there is only one return it
      if (getConsumerCount() == 1 || cs.isAvailable())
      {
         consumers.put(groupId, cs);
         cs.getGroupIds().add(groupId);
         return cs;
      }
      else
      {
         consumer = super.select(message, redeliver);
         ConsumerState ncs = consumerStateMap.get(consumer);
         while (!ncs.isAvailable())
         {
            consumer = super.select(message, redeliver);
            ncs = consumerStateMap.get(consumer);
            if (ncs == cs)
            {
               cs.getGroupIds().add(groupId);
               return cs;
            }
         }
         ncs.getGroupIds().add(groupId);
         return ncs;
      }
   }

   private void removeBinding(final SimpleString groupId, final ConsumerState consumerState)
   {
      consumerState.setAvailable(true);
      consumerState.getGroupIds().remove(groupId);
      consumers.remove(groupId);
   }

   /**
    * holds the current state of a consumer, is it available, what groups it is bound to etc.
    */
   class ConsumerState
   {
      private final Consumer consumer;

      private volatile boolean isBound = false;

      private volatile boolean available = true;

      private final List<SimpleString> groupIds = new ArrayList<SimpleString>();

      public ConsumerState(final Consumer consumer)
      {
         this.consumer = consumer;
      }

      public boolean isBound()
      {
         return isBound;
      }

      public void setBound(final boolean bound)
      {
         isBound = bound;
      }

      public boolean isAvailable()
      {
         return available;
      }

      public void setAvailable(final boolean available)
      {
         this.available = available;
      }

      public Consumer getConsumer()
      {
         return consumer;
      }

      public List<SimpleString> getGroupIds()
      {
         return groupIds;
      }

      @Override
      public boolean equals(final Object o)
      {
         if (this == o)
         {
            return true;
         }
         if (o == null || getClass() != o.getClass())
         {
            return false;
         }

         final ConsumerState that = (ConsumerState)o;

         if (!consumer.equals(that.consumer))
         {
            return false;
         }

         return true;
      }

      @Override
      public int hashCode()
      {
         return consumer.hashCode();
      }

   }
}
