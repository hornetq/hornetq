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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.util.SimpleString;

/**
 * Distributes message based on the message property 'JBM_GROUP_ID'. Once a message has been successfully delivered to a
 * consumer that consumer is then bound to that group. Any message that has the same group id set will always be
 * delivered to the same consumer.
 * The Initial consumer is the first consumer found, using the round robin policy, that hasn't been bound to a group, If
 * there are no consumers left that have not been bound to a group then the next consumer will be bound to 2 groups and
 * so on.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class GroupingRoundRobinDistributor extends RoundRobinDistributor
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ConcurrentMap<SimpleString, Consumer> cons = new ConcurrentHashMap<SimpleString, Consumer>();

   // Distributor implementation ------------------------------------

   public HandleStatus distribute(final MessageReference reference)
   {
      if (consumers.isEmpty())
      {
         return HandleStatus.BUSY;
      }
      
      final SimpleString groupId = (SimpleString)reference.getMessage().getProperty(MessageImpl.HDR_GROUP_ID);
      
      if (groupId != null)
      {
         int startPos = pos;
         
         boolean filterRejected = false;

         while (true)
         {
            Consumer consumer = consumers.get(pos);
            
            Consumer oldConsumer = cons.putIfAbsent(groupId, consumer);

            if (oldConsumer == null)
            {
               incrementPosition();
            }
            else
            {
               consumer = oldConsumer;
            }

            HandleStatus status = handle(reference, consumer);
            
            if (status == HandleStatus.HANDLED)
            {
               return HandleStatus.HANDLED;
            }
            else if (status == HandleStatus.NO_MATCH)
            {
               filterRejected = true;
            }
            else if (status == HandleStatus.BUSY)
            {
               // if we were previously bound, we can remove and try the next consumer
               return HandleStatus.BUSY;
            }
            // if we've tried all of them
            if (startPos == pos)
            {
               // Tried all of them
               if (filterRejected)
               {
                  return HandleStatus.NO_MATCH;
               }
               else
               {
                  // Give up - all consumers busy
                  return HandleStatus.BUSY;
               }
            }
         }
      }
      else
      {
         return super.distribute(reference);
      }
   }

   public synchronized boolean removeConsumer(Consumer consumer)
   {
      boolean removed = super.removeConsumer(consumer);
      
      if (removed)
      {
         for (SimpleString group : cons.keySet())
         {
            if (consumer == cons.get(group))
            {
               cons.remove(group);
            }
         }
      }
      return removed;
   }
}
