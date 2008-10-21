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

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;

/**
 * A RoundRobinDistributionPolicy
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class RoundRobinDistributionPolicy extends DistributionPolicyImpl
{
   Logger log = Logger.getLogger(RoundRobinDistributionPolicy.class);

   protected int pos = 0;

   @Override
   public synchronized void addConsumer(final Consumer consumer)
   {
      pos = 0;
      super.addConsumer(consumer);
   }

   @Override
   public synchronized boolean removeConsumer(final Consumer consumer)
   {
      pos = 0;
      return super.removeConsumer(consumer);
   }
   
   public synchronized int getConsumerCount()
   {
      return super.getConsumerCount();
   }

   public HandleStatus distribute(final MessageReference reference)
   {
      if (getConsumerCount() == 0)
      {
         return HandleStatus.BUSY;
      }
      int startPos = pos;
      boolean filterRejected = false;
      HandleStatus status;
      while (true)
      {
         status = handle(reference, getNextConsumer());

         if (status == HandleStatus.HANDLED)
         {
            return HandleStatus.HANDLED;
         }
         else if (status == HandleStatus.NO_MATCH)
         {
            filterRejected = true;
         }
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

   protected synchronized Consumer getNextConsumer()
   {
      Consumer consumer = consumers.get(pos);
      incrementPosition();
      return consumer;
   }

   protected void incrementPosition()
   {
      pos++;
      if (pos == consumers.size())
      {
         pos = 0;
      }
   }

   protected HandleStatus handle(final MessageReference reference, final Consumer consumer)
   {
      HandleStatus status;
      try
      {
         status = consumer.handle(reference);
      }
      catch (Throwable t)
      {
         log.warn("removing consumer which did not handle a message, " + "consumer=" +
                  consumer +
                  ", message=" +
                  reference, t);

         // If the consumer throws an exception we remove the consumer
         try
         {
            removeConsumer(consumer);
         }
         catch (Exception e)
         {
            log.error("Failed to remove consumer", e);
         }

         return HandleStatus.BUSY;
      }

      if (status == null)
      {
         throw new IllegalStateException("ClientConsumer.handle() should never return null");
      }
      return status;
   }
}
