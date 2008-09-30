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

import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.ServerMessage;

/**
 * 
 * A RoundRobinDistributionPolicy
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RoundRobinDistributionPolicy extends DistributionPolicyImpl
{
   int pos = -1;

   public Consumer select(ServerMessage message, boolean redeliver)
   {     
      if(consumers.isEmpty())
      {
         return null;
      }
      if (pos == -1)
      {
         //First time
         pos = 0;
         return consumers.get(pos);   
      }
      else
      {
         pos++;
         
         if (pos == consumers.size())
         {
            pos = 0;
         }
      }

      return consumers.get(pos);
   }

   public synchronized void addConsumer(Consumer consumer)
   {
      pos = -1;
      super.addConsumer(consumer);
   }

   public synchronized boolean removeConsumer(Consumer consumer)
   {

      pos = -1;
      return super.removeConsumer(consumer);
   }
}
