/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.core.postoffice.impl;

import java.util.List;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * A LocalQueueBinding
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 28 Jan 2009 12:42:23
 *
 *
 */
public class LocalQueueBinding implements Binding
{
   private final SimpleString address;
   
   private final Queue queue;
   
   private final Filter filter;
   
   private final SimpleString name;
   
   public LocalQueueBinding(final SimpleString address, final Queue queue)
   {
      this.address = address;
      
      this.queue = queue;
      
      this.filter = queue.getFilter();
      
      this.name = queue.getName();
   }
      
   public boolean filterMatches(final ServerMessage message) throws Exception
   {
      if (filter != null && !filter.match(message))
      {
         return false;
      }
      else
      {
         return true;
      }
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public Bindable getBindable()
   {
      return queue;
   }

   public SimpleString getRoutingName()
   {
      return name;
   }

   public SimpleString getUniqueName()
   {
      return name;
   }

   public boolean isExclusive()
   {
      return false;
   }

   public boolean isHighAcceptPriority(final ServerMessage message)
   {
      //It's a high accept priority if the queue has at least one matching consumer
      
      List<Consumer> consumers = queue.getConsumers();
      
      for (Consumer consumer: consumers)
      {
         Filter filter = consumer.getFilter();
         
         if (filter == null)
         {
            return true;
         }
         else
         {
            if (filter.match(message))
            {
               return true;
            }
         }
      }
      
      return false;
   }

   public boolean isQueueBinding()
   {
      return true;
   }

}
