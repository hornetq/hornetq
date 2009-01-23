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

package org.jboss.messaging.core.server.cluster.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.FlowBinding;
import org.jboss.messaging.util.SimpleString;

/**
 * A FlowBindingImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Jan 2009 18:55:22
 *
 *
 */
public class FlowBindingImpl extends BindingImpl implements FlowBinding
{
   private final Set<Filter> filters = new HashSet<Filter>();

   private final Map<SimpleString, Integer> filterCounts = new HashMap<SimpleString, Integer>();

   private int consumerCount;
   
   private final boolean duplicateDetection;

   public FlowBindingImpl(final SimpleString address,
                          final SimpleString uniqueName,
                          final SimpleString routingName,
                          final Bindable bindable,
                          final boolean duplicateDetection)
   {
      super(address, uniqueName, routingName, bindable, false, false);
      
      this.duplicateDetection = duplicateDetection;
   }

   public boolean accept(final ServerMessage message) throws Exception
   {
      if (consumerCount == 0)
      {
         return false;
      }

      boolean accepted = false;
      
      if (filters.isEmpty())
      {
         accepted = true;
      }
      else
      {
         for (Filter filter : filters)
         {
            if (filter.match(message))
            {
               accepted = true;
               
               break;
            }
         }         
      }
      
      if (duplicateDetection && accepted)
      {
         if (!message.containsProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID))
         {
            //Add the message id as a duplicate id header - this will be detected when routing on the remote node - 
            //any duplicates will be rejected
            byte[] bytes = new byte[8];
            
            ByteBuffer buff = ByteBuffer.wrap(bytes);
            
            buff.putLong(message.getMessageID());
            
            SimpleString dupID = new SimpleString(bytes);
            
            message.putStringProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID, dupID);                       
         }
      }
      
      return accepted;
   }

   public synchronized void addConsumer(final SimpleString filterString) throws Exception
   {
      if (filterString != null)
      {
         // There can actually be many consumers on the same queue with the same filter, so we need to maintain a ref
         // count

         Integer i = filterCounts.get(filterString);

         if (i == null)
         {
            filterCounts.put(filterString, 0);

            filters.add(new FilterImpl(filterString));
         }
         else
         {
            filterCounts.put(filterString, i + 1);
         }
      }

      consumerCount++;
   }

   public synchronized void removeConsumer(final SimpleString filterString) throws Exception
   {
      if (filterString != null)
      {
         Integer i = filterCounts.get(filterString);

         if (i != null)
         {
            int ii = i - 1;

            if (ii == 0)
            {
               filterCounts.remove(filterString);

               filters.remove(filterString);
            }
            else
            {
               filterCounts.put(filterString, ii);
            }
         }
      }

      consumerCount--;
   }

}
