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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * A FlowBindingFilter
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Jan 2009 20:53:20
 *
 *
 */
public class FlowBindingFilter implements Filter
{
   private volatile QueueInfo info;
   
   private final Map<String, Filter> filters = new HashMap<String, Filter>();
   
   public FlowBindingFilter(final QueueInfo info) throws Exception
   {
      this.info = info;
      
      updateInfo(info);
   }
   
   public void updateInfo(final QueueInfo info) throws Exception
   {
      this.info = info;
      
      List<String> filterStrings = info.getFilterStrings();
      
      for (String filterString: filterStrings)
      {
         Filter filter = filters.get(filterString);
         
         if (filter == null)
         {
            filter = new FilterImpl(new SimpleString(filterString));
            
            filters.put(filterString, filter);
         }
      }
      
      //TODO - this can be optimised by storing map of filters in QueueInfo
      if (filterStrings.size() < filters.size())
      {
         Iterator<String> iter = filters.keySet().iterator();
         
         while (iter.hasNext())
         {
            String filterString = iter.next();
            
            if (!filterStrings.contains(filterString))
            {
               iter.remove();
            }
         }
      }
   }
         
   public SimpleString getFilterString()
   {         
      return null;
   }

   public boolean match(final ServerMessage message)
   {
      if (info.getNumberOfConsumers() == 0)
      {
         return false;
      }
      
      if (filters.isEmpty())
      {
         return true;
      }
      else
      {         
         for (Filter filter: filters.values())
         {
            if (filter.match(message))
            {
               return true;
            }
         }
         return false;
      }
   }
   
}
