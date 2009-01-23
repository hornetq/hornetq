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

package org.jboss.messaging.core.postoffice.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.util.SimpleString;

/**
 * A BindingsImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 11 Dec 2008 08:34:33
 *
 *
 */
public class BindingsImpl implements Bindings
{   
   private static final Logger log = Logger.getLogger(BindingsImpl.class);

   private final ConcurrentMap<SimpleString, List<Binding>> routingNameBindingMap = new ConcurrentHashMap<SimpleString, List<Binding>>();
   
   private final Map<SimpleString, Integer> routingNamePositions = new ConcurrentHashMap<SimpleString, Integer>();
   
   private final List<Binding> bindingsList = new CopyOnWriteArrayList<Binding>();
   
   private final List<Binding> exclusiveBindings = new CopyOnWriteArrayList<Binding>();
   
   public List<Binding> getBindings()
   {
      return bindingsList;
   }

   public void addBinding(final Binding binding)
   {
      if (binding.isExclusive())
      {
         exclusiveBindings.add(binding);
      }
      else
      {
         SimpleString routingName = binding.getRoutingName();
         
         List<Binding> bindings = routingNameBindingMap.get(routingName);
         
         if (bindings == null)
         {
            bindings = new CopyOnWriteArrayList<Binding>();
            
            List<Binding> oldBindings = routingNameBindingMap.putIfAbsent(routingName, bindings);
            
            if (oldBindings != null)
            {
               bindings = oldBindings;
            }
         }
         
         bindings.add(binding);
      }
      
      bindingsList.add(binding);
   }
   
   public void removeBinding(final Binding binding)
   {
      if (binding.isExclusive())
      {
         exclusiveBindings.remove(binding);
      }
      else
      {
         SimpleString routingName = binding.getRoutingName();
         
         List<Binding> bindings = routingNameBindingMap.get(routingName);
         
         if (bindings != null)
         {
            bindings.remove(binding);
            
            if (bindings.isEmpty())
            {
               routingNameBindingMap.remove(routingName);
            }
         }
      }
      
      bindingsList.remove(binding);
   }
   
   public void route(ServerMessage message) throws Exception
   {
      route(message, null);
   }
   
   public void route(ServerMessage message, Transaction tx) throws Exception
   {
      if (!exclusiveBindings.isEmpty())
      {
         for (Binding binding: exclusiveBindings)
         {
            binding.getBindable().route(message, tx);
         }
      }
      else
      {
         Set<Bindable> chosen = new HashSet<Bindable>();
          
         for (Map.Entry<SimpleString, List<Binding>> entry: routingNameBindingMap.entrySet())
         {
            SimpleString routingName = entry.getKey();
            
            List<Binding> bindings = entry.getValue();
            
            if (bindings == null)
            {
               //The value can become null if it's concurrently removed while we're iterating - this is expected ConcurrentHashMap behaviour!
               continue;
            }
               
            Integer ipos = routingNamePositions.get(routingName);
            
            int pos = ipos != null ? ipos.intValue() : 0;
              
            int startPos = pos;
            
            int length = bindings.size();
              
            do
            {               
               Binding binding;
               
               try
               {
                  binding = bindings.get(pos);
               }
               catch (IndexOutOfBoundsException e)
               {
                  //This can occur if binding is removed while in route
                  if (!bindings.isEmpty())
                  {
                     pos = 0;
                     
                     continue;
                  }
                  else
                  {
                     break;
                  }
               }
               
               pos++;
               
               if (pos == length)
               {
                  pos = 0;
               }
               
               if (binding.accept(message))
               {
                  chosen.add(binding.getBindable());
                   
                  break;
               }
            }
            while (startPos != pos);
            
            if (pos != startPos)
            {
               routingNamePositions.put(routingName, pos);
            }
         }
                     
         for (Bindable bindable: chosen)
         {
            bindable.route(message, tx);
         }
      }      
   }
   
}
