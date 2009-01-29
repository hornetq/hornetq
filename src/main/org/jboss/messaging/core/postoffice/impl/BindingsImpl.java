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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
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

   private final Map<Integer, Binding> bindingsMap = new ConcurrentHashMap<Integer, Binding>();

   private final List<Binding> exclusiveBindings = new CopyOnWriteArrayList<Binding>();

   public Collection<Binding> getBindings()
   {
      return bindingsMap.values();
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

      bindingsMap.put(binding.getID(), binding);
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

      bindingsMap.remove(binding.getID());
   }

   private void routeFromCluster(final ServerMessage message, final Transaction tx) throws Exception
   {
      byte[] ids = (byte[])message.getProperty(MessageImpl.HDR_ROUTE_TO_IDS);
      
      ByteBuffer buff = ByteBuffer.wrap(ids);
      
      Set<Bindable> chosen = new HashSet<Bindable>();
      
      while (buff.hasRemaining())
      {
         int bindingID = buff.getInt();
         
         Binding binding = bindingsMap.get(bindingID);
         
         if (binding == null)
         {
            //The binding has been closed - we need to route the message somewhere else...............
            throw new IllegalStateException("Binding not found when routing from cluster - it must have closed");
            
            //FIXME need to deal with this better            
         }
         
         binding.willRoute(message);
         
         chosen.add(binding.getBindable());
      }
      
      for (Bindable bindable : chosen)
      {
         bindable.preroute(message, tx);
      }
      
      for (Bindable bindable : chosen)
      {
         bindable.route(message, tx);
      }
   }

   public void route(final ServerMessage message, final Transaction tx) throws Exception
   {
      if (!exclusiveBindings.isEmpty())
      {
         for (Binding binding : exclusiveBindings)
         {
            binding.getBindable().route(message, tx);
         }
      }
      else
      {
         if (message.getProperty(MessageImpl.HDR_FROM_CLUSTER) != null)
         {
            routeFromCluster(message, tx);
         }
         else
         {
            Set<Bindable> chosen = new HashSet<Bindable>();
   
            for (Map.Entry<SimpleString, List<Binding>> entry : routingNameBindingMap.entrySet())
            {
               SimpleString routingName = entry.getKey();
   
               List<Binding> bindings = entry.getValue();
   
               if (bindings == null)
               {
                  // The value can become null if it's concurrently removed while we're iterating - this is expected
                  // ConcurrentHashMap behaviour!
                  continue;
               }
   
               Integer ipos = routingNamePositions.get(routingName);
   
               int pos = ipos != null ? ipos.intValue() : 0;
   
               int length = bindings.size();
   
               int startPos = pos;
   
               Binding theBinding = null;
   
               int lastNoMatchingConsumerPos = -1;
   
               while (true)
               {
                  Binding binding;
                  try
                  {
                     binding = bindings.get(pos);
                  }
                  catch (IndexOutOfBoundsException e)
                  {
                     // This can occur if binding is removed while in route
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
   
                  if (binding.filterMatches(message))
                  {
                     // bindings.length == 1 ==> only a local queue so we don't check for matching consumers (it's an
                     // unnecessary overhead)
                     if (length == 1 || binding.isHighAcceptPriority(message))
                     {
                        theBinding = binding;
   
                        pos = incrementPos(pos, length);
   
                        break;
                     }
                     else
                     {
                        lastNoMatchingConsumerPos = pos;
                     }
                  }
   
                  pos = incrementPos(pos, length);
   
                  if (pos == startPos)
                  {
                     if (lastNoMatchingConsumerPos != -1)
                     {                     
                        try
                        {
                           theBinding = bindings.get(pos);
                        }
                        catch (IndexOutOfBoundsException e)
                        {
                           // This can occur if binding is removed while in route
                           if (!bindings.isEmpty())
                           {
                              pos = 0;
                              
                              lastNoMatchingConsumerPos = -1;
   
                              continue;
                           }
                           else
                           {
                              break;
                           }
                        }
                                            
                        pos = lastNoMatchingConsumerPos;
   
                        pos = incrementPos(pos, length);
                     }
                     break;
                  }
               }
   
               if (theBinding != null)
               {
                  theBinding.willRoute(message);
                  
                  chosen.add(theBinding.getBindable());
               }
   
               routingNamePositions.put(routingName, pos);
            }
   
            //TODO refactor to do this is one iteration
            
            for (Bindable bindable : chosen)
            {
               bindable.preroute(message, tx);
            }
            
            for (Bindable bindable : chosen)
            {
               bindable.route(message, tx);
            }
         }
      }
   }

   private final int incrementPos(int pos, int length)
   {
      pos++;

      if (pos == length)
      {
         pos = 0;
      }

      return pos;
   }

}
