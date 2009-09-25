/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.postoffice.impl;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.SimpleString;

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

   private volatile boolean routeWhenNoConsumers;

   public void setRouteWhenNoConsumers(final boolean routeWhenNoConsumers)
   {
      this.routeWhenNoConsumers = routeWhenNoConsumers;
   }

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
      byte[] ids = (byte[])message.removeProperty(MessageImpl.HDR_ROUTE_TO_IDS);

      ByteBuffer buff = ByteBuffer.wrap(ids);

      Set<Bindable> chosen = new HashSet<Bindable>();

      while (buff.hasRemaining())
      {
         int bindingID = buff.getInt();

         Binding binding = bindingsMap.get(bindingID);

         if (binding == null)
         {
            // The binding has been closed - we need to route the message somewhere else...............
            throw new IllegalStateException("Binding not found when routing from cluster - it must have closed " + bindingID);

            // FIXME need to deal with this better
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

   public boolean redistribute(final ServerMessage message, final Queue originatingQueue, final Transaction tx) throws Exception
   {
      if (routeWhenNoConsumers)
      {
         return false;
      }
      
      SimpleString routingName = originatingQueue.getName();

      List<Binding> bindings = routingNameBindingMap.get(routingName);

      if (bindings == null)
      {
         // The value can become null if it's concurrently removed while we're iterating - this is expected
         // ConcurrentHashMap behaviour!
         return false;
      }

      Integer ipos = routingNamePositions.get(routingName);

      int pos = ipos != null ? ipos.intValue() : 0;

      int length = bindings.size();

      int startPos = pos;

      Binding theBinding = null;

      // TODO - combine this with similar logic in route()
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
               startPos = 0;
               length = bindings.size();

               continue;
            }
            else
            {
               break;
            }
         }

         pos = incrementPos(pos, length);

         Filter filter = binding.getFilter();

         boolean highPrior = binding.isHighAcceptPriority(message);

         if (highPrior && binding.getBindable() != originatingQueue && (filter == null || filter.match(message)))
         {
            theBinding = binding;

            break;
         }

         if (pos == startPos)
         {
            break;
         }
      }

      routingNamePositions.put(routingName, pos);

      if (theBinding != null)
      {
         theBinding.willRoute(message);

         theBinding.getBindable().preroute(message, tx);

         theBinding.getBindable().route(message, tx);

         return true;
      }
      else
      {
         return false;
      }
   }

   public void route(final ServerMessage message, final Transaction tx) throws Exception
   {
      boolean routed = false;
      
      if (!exclusiveBindings.isEmpty())
      {
         for (Binding binding : exclusiveBindings)
         {
            if (binding.getFilter() == null || binding.getFilter().match(message))
            {
               binding.getBindable().route(message, tx);
               
               routed = true;
            }
         }
      }

      if (!routed)
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

               int lastLowPriorityBinding = -1;

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
                        startPos = 0;
                        length = bindings.size();

                        continue;
                     }
                     else
                     {
                        break;
                     }
                  }

                  Filter filter = binding.getFilter();

                  if (filter == null || filter.match(message))
                  {
                     // bindings.length == 1 ==> only a local queue so we don't check for matching consumers (it's an
                     // unnecessary overhead)
                     if (length == 1 || routeWhenNoConsumers || binding.isHighAcceptPriority(message))
                     {
                        theBinding = binding;

                        pos = incrementPos(pos, length);

                        break;
                     }
                     else
                     {
                        if (lastLowPriorityBinding == -1)
                        {
                           lastLowPriorityBinding = pos;
                        }
                     }
                  }

                  pos = incrementPos(pos, length);

                  if (pos == startPos)
                  {
                     if (lastLowPriorityBinding != -1)
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

                              lastLowPriorityBinding = -1;

                              continue;
                           }
                           else
                           {
                              break;
                           }
                        }

                        pos = lastLowPriorityBinding;

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

            // TODO refactor to do this is one iteration

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
