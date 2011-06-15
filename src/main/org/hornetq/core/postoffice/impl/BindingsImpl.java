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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.Proposal;
import org.hornetq.core.server.group.impl.Response;

/**
 * A BindingsImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *         Created 11 Dec 2008 08:34:33
 *
 *
 */
public class BindingsImpl implements Bindings
{
   private static final Logger log = Logger.getLogger(BindingsImpl.class);

   private static boolean isTrace = log.isTraceEnabled();

   private final ConcurrentMap<SimpleString, List<Binding>> routingNameBindingMap = new ConcurrentHashMap<SimpleString, List<Binding>>();

   private final Map<SimpleString, Integer> routingNamePositions = new ConcurrentHashMap<SimpleString, Integer>();

   private final Map<Long, Binding> bindingsMap = new ConcurrentHashMap<Long, Binding>();

   private final List<Binding> exclusiveBindings = new CopyOnWriteArrayList<Binding>();

   private volatile boolean routeWhenNoConsumers;

   private final GroupingHandler groupingHandler;

   private final PagingStore pageStore;

   private final SimpleString name;

   public BindingsImpl(final SimpleString name, final GroupingHandler groupingHandler, final PagingStore pageStore)
   {
      this.groupingHandler = groupingHandler;
      this.pageStore = pageStore;
      this.name = name;
   }

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
      if (isTrace)
      {
         log.trace("addBinding(" + binding + ") being called");
      }
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

      if (isTrace)
      {
         log.trace("Adding binding " + binding + " into " + this + " bindingTable: " + debugBindings());
      }

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

      if (isTrace)
      {
         log.trace("Removing binding " + binding + " into " + this + " bindingTable: " + debugBindings());
      }
   }

   public boolean redistribute(final ServerMessage message, final Queue originatingQueue, final RoutingContext context) throws Exception
   {

      if (routeWhenNoConsumers)
      {
         return false;
      }

      if (isTrace)
      {
         log.trace("Redistributing message " + message);
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
         theBinding.route(message, context);

         return true;
      }
      else
      {
         return false;
      }
   }

   public PagingStore getPagingStore()
   {
      return pageStore;
   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      boolean routed = false;

      if (!exclusiveBindings.isEmpty())
      {
         for (Binding binding : exclusiveBindings)
         {
            if (binding.getFilter() == null || binding.getFilter().match(message))
            {
               binding.getBindable().route(message, context);

               routed = true;
            }
         }
      }

      if (!routed)
      {
         // TODO this is a little inefficient since we do the lookup once to see if the property
         // is there, then do it again to remove the actual property
         if (message.containsProperty(MessageImpl.HDR_ROUTE_TO_IDS))
         {
            routeFromCluster(message, context);
         }
         else if (groupingHandler != null && message.containsProperty(Message.HDR_GROUP_ID))
         {
            routeUsingStrictOrdering(message, context, groupingHandler);
         }
         else
         {
            if (isTrace)
            {
               log.trace("Routing message " + message + " on binding=" + this);
            }
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

               Binding theBinding = getNextBinding(message, routingName, bindings);

               if (theBinding != null)
               {
                  theBinding.route(message, context);
               }
            }
         }
      }
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "BindingsImpl [name=" + name + "]";
   }

   private Binding getNextBinding(final ServerMessage message,
                                  final SimpleString routingName,
                                  final List<Binding> bindings)
   {
      Integer ipos = routingNamePositions.get(routingName);

      int pos = ipos != null ? ipos : 0;

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
      if (pos != startPos)
      {
         routingNamePositions.put(routingName, pos);
      }
      return theBinding;
   }

   private void routeUsingStrictOrdering(final ServerMessage message,
                                         final RoutingContext context,
                                         final GroupingHandler groupingGroupingHandler) throws Exception
   {
      SimpleString groupId = message.getSimpleStringProperty(Message.HDR_GROUP_ID);

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

         // concat a full group id, this is for when a binding has multiple bindings
         SimpleString fullID = groupId.concat(".").concat(routingName);

         // see if there is already a response
         Response resp = groupingGroupingHandler.getProposal(fullID);

         if (resp == null)
         {
            // ok lets find the next binding to propose
            Binding theBinding = getNextBinding(message, routingName, bindings);
            // TODO https://jira.jboss.org/jira/browse/HORNETQ-191
            resp = groupingGroupingHandler.propose(new Proposal(fullID, theBinding.getClusterName()));

            // if our proposal was declined find the correct binding to use
            if (resp.getAlternativeClusterName() != null)
            {
               theBinding = null;
               for (Binding binding : bindings)
               {
                  if (binding.getClusterName().equals(resp.getAlternativeClusterName()))
                  {
                     theBinding = binding;
                     break;
                  }
               }
            }

            // and lets route it
            if (theBinding != null)
            {
               theBinding.route(message, context);
            }
            else
            {
               throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST,
                                          "queue " + resp.getChosenClusterName() +
                                                   " has been removed cannot deliver message, queues should not be removed when grouping is used");
            }
         }
         else
         {
            // ok, we need to find the binding and route it
            Binding chosen = null;
            for (Binding binding : bindings)
            {
               if (binding.getClusterName().equals(resp.getChosenClusterName()))
               {
                  chosen = binding;
                  break;
               }
            }
            if (chosen != null)
            {
               chosen.route(message, context);
            }
            else
            {
               throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST,
                                          "queue " + resp.getChosenClusterName() +
                                                   " has been removed cannot deliver message, queues should not be removed when grouping is used");
            }
         }
      }
   }
   
   private String debugBindings()
   {
      StringWriter writer = new StringWriter();

      PrintWriter out = new PrintWriter(writer);

      out.println("\n***************************************");

      out.println("routingNameBindingMap:");
      if (routingNameBindingMap.isEmpty())
      {
         out.println("EMPTY!");
      }
      for (Map.Entry<SimpleString, List<Binding>> entry : routingNameBindingMap.entrySet())
      {
         out.print("key=" + entry.getKey() + ", value=" + entry.getValue());
//         for (Binding bind : entry.getValue())
//         {
//            out.print(bind + ",");
//         }
         out.println();
      }
      
      out.println();
      
      out.println("RoutingNamePositions:");
      if (routingNamePositions.isEmpty())
      {
         out.println("EMPTY!");
      }
      for (Map.Entry<SimpleString, Integer> entry : routingNamePositions.entrySet())
      {
         out.println("key=" + entry.getKey() + ", value=" + entry.getValue());
      }
      
      out.println();
      
      out.println("BindingsMap:");
      
      if (bindingsMap.isEmpty())
      {
         out.println("EMPTY!");
      }
      for (Map.Entry<Long, Binding> entry : bindingsMap.entrySet())
      {
         out.println("Key=" + entry.getKey() + ", value=" + entry.getValue());
      }
      
      out.println();
      
      out.println("ExclusiveBindings:");
      if (exclusiveBindings.isEmpty())
      {
         out.println("EMPTY!");
      }
      
      for (Binding binding: exclusiveBindings)
      {
         out.println(binding);
      }

      out.println("#####################################################");


      return writer.toString();
   }


   private void routeFromCluster(final ServerMessage message, final RoutingContext context) throws Exception
   {
      byte[] ids = (byte[])message.removeProperty(MessageImpl.HDR_ROUTE_TO_IDS);

      ByteBuffer buff = ByteBuffer.wrap(ids);

      while (buff.hasRemaining())
      {
         long bindingID = buff.getLong();

         Binding binding = bindingsMap.get(bindingID);

         if (binding != null)
         {
            binding.route(message, context);
         }
      }
   }

   private final int incrementPos(int pos, final int length)
   {
      pos++;

      if (pos == length)
      {
         pos = 0;
      }

      return pos;
   }

}
