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

package org.hornetq.core.server.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RouteContextList;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.transaction.Transaction;

/**
 * A RoutingContextImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class RoutingContextImpl implements RoutingContext
{
   
   // The pair here is Durable and NonDurable
   private final Map<SimpleString, RouteContextList> map = new HashMap<SimpleString, RouteContextList>();

   private Transaction transaction;

   private int queueCount;

   public RoutingContextImpl(final Transaction transaction)
   {
      this.transaction = transaction;
   }
   
   public void clear()
   {
      transaction = null;

      map.clear();
      
      queueCount = 0;
   }

   public void addQueue(final SimpleString address, final Queue queue)
   {

      RouteContextList listing = getContextListing(address);
      
      if (queue.isDurable())
      {
         listing.getDurableQueues().add(queue);
      }
      else
      {
         listing.getNonDurableQueues().add(queue);
      }

      queueCount++;
   }
   
   public RouteContextList getContextListing(SimpleString address)
   {
      RouteContextList listing = map.get(address);
      if (listing == null)
      {
         listing = new ContextListing();
         map.put(address, listing);
      }
      return listing;
   }

   public Transaction getTransaction()
   {
      return transaction;
   }

   public void setTransaction(final Transaction tx)
   {
      transaction = tx;
   }

   public List<Queue> getNonDurableQueues(SimpleString address)
   {
      return getContextListing(address).getNonDurableQueues();
   }

   public List<Queue> getDurableQueues(SimpleString address)
   {
      return getContextListing(address).getDurableQueues();
   }

   public int getQueueCount()
   {
      return queueCount;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.RoutingContext#getAddresses()
    */
   public Map<SimpleString, RouteContextList> getContexListing()
   {
      return this.map;
   }
   
   
   private class ContextListing implements RouteContextList
   {
      private final List<Queue> durableQueue = new ArrayList<Queue>(1);
      
      private final List<Queue> nonDurableQueue = new ArrayList<Queue>(1);
      
      public int getNumberOfDurableQueues()
      {
         return durableQueue.size();
      }
      
      public int getNumberOfNonDurableQueues()
      {
         return nonDurableQueue.size();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.RouteContextList#getDurableQueues()
       */
      public List<Queue> getDurableQueues()
      {
         return durableQueue;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.RouteContextList#getNonDurableQueues()
       */
      public List<Queue> getNonDurableQueues()
      {
         return nonDurableQueue;
      }
   }

}
