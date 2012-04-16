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

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;

/**
 * A LocalQueueBinding
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 28 Jan 2009 12:42:23
 *
 *
 */
public class LocalQueueBinding implements QueueBinding
{
   private final SimpleString address;

   private final Queue queue;

   private final Filter filter;

   private final SimpleString name;

   private final SimpleString clusterName;

   public LocalQueueBinding(final SimpleString address, final Queue queue, final SimpleString nodeID)
   {
      this.address = address;

      this.queue = queue;

      filter = queue.getFilter();

      name = queue.getName();

      clusterName = name.concat(nodeID);
   }

   public long getID()
   {
      return queue.getID();
   }

   public Filter getFilter()
   {
      return filter;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public Bindable getBindable()
   {
      return queue;
   }

   public Queue getQueue()
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

   public SimpleString getClusterName()
   {
      return clusterName;
   }

   public boolean isExclusive()
   {
      return false;
   }

   public int getDistance()
   {
      return 0;
   }

   public boolean isHighAcceptPriority(final ServerMessage message)
   {
      // It's a high accept priority if the queue has at least one matching consumer

      return queue.hasMatchingConsumer(message);
   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      queue.route(message, context);
   }

   public boolean isQueueBinding()
   {
      return true;
   }

   public int consumerCount()
   {
      return queue.getConsumerCount();
   }

   public BindingType getType()
   {
      return BindingType.LOCAL_QUEUE;
   }
   
   public void close() throws Exception
   {
      queue.close();
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "LocalQueueBinding [address=" + address +
             ", queue=" +
             queue +
             ", filter=" +
             filter +
             ", name=" +
             name +
             ", clusterName=" +
             clusterName +
             "]";
   }

}
