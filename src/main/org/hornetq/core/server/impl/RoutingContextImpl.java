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
import java.util.List;

import org.hornetq.core.server.Queue;
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
   private final List<Queue> queues = new ArrayList<Queue>();

   private Transaction transaction;

   private int depth;

   public RoutingContextImpl(final Transaction transaction)
   {
      this.transaction = transaction;
   }

   public void addQueue(final Queue queue)
   {
      queues.add(queue);
   }

   public Transaction getTransaction()
   {
      return transaction;
   }

   public void setTransaction(final Transaction tx)
   {
      transaction = tx;
   }

   public List<Queue> getQueues()
   {
      return queues;
   }

   public void decrementDepth()
   {
      depth--;
   }

   public int getDepth()
   {
      return depth;
   }

   public void incrementDepth()
   {
      depth++;
   }

}
