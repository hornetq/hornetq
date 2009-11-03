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

package org.hornetq.core.replication.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.replication.ReplicationContext;

/**
 * A ReplicationToken
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationContextImpl implements ReplicationContext
{
   private List<Runnable> tasks;
   
   private AtomicInteger pendings = new AtomicInteger(0);
   
   private volatile boolean complete = false;
   
   /**
    * @param executor
    */
   public ReplicationContextImpl()
   {
      super();
   }

   /** To be called by the replication manager, when new replication is added to the queue */
   public void linedUp()
   {
      pendings.incrementAndGet();
   }

   /** You may have several actions to be done after a replication operation is completed. */
   public void addReplicationAction(Runnable runnable)
   {
      if (complete)
      {
         // Sanity check, this shouldn't happen
         throw new IllegalStateException("The Replication Context is complete, and no more tasks are accepted");
      }

      if (tasks == null)
      {
         // No need to use Concurrent, we only add from a single thread.
         // We don't add any more Runnables after it is complete
         tasks = new ArrayList<Runnable>();
      }
      
      tasks.add(runnable);
   }

   /** To be called by the replication manager, when data is confirmed on the channel */
   public synchronized void replicated()
   {
      if (pendings.decrementAndGet() == 0 && complete)
      {
         flush();
      }
   }


   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationToken#complete()
    */
   public synchronized void complete()
   {
      complete = true;
      if (pendings.get() == 0 && complete)
      {
         flush();
      }
  }
   
   public synchronized void flush()
   {
      if (tasks != null)
      {
         for (Runnable run : tasks)
         {
            run.run();
         }
         tasks.clear();
      }
   }
   
   
}
