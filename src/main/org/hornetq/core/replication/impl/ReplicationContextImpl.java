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
import java.util.concurrent.Executor;

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
   final Executor executor;
   
   private ArrayList<Runnable> tasks;
   
   private volatile int pendings;
   
   /**
    * @param executor
    */
   public ReplicationContextImpl(Executor executor)
   {
      super();
      this.executor = executor;
   }

   /** To be called by the replication manager, when new replication is added to the queue */
   public synchronized void linedUp()
   {
      pendings++;
   }

   /** To be called by the replication manager, when data is confirmed on the channel */
   public synchronized void replicated()
   {
      if (--pendings == 0)
      {
         if (tasks != null)
         {
            for (Runnable run : tasks)
            {
               executor.execute(run);
            }
            tasks.clear();
         }
      }
   }
   
   /** You may have several actions to be done after a replication operation is completed. */
   public synchronized void addReplicationAction(Runnable runnable)
   {
      if (pendings == 0)
      {
         executor.execute(runnable);
      }
      else
      {
         if (tasks == null)
         {
            tasks = new ArrayList<Runnable>();
         }
         
         tasks.add(runnable);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationToken#complete()
    */
   public void complete()
   {
      // TODO Auto-generated method stub
      
   }
}
