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

package org.hornetq.core.persistence.impl.journal;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.utils.ExecutorFactory;

/**
 * 
 * This class will hold operations when there are IO operations...
 * and it will 
 * 
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class OperationContextImpl implements OperationContext
{
   
   private static final ThreadLocal<OperationContext> threadLocalContext = new ThreadLocal<OperationContext>();

   public static void clearContext()
   {
      threadLocalContext.set(null);
   }
   
   public static OperationContext getContext(final ExecutorFactory executorFactory)
   {
      OperationContext token = threadLocalContext.get();
      if (token == null)
      {
         token = new OperationContextImpl(executorFactory.getExecutor());
         threadLocalContext.set(token);
      }
      return token;
   }
   
   public static void setContext(OperationContext context)
   {
      threadLocalContext.set(context);
   }
   
   
   private List<TaskHolder> tasks;

   private volatile int storeLineUp = 0;

   private volatile int replicationLineUp = 0;

   private int minimalStore = Integer.MAX_VALUE;

   private int minimalReplicated = Integer.MAX_VALUE;

   private int stored = 0;

   private int replicated = 0;

   private int errorCode = -1;

   private String errorMessage = null;

   private Executor executor;

   private final AtomicInteger executorsPending = new AtomicInteger(0);

   public OperationContextImpl(final Executor executor)
   {
      super();
      this.executor = executor;
   }

   /** To be called by the replication manager, when new replication is added to the queue */
   public void lineUp()
   {
      storeLineUp++;
   }

   public void replicationLineUp()
   {
      replicationLineUp++;
   }

   /** this method needs to be called before the executor became operational */
   public void setExecutor(Executor executor)
   {
      this.executor = executor;
   }

   public synchronized void replicationDone()
   {
      replicated++;
      checkTasks();
   }

   /** You may have several actions to be done after a replication operation is completed. */
   public void executeOnCompletion(final IOAsyncTask completion)
   {
      if (errorCode != -1)
      {
         completion.onError(errorCode, errorMessage);
         return;
      }

      boolean executeNow = false;

      synchronized (this)
      {
         if (tasks == null)
         {
            tasks = new LinkedList<TaskHolder>();
            minimalReplicated = replicationLineUp;
            minimalStore = storeLineUp;
         }

         // On this case, we can just execute the context directly
         if (replicationLineUp == replicated && storeLineUp == stored)
         {
            if (executor != null)
            {
               // We want to avoid the executor if everything is complete...
               // However, we can't execute the context if there are executions pending
               // We need to use the executor on this case
               if (executorsPending.get() == 0)
               {
                  // No need to use an executor here or a context switch
                  // there are no actions pending.. hence we can just execute the task directly on the same thread
                  executeNow = true;
               }
               else
               {
                  execute(completion);
               }
            }
            else
            {
               executeNow = true;
            }
         }
         else
         {
            tasks.add(new TaskHolder(completion));
         }
      }
      
      if (executeNow)
      {
         // Executing outside of any locks
         completion.done();
      }

   }

   /** To be called by the storage manager, when data is confirmed on the channel */
   public synchronized void done()
   {
      stored++;
      checkTasks();
   }

   private void checkTasks()
   {
      if (stored >= minimalStore && replicated >= minimalReplicated)
      {
         Iterator<TaskHolder> iter = tasks.iterator();
         while (iter.hasNext())
         {
            TaskHolder holder = iter.next();
            if (stored >= holder.storeLined && replicated >= holder.replicationLined)
            {
               if (executor != null)
               {
                  // If set, we use an executor to avoid the server being single threaded
                  execute(holder.task);
               }
               else
               {
                  holder.task.done();
               }

               iter.remove();
            }
            else
            {
               // The actions need to be done in order...
               // so it must achieve both conditions before we can proceed to more tasks
               break;
            }
         }
      }
   }

   /**
    * @param holder
    */
   private void execute(final IOAsyncTask task)
   {
      executorsPending.incrementAndGet();
      executor.execute(new Runnable()
      {
         public void run()
         {
            // If any IO is done inside the callback, it needs to be done on a new context
            clearContext();
            task.done();
            executorsPending.decrementAndGet();
         }
      });
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationToken#complete()
    */
   public void complete()
   {
      // We hold errors until the complete is set, or the callbacks will never get informed
      errorCode = -1;
      errorMessage = null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.asyncio.AIOCallback#onError(int, java.lang.String)
    */
   public synchronized void onError(int errorCode, String errorMessage)
   {
      this.errorCode = errorCode;
      this.errorMessage = errorMessage;

      if (tasks != null)
      {
         Iterator<TaskHolder> iter = tasks.iterator();
         while (iter.hasNext())
         {
            TaskHolder holder = iter.next();
            holder.task.onError(errorCode, errorMessage);
            iter.remove();
         }
      }
   }

   class TaskHolder
   {
      int storeLined;

      int replicationLined;

      IOAsyncTask task;

      TaskHolder(IOAsyncTask task)
      {
         this.storeLined = storeLineUp;
         this.replicationLined = replicationLineUp;
         this.task = task;
      }
   }

}
