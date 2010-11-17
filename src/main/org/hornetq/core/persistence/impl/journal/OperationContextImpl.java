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

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.impl.SimpleWaitIOCallback;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.utils.ExecutorFactory;

/**
 * 
 * Each instance of OperationContextImpl is associated with an executor (usually an ordered Executor).
 * 
 * Tasks are hold until the operations are complete and executed in the natural order as soon as the operations are returned 
 * from replication and storage.
 * 
 * If there are no pending IO operations, the tasks are just executed at the callers thread without any context switch.
 * 
 * So, if you are doing operations that are not dependent on IO (e.g NonPersistentMessages) you wouldn't have any context switch.
 * 
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class OperationContextImpl implements OperationContext
{
   private static final Logger log = Logger.getLogger(OperationContextImpl.class);

   private static final ThreadLocal<OperationContext> threadLocalContext = new ThreadLocal<OperationContext>();

   public static void clearContext()
   {
      OperationContextImpl.threadLocalContext.set(null);
   }

   public static OperationContext getContext(final ExecutorFactory executorFactory)
   {
      OperationContext token = OperationContextImpl.threadLocalContext.get();
      if (token == null)
      {
         token = new OperationContextImpl(executorFactory.getExecutor());
         OperationContextImpl.threadLocalContext.set(token);
      }
      return token;
   }

   public static void setContext(final OperationContext context)
   {
      OperationContextImpl.threadLocalContext.set(context);
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

   private final Executor executor;

   private final AtomicInteger executorsPending = new AtomicInteger(0);

   public OperationContextImpl(final Executor executor)
   {
      super();
      this.executor = executor;
   }

   public void storeLineUp()
   {
      storeLineUp++;
   }

   public void replicationLineUp()
   {
      replicationLineUp++;
   }

   public synchronized void replicationDone()
   {
      replicated++;
      checkTasks();
   }

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
            tasks.add(new TaskHolder(completion));
         }
      }

      if (executeNow)
      {
         // Executing outside of any locks
         completion.done();
      }

   }

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
               // If set, we use an executor to avoid the server being single threaded
               execute(holder.task);

               iter.remove();
            }
            else
            {
               // End of list here. No other task will be completed after this
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
      try
      {
         executor.execute(new Runnable()
         {
            public void run()
            {
               // If any IO is done inside the callback, it needs to be done on a new context
               OperationContextImpl.clearContext();
               task.done();
               executorsPending.decrementAndGet();
            }
         });
      }
      catch (Throwable e)
      {
         OperationContextImpl.log.warn("Error on executor's submit", e);
         executorsPending.decrementAndGet();
         task.onError(HornetQException.INTERNAL_ERROR,
                      "It wasn't possible to complete IO operation - " + e.getMessage());
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationToken#complete()
    */
   public void complete()
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.asyncio.AIOCallback#onError(int, java.lang.String)
    */
   public synchronized void onError(final int errorCode, final String errorMessage)
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

      TaskHolder(final IOAsyncTask task)
      {
         storeLined = storeLineUp;
         replicationLined = replicationLineUp;
         this.task = task;
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.OperationContext#waitCompletion()
    */
   public void waitCompletion() throws Exception
   {
      waitCompletion(0);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.OperationContext#waitCompletion(long)
    */
   public boolean waitCompletion(final long timeout) throws Exception
   {
      SimpleWaitIOCallback waitCallback = new SimpleWaitIOCallback();
      executeOnCompletion(waitCallback);
      complete();
      if (timeout == 0)
      {
         waitCallback.waitCompletion();
         return true;
      }
      else
      {
         return waitCallback.waitCompletion(timeout);
      }
   }

}
