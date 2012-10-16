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

package org.hornetq.utils;

import org.hornetq.core.HornetQCoreLogger;

import java.util.LinkedList;
import java.util.concurrent.Executor;


/**
 * A factory for producing executors that run all tasks in order, which delegate to a single common executor instance.
 *
 * @author <a href="david.lloyd@jboss.com">David Lloyd</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public final class OrderedExecutorFactory implements ExecutorFactory
{
   private final Executor parent;

   /**
    * Construct a new instance delegating to the given parent executor.
    *
    * @param parent the parent executor
    */
   public OrderedExecutorFactory(final Executor parent)
   {
      this.parent = parent;
   }

   /**
    * Get an executor that always executes tasks in order.
    *
    * @return an ordered executor
    */
   public Executor getExecutor()
   {
      return new OrderedExecutor(parent);
   }

   /**
    * An executor that always runs all tasks in order, using a delegate executor to run the tasks.
    * <p/>
    * More specifically, any call B to the {@link #execute(Runnable)} method that happens-after another call A to the
    * same method, will result in B's task running after A's.
    */
   private static final class OrderedExecutor implements Executor
   {
      // @protectedby tasks
      private final LinkedList<Runnable> tasks = new LinkedList<Runnable>();

      // @protectedby tasks
      private boolean running;

      private final Executor parent;

      private final Runnable runner;

      /**
       * Construct a new instance.
       *
       * @param parent the parent executor
       */
      public OrderedExecutor(final Executor parent)
      {
         this.parent = parent;
         runner = new Runnable()
         {
            public void run()
            {
               for (;;)
               {
                  final Runnable task;
                  synchronized (tasks)
                  {
                     task = tasks.poll();
                     if (task == null)
                     {
                        running = false;
                        return;
                     }
                  }
                  try
                  {
                     task.run();
                  }
                  catch (Throwable t)
                  {
                     HornetQCoreLogger.LOGGER.caughtunexpectedThrowable(t);
                  }
               }
            }
         };
      }

      /**
       * Run a task.
       *
       * @param command the task to run.
       */
      public void execute(final Runnable command)
      {
         synchronized (tasks)
         {
            tasks.add(command);
            if (!running)
            {
               running = true;
               parent.execute(runner);
            }
         }
      }

      public String toString()
      {
         return "OrderedExecutor(running=" + running + ", tasks=" + tasks + ")";
      }
   }
}
