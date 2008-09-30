/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.util;

import java.util.LinkedList;
import java.util.concurrent.Executor;

/**
 * A OrderedExecutorFactory2
 *
 * @author <a href="mailto:david.lloyd@jboss.com">David LLoyd</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
                     // eat it!
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
      public void execute(Runnable command)
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
   }
}
