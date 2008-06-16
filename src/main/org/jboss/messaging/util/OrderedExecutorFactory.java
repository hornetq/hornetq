/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * This factory creates a hierarchy of Executor which shares the threads of the
 * parent Executor (typically, the root parent is a Thread pool).
 * 
 * @author <a href="david.lloyd@jboss.com">David Lloyd</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public final class OrderedExecutorFactory implements ExecutorFactory
{
   private final Executor parent;
   private final Set<ChildExecutor> runningChildren = Collections.synchronizedSet(new HashSet<ChildExecutor>());

   public OrderedExecutorFactory(final Executor parent)
   {
      this.parent = parent;
   }

   public Executor getExecutor()
   {
      return new ChildExecutor();
   }

   private final class ChildExecutor implements Executor, Runnable
   {
      private final LinkedList<Runnable> tasks = new LinkedList<Runnable>();

      public void execute(Runnable command)
      {
         synchronized (tasks)
         {
            tasks.add(command);
            if (tasks.size() == 1 && runningChildren.add(this))
            {
               parent.execute(this);
            }
         }
      }

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
                  runningChildren.remove(this);
                  return;
               }
            }
            task.run();
         }
      }
   }
}
