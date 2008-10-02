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
