/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package org.jboss.messaging.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * A JBMThreadFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JBMThreadFactory implements ThreadFactory
{
   private final ThreadGroup group;

   private final AtomicInteger threadCount = new AtomicInteger(0);

   private final int threadPriority;
   
   private final boolean daemon;
   
   public JBMThreadFactory(final String groupName, final boolean daemon)
   {
      this(groupName, Thread.NORM_PRIORITY, daemon);
   }

   public JBMThreadFactory(String groupName, int threadPriority, final boolean daemon)
   {
      this.group = new ThreadGroup(groupName + "-" + System.identityHashCode(this));
            
      this.threadPriority = threadPriority;
      
      this.daemon = daemon;
   }

   public Thread newThread(final Runnable command)
   {
      Thread t = new Thread(group, command, "Thread-" + threadCount.getAndIncrement() +
                                            " (group:" +
                                            group.getName() +
                                            ")");

      t.setDaemon(daemon);
      t.setPriority(threadPriority);
      return t;
   }
}
