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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * A JBMThreadFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class HornetQThreadFactory implements ThreadFactory
{
   private final ThreadGroup group;

   private final AtomicInteger threadCount = new AtomicInteger(0);

   private final int threadPriority;
   
   private final boolean daemon;
   
   public HornetQThreadFactory(final String groupName, final boolean daemon)
   {
      this(groupName, Thread.NORM_PRIORITY, daemon);
   }

   public HornetQThreadFactory(String groupName, int threadPriority, final boolean daemon)
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
