/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.server;

import org.jboss.messaging.util.RotatingPool;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * A QueuedExecutorPool

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class QueuedExecutorPool extends RotatingPool
{
   public QueuedExecutorPool(int maxSize)
   {
      super(maxSize);
   }
   
   protected Object createEntry()
   {
      return new QueuedExecutor();
   }
   
   public void shutdown()
   {
      for (int i = 0; i < entries.length; i++)
      {
         QueuedExecutor q = (QueuedExecutor)entries[i];
         
         if (q != null)
         {
            q.shutdownAfterProcessingCurrentlyQueuedTasks();
         }
      }
      
      entries = null;
   }
}
