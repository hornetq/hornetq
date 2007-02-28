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
package org.jboss.messaging.util;

import EDU.oswego.cs.dl.util.concurrent.Channel;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * A ClearableQueuedExecutor
 * 
 * This class extends the QueuedExector with a method to clear all but the currently
 * executing task without shutting it down.
 * 
 * We need this functionality when failing over a session.
 * 
 * In that case we need to clear all tasks apart from the currently executing one.
 * 
 * We can't just shutdownAfterProcessingCurrentTask then use another instance
 * after failover since when failover resumes the current task and the next delivery
 * will be executed on different threads and smack into each other.
 * 
 * http://jira.jboss.org/jira/browse/JBMESSAGING-904
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClearableQueuedExecutor extends QueuedExecutor
{
   public ClearableQueuedExecutor()
   {
   }

   public ClearableQueuedExecutor(Channel channel)
   {
      super(channel);
   }

   public void clearAllExceptCurrentTask()
   {
      try
      { 
        while (queue_.poll(0) != null);
      }
      catch (InterruptedException ex)
      {
        Thread.currentThread().interrupt();
      }
   }
   
}

