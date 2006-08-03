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
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.ChannelSupport;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox"jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Queue extends ChannelSupport implements CoreDestination
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   //TODO - Do we really need to cache these values here?
   //Can't we just return the values from the corresponding ChannelState?
   private int fullSize;
   
   private int pageSize;
   
   private int downCacheSize;

   // Constructors --------------------------------------------------

   public Queue(long id, MessageStore ms, PersistenceManager pm, MemoryManager mm,
                boolean recoverable, int fullSize, int pageSize, int downCacheSize,
                QueuedExecutor executor)
   {      
      super(id, ms, pm, mm, true, recoverable, fullSize, pageSize, downCacheSize, executor);
      
      //TODO make the policy configurable
      //By default we use a router with a round robin policy for even distribution in the
      //case of multiple consumers
      router = new RoundRobinPointToPointRouter();
      
      this.fullSize = fullSize;
      this.pageSize = pageSize;
      this.downCacheSize = downCacheSize;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "CoreQueue[" + getChannelID() + "]";
   }
   
   public boolean isQueue()
   {
      return true;
   }
   
   public int getMessageCount()
   {
      return messageCount();
   }
   
   // CoreDestination implementation -------------------------------
   
   public long getId()
   {
      return channelID;
   }
   
   public int getFullSize()
   {
      return fullSize;
   }
   
   public int getPageSize()
   {
      return pageSize;
   }
   
   public int getDownCacheSize()
   {
      return downCacheSize;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
