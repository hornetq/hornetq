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

import java.util.List;

import javax.jms.InvalidSelectorException;

import org.jboss.jms.selector.Selector;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.ChannelSupport;
import org.jboss.messaging.core.CoreDestination;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox"jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Queue extends ChannelSupport implements CoreDestination, ManageableQueue
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   private int m_fullSize;
   private int m_pageSize;
   private int m_downCacheSize;
   
   // Constructors --------------------------------------------------

   public Queue(long id, MessageStore ms, PersistenceManager pm,
                boolean recoverable, int fullSize, int pageSize, int downCacheSize)
   {
      super(id, ms, pm, true, recoverable, fullSize, pageSize, downCacheSize);
      router = new PointToPointRouter();
      m_fullSize = fullSize;
      m_pageSize = pageSize;
      m_downCacheSize = downCacheSize;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "CoreQueue[" + getChannelID() + "]";
   }
   
   public void load() throws Exception
   {
      state.load();
   }
   
   public boolean isQueue()
   {
      return true;
   }
   
   // ManageableQueue implementation --------------------------------
   
   /**
    * @see ManageableQueue#getMessageCount()
    */
   public int getMessageCount()
   {
	   return state.messageCount();
   }
   
   public List getMessages(String selector) throws InvalidSelectorException
   {
      if (null == selector)
         return browse();
      else
         return browse(new Selector(selector));
   }

   // CoreDestination implementation -------------------------------
   
   public long getId()
   {
      return channelID;
   }

   /**
    * @see CoreDestination#getFullSize()
    */
   public int getFullSize()
   {
      return m_fullSize;
   }
   
   /**
    * @see CoreDestination#getPageSize()
    */
   public int getPageSize()
   {
      return m_pageSize;
   }
   
   /**
    * @see CoreDestination#getDownCacheSize()
    */
   public int getDownCacheSize()
   {
      return m_downCacheSize;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
