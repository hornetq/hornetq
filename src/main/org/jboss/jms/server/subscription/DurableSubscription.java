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
package org.jboss.jms.server.subscription;

import org.jboss.jms.selector.Selector;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A DurableSubscription.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class DurableSubscription extends Subscription
{
   protected String name;
   
   protected String clientID;
   
   public DurableSubscription(long id, Topic topic, 
         MessageStore ms, PersistenceManager pm, MemoryManager mm, 
         int fullSize, int pageSize, int downCacheSize, QueuedExecutor executor,
         Selector selector, boolean isNoLocal,
         String name, String clientID)
   {
      super(id, topic, ms, pm, mm, true, fullSize, pageSize, downCacheSize, executor, selector, isNoLocal);
      this.name = name;
      this.clientID = clientID;
   }
      
   public String getName()
   {
      return name;
   }
   
   public String getClientID()
   {
      return clientID;
   }
   
   public void unsubscribe() throws Exception
   {
      disconnect();
      if (pm != null)
      {
         pm.removeAllChannelData(this.channelID);            
      }
   }   
}
