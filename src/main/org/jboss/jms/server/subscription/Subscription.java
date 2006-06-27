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
import org.jboss.messaging.core.local.CoreSubscription;
import org.jboss.messaging.core.local.Topic;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

/**
 * 
 * A Subscription.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Subscription extends CoreSubscription
{
   private boolean isNoLocal;
   
   public Subscription(long id, Topic topic, 
         MessageStore ms, PersistenceManager pm, MemoryManager mm,
         int fullSize, int pageSize, int downCacheSize, Selector selector, boolean isNoLocal)
   {
      this(id, topic, ms, pm, mm, false, fullSize, pageSize, downCacheSize, selector, isNoLocal);
   }
   
   protected Subscription(long id, Topic topic, 
         MessageStore ms, PersistenceManager pm, MemoryManager mm, boolean recoverable,
         int fullSize, int pageSize, int downCacheSize, Selector selector, boolean isNoLocal)
   {
      super(id, topic, ms, pm, mm, recoverable, fullSize, pageSize, downCacheSize, selector);
      this.isNoLocal = isNoLocal;
   }
         
   public String getSelector()
   {
      return filter == null ? null : ((Selector)filter).getExpression();      
   }
   
   public boolean isNoLocal()
   {
      return isNoLocal;
   }
}
