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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.memory.MemoryManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;

/**
 * Represents a subscription to a destination (topic or queue).
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class CoreSubscription extends Pipe
{
   // Constants -----------------------------------------------------
   
   private static final Logger log;
   
   private static final boolean trace;
   
   private boolean connected;
   
   static
   {
      log = Logger.getLogger(CoreSubscription.class);
      trace = log.isTraceEnabled();
   }
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected Topic topic;

   protected Filter filter;
   
   // Constructors --------------------------------------------------
 
   public CoreSubscription(long id, Topic topic, 
                           MessageStore ms, PersistenceManager pm, MemoryManager mm,
                           boolean recoverable,
                           int fullSize, int pageSize, int downCacheSize, Filter filter)
                              
   {
      // A CoreSubscription must accept reliable messages
      super(id, ms, pm, mm, true, recoverable, fullSize, pageSize, downCacheSize);
      this.topic = topic;
      this.filter = filter;
   }
  
   // Channel implementation ----------------------------------------
   
   public Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   { 
      //If the subscription has a Filter we do not accept any Message references that do not
      //match the Filter
      if (filter != null && !filter.accept(r))
      {
         if (trace) { log.trace(this + " not accepting " + r); }
         return null;
      }
      
      return super.handle(sender, r, tx);
   }

   // Public --------------------------------------------------------
   
   public void connect()
   {
      if (!connected)
      {
         topic.add(this);
         connected = true;
      }
   }
   
   public void disconnect()
   {
      if (connected)
      {
         topic.remove(this);
         connected = false;
      }
   }
      
   public Topic getTopic()
   {
      return topic;
   }
   
   public void load() throws Exception
   {
      state.load();
   }
     
   public String toString()
   {
      return "CoreSubscription[" + getChannelID() + ", " + topic + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
