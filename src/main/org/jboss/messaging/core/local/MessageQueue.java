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
import org.jboss.messaging.core.PagingChannel;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A MessageQueue
 * 
 * Represents a generic message queue.
 * 
 * Can be used to implement a point to point queue, or a subscription fed from a topic
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class MessageQueue extends PagingChannel
{
   // Constants -----------------------------------------------------
   
   private static final Logger log;
   
   private static final boolean trace;
   
   static
   {
      log = Logger.getLogger(MessageQueue.class);
      trace = log.isTraceEnabled();
   }

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected Filter filter;
   
   
   // Constructors --------------------------------------------------

   public MessageQueue(long id, MessageStore ms, PersistenceManager pm,             
                       boolean acceptReliableMessages, boolean recoverable,
                       int fullSize, int pageSize, int downCacheSize, QueuedExecutor executor,
                       Filter filter)
   {
      super(id, ms, pm, acceptReliableMessages, recoverable, fullSize, pageSize, downCacheSize, executor);
      
      router = new RoundRobinPointToPointRouter();
      
      this.filter = filter;
   }
    
   // Channel implementation ----------------------------------------
   
   public Delivery handle(DeliveryObserver sender, Routable r, Transaction tx)
   { 
      //If the queue has a Filter we do not accept any Message references that do not
      //match the Filter
      if (filter != null && !filter.accept(r))
      {
         if (trace) { log.trace(this + " not accepting " + r); }
         return null;
      }
      
      return super.handle(sender, r, tx);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "Queue[" + getChannelID() + "]";
   }
   
   public Filter getFilter()
   {
      return filter;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
