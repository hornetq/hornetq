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
package org.jboss.messaging.core.plugin.exchange;

import java.util.Map;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.tx.Transaction;

/**
 * A DirectExchange
 * 
 * A direct exchange routes messages directly to a queue if the routing key matches the
 * queue name
 * 
 * This type of exchange is used for routing to point to point queues.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DirectExchange extends ExchangeSupport
{
   private static final Logger log = Logger.getLogger(DirectExchange.class);
      
   //To avoid having to lookup the name map based on the node id every time we route a message
   //we store a reference here
   private Map shortcutNameMap;
   
   public DirectExchange() throws Exception
   {      
   }
   
   /*
    * This constructor should only be used for testing
    */
   public DirectExchange(DataSource ds, TransactionManager tm)
   {
      super(ds, tm);
   }
   
     
   /*
    * Direct exchanges aren't clustered
    *
    */
   public void injectAttributes(String exchangeName, String nodeID,
                                MessageStore ms, IdManager im, QueuedExecutorPool pool) throws Exception
   {
      super.injectAttributes(exchangeName, nodeID, ms, im, pool);
   }
   
   public boolean route(MessageReference ref, String routingKey, Transaction tx) throws Exception
   {    
      if (ref == null)
      {
         throw new IllegalArgumentException("Reference is null");
      }
      
      if (routingKey == null)
      {
         throw new IllegalArgumentException("Routing key is null");
      }
      
      lock.readLock().acquire();
      
      try
      {         
         //We just route based on the name
         if (shortcutNameMap == null)
         {
            shortcutNameMap = (Map)nameMaps.get(nodeId);
         }
         
         Binding binding = null;
                  
         if (shortcutNameMap != null)
         {
            binding = (Binding)shortcutNameMap.get(routingKey);
         }
         
         if (binding == null)
         {
            //No mapping
            return false;
         }
         
         Delivery del = null;
         
         if (binding.isActive())
         {         
            MessageQueue queue = binding.getQueue();            
         
            del = queue.handle(null, ref, tx);
         }
         
         if (del == null || !del.isDone())
         {
            return false;
         }
         else
         {
            return true;
         }
      }
      finally
      {
         lock.readLock().release();
      }
   }
}
