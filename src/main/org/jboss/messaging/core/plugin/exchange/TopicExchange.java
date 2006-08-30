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

import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;

/**
 * A non-clustered TopicExchange
 * 
 * Roughly based on the AMQP topic exchange
 *
 * Currently we don't support hierarchies of topics, but it should be fairly
 * straightforward to extend this class to support them.
 * 
 * For the topic exchange the condition should be just be the topic name
 * 
 * When we support topic hierarchies this will change to a proper wildcard.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class TopicExchange extends ExchangeSupport
{
   private static final Logger log = Logger.getLogger(ClusteredTopicExchange.class);
   
   protected TransactionRepository tr;   
      
   public TopicExchange() throws Exception
   { 
   }      
   
   /*
    * This constructor should only be used for testing
    */
   public TopicExchange(DataSource ds, TransactionManager tm)
   {
      super(ds, tm);
   }
   
   /*
    * Clustered Topic exchange
    */
   public void injectAttributes(String exchangeName, String nodeID,
                                MessageStore ms, IdManager im, QueuedExecutorPool pool,
                                TransactionRepository tr) throws Exception
   {
      super.injectAttributes(exchangeName, nodeID, ms, im, pool);
      
      this.tr = tr;
   }
   
   public boolean route(MessageReference ref, String routingKey, Transaction tx) throws Exception
   {
      if (ref == null)
      {
         throw new IllegalArgumentException("Message reference is null");
      }
      
      if (routingKey == null)
      {
         throw new IllegalArgumentException("Routing key is null");
      }
      
      lock.readLock().acquire();
                
      try
      {                 
         log.info("Routing message with key: " + routingKey);
         
         //We route on the condition
         List bindings = (List)conditionMap.get(routingKey);
         
         if (bindings != null)
         {            
            Iterator iter = bindings.iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
               
               if (binding.isActive() && binding.getNodeId().equals(this.nodeId))
               {
                  //It's a local binding so we pass the message on to the subscription
                  MessageQueue subscription = binding.getQueue();
               
                  subscription.handle(null, ref, tx);                  
               }               
            }                        
         }
      }
      finally
      {                  
         lock.readLock().release();
      }
         
      // We don't care if the individual subscriptions accepted the reference
      return true; 
   }      
}
