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
package org.jboss.messaging.core.plugin.exchange.cluster;

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
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.exchange.Binding;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jgroups.Channel;

/**
 * A Clustered TopicExchange
 * 
 * Similar in some ways to the AMQP topic exchange
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
public class ClusteredTopicExchange extends ClusteredExchangeSupport
{
   private static final Logger log = Logger.getLogger(ClusteredTopicExchange.class);     
   
   protected TransactionRepository tr;  
    
   /*
    * This constructor should only be used for testing
    */
   public ClusteredTopicExchange(DataSource ds, TransactionManager tm)
   {
      super(ds, tm);
   }
   
   public void injectAttributes(Channel controlChannel, Channel dataChannel,
                                String groupName, String exchangeName, String nodeID,
                                MessageStore ms, IdManager im, QueuedExecutorPool pool,
                                TransactionRepository tr, PersistenceManager pm) throws Exception
   {
      super.injectAttributes(controlChannel, dataChannel,
                             groupName, exchangeName, nodeID, ms, im, pool, pm);
      
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
         // We route on the condition
         List bindings = (List)conditionMap.get(routingKey);
      
         if (bindings != null)
         {                
            // When routing a persistent message without a transaction then we may need to start an 
            // internal transaction in order to route it.
            // We do this if the message is reliable AND:
            // (
            // a) The message needs to be routed to more than one durable subscription. This is so we
            // can guarantee the message is persisted on all the durable subscriptions or none if failure
            // occurs - i.e. the persistence is transactional
            // OR
            // b) There is at least one durable subscription on a different node.
            // In this case we need to start a transaction since we want to add a callback on the transaction
            // to cast the message to other nodes
            // )
                        
            //TODO we can optimise this out by storing this as a flag somewhere
            boolean startInternalTx = false;
      
            if (tx == null)
            {
               if (ref.isReliable())
               {
                  Iterator iter = bindings.iterator();
                  
                  int count = 0;
                  
                  while (iter.hasNext())
                  {
                     Binding binding = (Binding)iter.next();
                     
                     if (binding.isDurable())
                     {
                        count++;
                        
                        if (count == 2 || !binding.getNodeId().equals(this.nodeId))
                        {
                           startInternalTx = true;
                           
                           break;
                        }                          
                     }
                  }
               }
               
               if (startInternalTx)
               {
                  tx = tr.createTransaction();
               }
            }
                       
            Iterator iter = bindings.iterator();
            
            boolean sendRemotely = false;

            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
               
               if (binding.isActive())
               {            
                  if (binding.getNodeId().equals(this.nodeId))
                  {
                     //It's a local binding so we pass the message on to the subscription
                     MessageQueue subscription = binding.getQueue();
                  
                     subscription.handle(null, ref, tx);
                  }
                  else
                  {
                     //It's a binding on a different exchange instance on the cluster
                     sendRemotely = true;                     
                      
                     if (ref.isReliable() && binding.isDurable())
                     {
                        //Insert the reference into the database
                        pm.addReference(binding.getChannelId(), ref, tx);
                     }
                  }                     
               }
            } 
            
            //Now we've sent the message to all the local subscriptions, we might also need
            //to multicast the message to the other exchange instances on the cluster if there are
            //subscriptions on those nodes that need to receive the message
            if (sendRemotely)
            {
               if (tx == null)
               {
                  //We just throw the message on the network - no need to wait for any reply            
                  asyncSendRequest(new MessageRequest(routingKey, ref.getMessage()));               
               }
               else
               {
                  CastMessagesCallback callback = (CastMessagesCallback)tx.getCallback(this);
                  
                  if (callback == null)
                  {
                     callback = new CastMessagesCallback(nodeId, tx.getId(), ClusteredTopicExchange.this);
                     
                     //This callback must be executed first
                     tx.addFirstCallback(callback, this);
                  }
                      
                  callback.addMessage(routingKey, ref.getMessage());                  
               }
            }
            
            if (startInternalTx)
            {
               tx.commit();
            }
         }
      }
      finally
      {                  
         lock.readLock().release();
      }
         
      // We don't care if the individual subscriptions accepted the reference
      // We always return true for a topic
      return true; 
   }
   
   /*
    * We have received a reference cast from another node - and we need to route it to our local
    * subscriptions    
    */
   protected void routeFromCluster(MessageReference ref, String routingKey) throws Exception
   {
      lock.readLock().acquire();
      
      try
      {      
         // We route on the condition
         List bindings = (List)conditionMap.get(routingKey);
      
         if (bindings != null)
         {                                
            Iterator iter = bindings.iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
               
               if (binding.isActive())
               {            
                  if (binding.getNodeId().equals(this.nodeId))
                  {  
                     //It's a local binding so we pass the message on to the subscription
                     MessageQueue subscription = binding.getQueue();
                  
                     //TODO instead of adding a new method on the channel
                     //we should set a header and use the same method
                     subscription.handleDontPersist(null, ref, null);
                  }                               
               }
            }                          
         }
      }
      finally
      {                  
         lock.readLock().release();
      }
   }            
}
