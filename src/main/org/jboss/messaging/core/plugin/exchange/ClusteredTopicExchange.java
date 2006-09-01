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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.MessageQueue;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.core.tx.TxCallback;
import org.jgroups.Channel;

/**
 * A Clustered TopicExchange
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
public class ClusteredTopicExchange extends ClusteredExchangeSupport
{
   private static final Logger log = Logger.getLogger(ClusteredTopicExchange.class);     
   
   protected TransactionRepository tr;  
   
   protected PersistenceManager pm;
   
   protected boolean strictPersistentMessageReliability;
      
   public ClusteredTopicExchange() throws Exception
   { 
   }      
   
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
                             groupName, exchangeName, nodeID, ms, im, pool);
      
      this.tr = tr;
      
      this.pm = pm;
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
            //When routing a reliable message to multiple durable subscriptions we
            //must ensure that the message is persisted in all durable subscriptions
            //Therefore if the message is reliable and there is more than one durable subscription
            //Then we create an internal transaction and route the message in the context of that
            
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
                        
                        if (count == 2)
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
                     
                     //For remote durable subscriptions we persist the message *before* sending
                     //it to the remote node. We do this for two reasons
                     //1) For reliable messages we must ensure that the message is persisted in all durable subscriptions
                     // and not a subset in case of failure - if we were persisting on receipt at the remote
                     // durable sub then we would have to use expensive 2PC to ensure this
                     //2) We can insert in all the durable subs in a single JDBC tx rather than many
                     
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
                  asyncCastMessage(routingKey, ref.getMessage());               
                  
                  //Big TODO - is this ok for reliable messages ?????????
                  //What if this node fails and the other nodes don't receive the cast?
                  //We have already persisted the message so we won't lose it
                  //but the message won't be available to be consumed on the other node
                  //until that node restarts - which might be 10 years from now.
                  //What we should really do is, for persistent messages, save and cast them as 2 participants
                  //in a fully recoverable 2pc transaction - but this is going to be a performance hit                                    
                  //
                  //Need to think about this some more
                  
               }
               else
               {
                  CastingCallback callback = (CastingCallback)tx.getKeyedCallback(this);
                  
                  if (callback == null)
                  {
                     callback = new CastingCallback();
                     
                     tx.addKeyedCallback(callback, this);
                  }
                  
                  log.info("Got callback:" + callback);
                  
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
//                     //When receiving a reliable message from the network and routing to
//                     //a durable subscription, then the message has always been persisted
//                     //before the send
//                     if (ref.isReliable() && binding.isDurable())
//                     {
//                        //Do what?
//                     }
                     
                     //It's a local binding so we pass the message on to the subscription
                     MessageQueue subscription = binding.getQueue();
                  
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
   
   /*
    * This class casts messages across the cluster on commit of a transaction
    */
   private class CastingCallback implements TxCallback
   {           
      private List messages = new ArrayList();
      
      private void addMessage(String routingKey, Message message)
      {
         log.info("Adding message");
         messages.add(new MessageHolder(routingKey, message));
      }
      
      private CastingCallback()
      {
         messages = new ArrayList();
      }

      public void afterCommit(boolean onePhase) throws Exception
      {
         //Cast the messages
         //TODO - would it be any more performant if we cast them in a single jgroups message?
         Iterator iter = messages.iterator();
         
         while (iter.hasNext())
         {
            MessageHolder holder = (MessageHolder)iter.next();
            
            asyncCastMessage(holder.getRoutingKey(), holder.getMessage());
         }
      }

      public void afterPrepare() throws Exception
      { 
      }

      public void afterRollback(boolean onePhase) throws Exception
      {
      }

      public void beforeCommit(boolean onePhase) throws Exception
      {
      }

      public void beforePrepare() throws Exception
      {
      }

      public void beforeRollback(boolean onePhase) throws Exception
      {
      }
      
      private class MessageHolder
      {
         String routingKey;
         
         Message message;
         
         private MessageHolder(String routingKey, Message message)
         {
            this.routingKey = routingKey;
            this.message = message;
         }
         
         private String getRoutingKey()
         {
            return routingKey;
         }
         
         private Message getMessage()
         {
            return message;
         }
      }
     
   }    
}
