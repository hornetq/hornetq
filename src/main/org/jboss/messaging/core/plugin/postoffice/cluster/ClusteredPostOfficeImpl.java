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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.contract.Binding;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.postoffice.ConditionBindings;
import org.jboss.messaging.core.plugin.postoffice.PostOfficeImpl;
import org.jboss.messaging.core.plugin.postoffice.BindingImpl;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.Util;
import org.w3c.dom.Element;

/**
 * 
 * A ClusteredPostOfficeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteredPostOfficeImpl extends PostOfficeImpl implements ClusteredPostOffice, PostOfficeInternal
{
   private static final Logger log = Logger.getLogger(ClusteredPostOfficeImpl.class);
                       
   private Channel syncChannel;
   
   private Channel asyncChannel;
   
   private String groupName;
   
   private MessageDispatcher controlMessageDispatcher;
   
   private MessageListener controlMessageListener;
   
   private Receiver dataReceiver;
   
   private MembershipListener controlMembershipListener;
   
   private RequestHandler requestHandler;
   
   private Object setStateLock = new Object();
   
   private boolean stateSet;
   
   private View currentView;
   
   //Map < Address, node id>
   private Map nodeIdAddressMap;
   
   private Map holdingArea;
   
   private PersistenceManager pm;
   
   private Element syncChannelConfigE;
   
   private Element asyncChannelConfigE;
   
   private String syncChannelConfigS;
   
   private String asyncChannelConfigS;
   
   private long stateTimeout;
   
   private long castTimeout;
   
   private RoutingPolicy routingPolicy;
      
   public ClusteredPostOfficeImpl()
   {        
      init();
   }
   
   private void init()
   {
      this.nodeIdAddressMap = new HashMap();
      
      this.holdingArea = new HashMap();
   }
   
   /*
    * Constructor using Element for configuration
    */
   public ClusteredPostOfficeImpl(DataSource ds, TransactionManager tm, Properties sqlProperties,
                              boolean createTablesOnStartup,
                              String nodeId, String officeName, MessageStore ms,
                              String groupName,
                              Element syncChannelConfig,
                              Element asyncChannelConfig,
                              TransactionRepository tr,
                              PersistenceManager pm,
                              long stateTimeout, long castTimeout,
                              RoutingPolicy routingPolicy) throws Exception
   {            
      this(ds, tm, sqlProperties, createTablesOnStartup, nodeId, officeName, ms,
           groupName, tr, pm, stateTimeout, castTimeout, routingPolicy);
      
      this.syncChannelConfigE = syncChannelConfig;      
      this.asyncChannelConfigE = asyncChannelConfig;     
   }
     
   /*
    * Constructor using String for configuration
    */
   public ClusteredPostOfficeImpl(DataSource ds, TransactionManager tm, Properties sqlProperties,
                              boolean createTablesOnStartup,
                              String nodeId, String officeName, MessageStore ms,
                              String groupName,
                              String syncChannelConfig,
                              String asyncChannelConfig,
                              TransactionRepository tr,
                              PersistenceManager pm,
                              long stateTimeout, long castTimeout,
                              RoutingPolicy routingPolicy) throws Exception
   {            
      this(ds, tm, sqlProperties, createTablesOnStartup, nodeId, officeName, ms,
           groupName, tr, pm, stateTimeout, castTimeout, routingPolicy);

      this.syncChannelConfigS = syncChannelConfig;      
      this.asyncChannelConfigS = asyncChannelConfig;     
   }
   
   private ClusteredPostOfficeImpl(DataSource ds, TransactionManager tm, Properties sqlProperties,
                               boolean createTablesOnStartup,
                               String nodeId, String officeName, MessageStore ms,
                               String groupName,
                               TransactionRepository tr,
                               PersistenceManager pm,
                               long stateTimeout, long castTimeout,
                               RoutingPolicy routingPolicy)
   {
      super (ds, tm, sqlProperties, createTablesOnStartup, nodeId, officeName, ms, tr);
       
      this.pm = pm;
      
      this.groupName = groupName;
      
      this.stateTimeout = stateTimeout;
      
      this.castTimeout = castTimeout;
      
      this.routingPolicy = routingPolicy;
      
      init();
   }

   
   public void start() throws Exception
   {
      if (syncChannelConfigE != null)
      {        
         this.syncChannel = new JChannel(syncChannelConfigE);
         this.asyncChannel = new JChannel(asyncChannelConfigE); 
      }
      else
      {
         this.syncChannel = new JChannel(syncChannelConfigS);
         this.asyncChannel = new JChannel(asyncChannelConfigS); 
      }
      
      //We don't want to receive local messages on any of the channels
      syncChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
      
      asyncChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
      
      this.controlMessageListener = new ControlMessageListener();
      
      this.requestHandler = new PostOfficeRequestHandler();
      
      this.controlMembershipListener = new ControlMembershipListener();
      
      this.controlMessageDispatcher = new MessageDispatcher(syncChannel, controlMessageListener,
                                                            controlMembershipListener, requestHandler, true);      
      this.dataReceiver = new DataReceiver();
      
      asyncChannel.setReceiver(dataReceiver);    
             
      syncChannel.connect(groupName);
      
      asyncChannel.connect(groupName);
      
      super.start();
      
      Address currentAddress = syncChannel.getLocalAddress();
             
      handleAddressNodeMapping(currentAddress, nodeId);
      
      syncSendRequest(new SendNodeIdRequest(currentAddress, nodeId));
   }

   public void stop() throws Exception
   {
      super.stop();
      
      syncChannel.close();
      
      asyncChannel.close();
   }  
   
   // PostOffice implementation ---------------------------------------        
   
   public Binding bindClusteredQueue(String queueName, String condition, Queue queue) throws Exception
   {           
      Binding binding = super.bindQueue(queueName, condition, queue);
      
      boolean durable = queue.isRecoverable();
      
      String filter = queue.getFilter() == null ? null : queue.getFilter().getFilterString();
      
      BindRequest request =
         new BindRequest(nodeId, queueName, condition, filter,
                         binding.getChannelId(), durable);
      
      syncSendRequest(request);
      
      return binding;
   }
   
   public Binding unbindClusteredQueue(String queueName) throws Throwable
   {
      Binding binding = super.unbindQueue(queueName);
      
      UnbindRequest request = new UnbindRequest(nodeId, queueName);
      
      syncSendRequest(request);
      
      return binding;
   }
   
   /*
    * This is called by the server peer if it determines that the server crashed last time it was run
    */
   public void recover() throws Exception
   {
      //We send a "check" message to all nodes of the cluster
      asyncSendRequest(new CheckMessage(nodeId));
   }
   
   public boolean route(MessageReference ref, String condition, Transaction tx) throws Exception
   {
      if (ref == null)
      {
         throw new IllegalArgumentException("Message reference is null");
      }
      
      if (condition == null)
      {
         throw new IllegalArgumentException("Condition is null");
      }
      
      boolean routed = false;
      
      lock.readLock().acquire();
      
      try
      {      
         ConditionBindings cb = (ConditionBindings)conditionMap.get(condition);
         
         boolean startInternalTx = false;
         
         if (cb != null)
         {
            if (tx == null && ref.isReliable())
            {
               if (!(cb.getDurableCount() == 1 && cb.getLocalDurableCount() == 1))
               {
                  // When routing a persistent message without a transaction then we may need to start an 
                  // internal transaction in order to route it.
                  // This is so we can guarantee the message is delivered to all or none of the subscriptions.
                  // We need to do this if there is any other than a single local durable subscription
                  startInternalTx = true;
               }
            }
            
            if (startInternalTx)
            {
               tx = tr.createTransaction();
            }
            
            //There may be no transaction in the following cases
            //1) No transaction specified in params and reference is unreliable
            //2) No transaction specified in params and reference is reliable and there is only one
            //   or less local durable subscription
         
            Collection bindingLists = cb.getBindingsByName();

            Iterator iter = bindingLists.iterator();
            
            int numberRemote = 0;
            
            Map queueNameNodeIdMap = null;
            
            while (iter.hasNext())
            {
               //Each list is the list of bindings which have the same queue name
               List bindings = (List)iter.next();
               
               //We may have more than one binding with the same queue name on different nodes in the
               //following situations:
               //1) When a point to point queue has been deployed across the cluster
               //2) When a durable subscription has been created on multiple nodes to implement
               // shared durable subscriptions.
               //In both of these cases we only want one of the queues to receive the message, we choose which 
               //one by asking the routing policy
               Binding binding;
               
               if (bindings.size() == 1)
               {
                  binding = (Binding)bindings.get(0);
               }
               else if (bindings.size() > 1)
               {
                  binding = routingPolicy.choose(bindings); 
                  
                  if (queueNameNodeIdMap == null)
                  {
                     queueNameNodeIdMap = new HashMap();
                  }
                  
                  if (!binding.getNodeId().equals(this.nodeId))
                  {
                     //Chose a remote node
                     //We add the node id to the map against the name
                     //This is used on receipt to work out if a particular queue should
                     //accept the message, when multicasted
                     queueNameNodeIdMap.put(binding.getQueueName(), binding.getNodeId());
                  }
               }
               else
               {
                  throw new IllegalStateException("No bindings in list!");
               }
               
               if (binding.getNodeId().equals(this.nodeId))
               {
                  //It's a local binding so we pass the message on to the queue
                  Queue queue = binding.getQueue();
               
                  Delivery del = queue.handle(null, ref, tx);    
                  
                  if (del != null && del.isSelectorAccepted())
                  {
                     routed = true;
                  }  
               }
               else
               {
                  //It's a binding on a different office instance on the cluster
                  numberRemote++;                     
                   
                  if (ref.isReliable() && binding.isDurable())
                  {
                     //Insert the reference into the database
                     
                     //TODO perhaps we should do this via a stub queue class
                     pm.addReference(binding.getChannelId(), ref, tx);
                  }
                  
                  routed = true;
               }                                                
            }
                                    
            //Now we've sent the message to any local queues, we might also need
            //to send the message to the other office instances on the cluster if there are
            //queues on those nodes that need to receive the message
            
            if (numberRemote > 0)
            {
               //TODO - If numberRemote == 1, we could do unicast rather than multicast
               //This would avoid putting strain on nodes that don't need to receive the reference
               //This would be the case for load balancing queues where the routing policy
               //sometimes allows a remote queue to get the reference
               
               if (tx == null)
               {
                  //We just throw the message on the network - no need to wait for any reply                  
                  asyncSendRequest(new MessageRequest(condition, ref.getMessage(), queueNameNodeIdMap));               
               }
               else
               {
                  CastMessagesCallback callback = (CastMessagesCallback)tx.getCallback(this);
                  
                  if (callback == null)
                  {
                     callback = new CastMessagesCallback(nodeId, tx.getId(), ClusteredPostOfficeImpl.this);
                     
                     //This callback must be executed first
                     tx.addFirstCallback(callback, this);
                  }
                      
                  callback.addMessage(condition, ref.getMessage(), queueNameNodeIdMap);                  
               }
            }
            
            if (startInternalTx)
            {               
               // TODO - do we need to rollback if an exception is thrown??
               tx.commit();
            }
         }
      }
      finally
      {                  
         lock.readLock().release();
      }
         
      return routed; 
   }
   
   // PostOfficeInternal implementation ------------------------------------------------------------------
   
   /*
    * Called when another node adds a binding
    */
   public void addBindingFromCluster(String nodeId, String queueName, String condition,
                                      String filterString, long channelID, boolean durable)
      throws Exception
   {
      lock.writeLock().acquire();
      
      try
      {                     
         //Sanity
         if (!nodeIdAddressMap.containsKey(nodeId))
         {
            throw new IllegalStateException("Cannot find address for node: " + nodeId);
         }
         
         // We currently only allow one binding per name per node
         Map nameMap = (Map)nameMaps.get(nodeId);
         
         Binding binding = null;
         
         if (nameMap != null)
         {
            binding = (Binding)nameMap.get(queueName);
         }
         
         if (binding != null)
         {
            throw new IllegalArgumentException(this.nodeId + "Binding already exists for node Id " + nodeId + " queue name " + queueName);
         }
         
         binding = new BindingImpl(nodeId, queueName, condition, filterString,
                                   channelID, durable); 
         
         binding.activate();
         
         addBinding(binding);         
      }
      finally
      {
         lock.writeLock().release();
      }
   }
   
   /*
    * Called when another node removes a binding
    */
   public void removeBindingFromCluster(String nodeId, String queueName) throws Exception
   {
      lock.writeLock().acquire();
      
      try
      {         
         // Sanity
         if (!nodeIdAddressMap.containsKey(nodeId))
         {
            throw new IllegalStateException("Cannot find address for node: " + nodeId);
         }
         
         removeBinding(nodeId, queueName);         
      }
      finally
      {
         lock.writeLock().release();
      }
   }
   
   public void handleAddressNodeMapping(Address address, String nodeId) throws Exception
   {
      lock.writeLock().acquire();
      
      try
      { 
         nodeIdAddressMap.put(nodeId, address.toString());
      }
      finally
      {
         lock.writeLock().release();
      }
   }
   
   public void addToQueue(String queueName, List messages) throws Exception
   {
      lock.readLock().acquire();      
            
      try
      {
         Binding binding = this.getBindingForQueueName(queueName);
         
         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue name " + queueName);
         }
         
         Queue queue = binding.getQueue();
         
         Iterator iter = messages.iterator();
         
         while (iter.hasNext())
         {
            MessageReference ref = null;
            
            try
            {
               org.jboss.messaging.core.Message msg = (org.jboss.messaging.core.Message)iter.next();
               
               ref = ms.reference(msg);
               
               queue.handleDontPersist(null, ref, null);
            }
            finally
            {
               if (ref != null)
               {
                  ref.releaseMemoryReference();
               }
            }
         }    
      }
      finally
      {
         
         lock.readLock().release();
      }
   }
   
   public void routeFromCluster(org.jboss.messaging.core.Message message, String routingKey,
                                Map queueNameNodeIdMap) throws Exception
   {
      lock.readLock().acquire();      
      
      // Need to reference the message
      MessageReference ref = null;
      try
      {
         ref = ms.reference(message);
              
         // We route on the condition
         ConditionBindings cb = (ConditionBindings)conditionMap.get(routingKey);
      
         if (cb != null)
         {                                
            List bindings = cb.getAllBindings();
            
            Iterator iter = bindings.iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
                                             
               if (binding.isActive())
               {            
                  if (binding.getNodeId().equals(this.nodeId))
                  {  
                     boolean handle = true;
                     
                     if (queueNameNodeIdMap != null)
                     {
                        String desiredNodeId = (String)queueNameNodeIdMap.get(binding.getQueueName());
                        
                        //When there are more than one queues with the same name across the cluster we only
                        //want to chose one of them
                        
                        if (desiredNodeId != null)
                        {
                           handle = desiredNodeId.equals(nodeId);
                        }
                     }
                     
                     if (handle)
                     {
                        //It's a local binding so we pass the message on to the subscription
                        Queue subscription = binding.getQueue();
                     
                        //TODO instead of adding a new method on the channel
                        //we should set a header and use the same method
                        subscription.handleDontPersist(null, ref, null);
                     }
                  }                               
               }
            }                          
         }
      }
      finally
      {
         if (ref != null)
         {
            ref.releaseMemoryReference();
         }
         lock.readLock().release();
      }
   }
   
   public void asyncSendRequest(ClusterRequest request) throws Exception
   {            
      //TODO - handle serialization more efficiently
      asyncChannel.send(new Message(null, null, request));
   }
   
   /*
    * We put the transaction in the holding area
    */
   public void holdTransaction(TransactionId id, ClusterTransaction tx) throws Exception
   {
      synchronized (holdingArea)
      {
         holdingArea.put(id, tx);
      } 
   }
   
   public void commitTransaction(TransactionId id) throws Exception
   {
      ClusterTransaction tx = null;
      
      synchronized (holdingArea)
      {
         tx = (ClusterTransaction)holdingArea.remove(id);
      }
      
      if (tx == null)
      {
         throw new IllegalStateException("Cannot find transaction transaction id: " + id);
      }
      
      tx.commit(this);
   }
   
   /*
    * Called by a node if it starts and it detects that it crashed since it's last start-up.
    * This method then checks to see if there any messages from that node in the holding area
    * and if they are also in the database they will be processed
    */
   public void check(String nodeId) throws Exception
   {
      synchronized (holdingArea)
      {
         Iterator iter = holdingArea.entrySet().iterator();
         
         List toRemove = new ArrayList();
         
         while (iter.hasNext())
         {
            Map.Entry entry = (Map.Entry)iter.next();
            
            TransactionId id = (TransactionId)entry.getKey();
            
            if (id.getNodeId().equals(nodeId))
            {
               List holders = (List)entry.getValue();
               
               boolean wasCommitted = checkTransaction(holders);               
               
               if (wasCommitted)
               {
                  //We can process the transaction
                  Iterator iter2 = holders.iterator();
                  
                  while (iter2.hasNext())
                  {
                     MessageHolder holder = (MessageHolder)iter2.next();
                     
                     routeFromCluster(holder.getMessage(), holder.getRoutingKey(), holder.getQueueNameToNodeIdMap());
                  }
                  
                  toRemove.add(id);
               }
            }
         }
         
         //Remove the transactions from the holding area
         
         iter = toRemove.iterator();
         
         while (iter.hasNext())
         {
            TransactionId id = (TransactionId)iter.next();
            
            holdingArea.remove(id);
         }
      }
   }
   
   private boolean checkTransaction(List messageHolders) throws Exception
   {
      Iterator iter = messageHolders.iterator();
      
      //We only need to check that one of the refs made it to the database - the refs would have
      //been inserted into the db transactionally, so either they're all there or none are
      MessageHolder holder = (MessageHolder)iter.next();
      
      List bindings = listBindingsForCondition(holder.getRoutingKey());
      
      if (bindings == null)
      {
         throw new IllegalStateException("Cannot find bindings for key: " + holder.getRoutingKey());
      }
      
      Iterator iter2 = bindings.iterator();
      
      long channelID = -1;
      boolean found = false;
      
      while (iter2.hasNext())
      {
         Binding binding = (Binding)iter2.next();
         
         if (binding.isDurable())
         {
            found = true;
            
            channelID = binding.getChannelId();
         }
      }
      
      if (!found)
      {
         throw new IllegalStateException("Cannot find bindings");
      }
      
      if (pm.referenceExists(channelID, holder.getMessage().getMessageID()))
      {
         return true;
      }
      else
      {
         return false;
      }
   }
    
   protected void loadBindings() throws Exception
   {
      // TODO I need to know whether this call times out - how do I know this??
      boolean isState = syncChannel.getState(null, stateTimeout);
                              
      if (!isState)
      {       
         //Must be first member in group or non clustered- we load the state ourself from the database
         super.loadBindings();      
      }
      else
      {
         //The state will be set in due course via the MessageListener - we must wait until this happens
         
         synchronized (setStateLock)
         {
            //TODO we should implement a timeout on this
            while (!stateSet)
            {
               setStateLock.wait();
            } 
         }
      }
   }
   
   // Public ------------------------------------------------------------------------------------------
   
   
   // Protected ---------------------------------------------------------------------------------------
   
   
   protected void syncSendRequest(ClusterRequest request) throws Exception
   {            
      //TODO - handle serialization more efficiently
      
      Message message = new Message(null, null, request);      
      
      controlMessageDispatcher.castMessage(null, message, GroupRequest.GET_ALL, castTimeout);
   }
   
   /*
    * We have received a reference cast from another node - and we need to route it to our local
    * subscriptions    
    */
//   protected void routeFromCluster(MessageReference ref, String routingKey) throws Exception
//   {
//      lock.readLock().acquire();
//      
//      try
//      {      
//         // We route on the condition
//         List bindings = (List)conditionMap.get(routingKey);
//      
//         if (bindings != null)
//         {                                
//            Iterator iter = bindings.iterator();
//            
//            while (iter.hasNext())
//            {
//               Binding binding = (Binding)iter.next();
//               
//               if (binding.isActive())
//               {            
//                  if (binding.getNodeId().equals(this.nodeId))
//                  {  
//                     //It's a local binding so we pass the message on to the subscription
//                     Queue subscription = binding.getQueue();
//                  
//                     //TODO instead of adding a new method on the channel
//                     //we should set a header and use the same method
//                     subscription.handleDontPersist(null, ref, null);
//                  }                               
//               }
//            }                          
//         }
//      }
//      finally
//      {                  
//         lock.readLock().release();
//      }
//   }            
//   
  
       
   // Private ------------------------------------------------------------------------------------------
             
   private void removeBindingsForAddress(String address) throws Exception
   {
      lock.writeLock().acquire();
      
      try
      { 
         Iterator iter = nodeIdAddressMap.entrySet().iterator();
         
         String nodeId = null;
         while (iter.hasNext())
         {
            Map.Entry entry = (Map.Entry)iter.next();
            
            String str = (String)entry.getValue();
            
            if (str.equals(address))
            {
               nodeId = (String)entry.getKey();
            }
         }
         
         if (nodeId == null)
         {
            throw new IllegalStateException("Cannot find node id for address: " + address);
         }
         
         Map nameMap = (Map)nameMaps.get(nodeId);

         if (nameMap != null)
         {
            List toRemove = new ArrayList();
            
            iter = nameMap.values().iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
               
               if (!binding.isDurable())
               {
                  toRemove.add(binding);
               }
            }
            
            iter = toRemove.iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
               
               removeBinding(nodeId, binding.getQueueName());
            }
         }
         
         //Remove the address mapping
         nodeIdAddressMap.remove(nodeId);
      }
      finally
      {
         lock.writeLock().release();
      }
   }
      
   //TODO - Sort out serialization properly
   
   private byte[] getStateAsBytes() throws Exception
   {
      List bindings = new ArrayList();
      
      Iterator iter = nameMaps.values().iterator();
      
      while (iter.hasNext())
      {
         Map map  = (Map)iter.next();
         
         Iterator iter2 = map.values().iterator();
         
         while (iter2.hasNext())
         {
            bindings.add(iter2.next());
         }
      }
      
      SharedState state = new SharedState(bindings, nodeIdAddressMap);
      
      byte[] bytes = Util.objectToByteBuffer(state);
      
      return bytes;
   }
   
   private void processStateBytes(byte[] bytes) throws Exception
   {
      SharedState state = (SharedState)Util.objectFromByteBuffer(bytes);
      
      nameMaps.clear();
      
      conditionMap.clear();
                 
      List bindings = state.getBindings();
      
      Iterator iter = bindings.iterator();
      
      while (iter.hasNext())
      {
         Binding binding = (Binding)iter.next();
         
         addBinding(binding);
      }
      
      this.nodeIdAddressMap.clear();
      
      this.nodeIdAddressMap.putAll(state.getNodeIdAddressMap());
   }
   
   // Inner classes -------------------------------------------------------------------
    
   /*
    * This class is used to manage state on the control channel
    */
   private class ControlMessageListener implements MessageListener
   {
      public byte[] getState()
      {     
         try
         {
            lock.writeLock().acquire();
         }
         catch (InterruptedException e)
         {
            log.error("Thread Interrupted", e);
         }
         try
         {
            return getStateAsBytes();
         }
         catch (Exception e)
         {
            IllegalStateException e2 = new IllegalStateException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }     
         finally
         {
            lock.writeLock().release();
         }
      }
      
      public void receive(Message message)
      {         
         log.info("Received message on control channel: " + message);
      }
      
      public void setState(byte[] bytes)
      {
         if (bytes != null)
         {
            
            try
            {
               lock.writeLock().acquire();         
            }
            catch (InterruptedException e)
            {
               log.error("Thread interrupted", e);
            }
            try
            {
               processStateBytes(bytes);               
            }
            catch (Exception e)
            {
               IllegalStateException e2 = new IllegalStateException(e.getMessage());
               e2.setStackTrace(e.getStackTrace());
               throw e2;
            }
            finally
            {
               lock.writeLock().release();
            }
         }
               
         synchronized (setStateLock)
         {
            stateSet = true;
            setStateLock.notify();
         }
      }      
   }
   
   /*
    * We use this class so we notice when members leave the group
    */
   private class ControlMembershipListener implements MembershipListener
   {
      public void block()
      {
         //NOOP
      }

      public void suspect(Address address)
      {
         //NOOP
      }

      public void viewAccepted(View view)
      {
         if (currentView != null)
         {
            Iterator iter = currentView.getMembers().iterator();
            
            while (iter.hasNext())
            {
               Address address = (Address)iter.next();
               
               if (!view.containsMember(address))
               {
                  //Member must have left                  
                  //We don't remove bindings for ourself
                  
                  Address currentAddress = syncChannel.getLocalAddress();
                  
                  if (!address.equals(currentAddress))
                  {                  
                     try
                     {
                        removeBindingsForAddress(address.toString());
                     }               
                     catch (Exception e)
                     {
                        IllegalStateException e2 = new IllegalStateException(e.getMessage());
                        e2.setStackTrace(e.getStackTrace());
                        throw e2;
                     }
                  }
               }
            }
         }
         
         currentView = view;
      }

      public byte[] getState()
      {        
         //NOOP
         return null;
      }     
   }
   
   
   /*
    * This class is used to listen for messages on the data channel
    */
   private class DataReceiver implements Receiver
   {
      public void block()
      {   
         //NOOP
      }

      public void suspect(Address address)
      { 
         //NOOP
      }

      public void viewAccepted(View view)
      { 
         //NOOP
      }

      public byte[] getState()
      {         
         //NOOP
         return null;
      }
      
      public void receive(Message message)
      {
         try
         {
            //TODO handle deserialization more efficiently            
            ClusterRequest request = (ClusterRequest)message.getObject();
            
            request.execute(ClusteredPostOfficeImpl.this);
         }
         catch (Exception e)
         {
            IllegalStateException e2 = new IllegalStateException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }         
      }
      
      public void setState(byte[] bytes)
      {
         //NOOP         
      }      
   }
          
   /*
    * This class is used to handle synchronous requests
    */
   private class PostOfficeRequestHandler implements RequestHandler
   {
      public Object handle(Message message)
      {
         //TODO handle deserialization more efficiently
         
         ClusterRequest request = (ClusterRequest)message.getObject();
              
         try
         {            
            request.execute(ClusteredPostOfficeImpl.this);
         }
         catch (Exception e)
         {
            IllegalStateException e2 = new IllegalStateException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }
         return null;
      }      
   }   
}