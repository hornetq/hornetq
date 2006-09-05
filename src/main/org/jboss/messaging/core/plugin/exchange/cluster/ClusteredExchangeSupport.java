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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.exchange.Binding;
import org.jboss.messaging.core.plugin.exchange.DefaultBinding;
import org.jboss.messaging.core.plugin.exchange.ExchangeSupport;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.Util;

import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.WriterPreferenceReadWriteLock;

/**
 * A ClusteredExchangeSupport
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class ClusteredExchangeSupport extends ExchangeSupport implements ExchangeInternal
{
   private static final Logger log = Logger.getLogger(ClusteredExchangeSupport.class);
      
   //TODO - make configurable
   private static final int GET_STATE_TIMEOUT = 5000;
   
   private static final int CAST_TIMEOUT = 5000;
                    
   protected Channel syncChannel;
   
   protected Channel asyncChannel;
   
   private MessageDispatcher controlMessageDispatcher;
   
   private MessageListener controlMessageListener;
   
   private Receiver dataReceiver;
   
   private MembershipListener controlMembershipListener;
   
   private RequestHandler requestHandler;
   
   private Object setStateLock = new Object();
   
   private boolean stateSet;
   
   private View currentView;
   
   private Address currentAddress;
   
   //Map < Address, node id>
   private Map nodeIdAddressMap;
   
   private Map holdingArea;
   
   protected PersistenceManager pm;
   
   public ClusteredExchangeSupport() throws Exception
   {          
   }
   
   /*
    * This constructor should only be used for testing
    */
   protected ClusteredExchangeSupport(DataSource ds, TransactionManager tm)
   {
      super(ds, tm);
   }
   
   //TODO - this is a bit of a mess - this stuff should go into startService
   //but we can't currently do that since startService will get called
   //before we get a chance to inject the attributes
   protected void injectAttributes(Channel syncChannel, Channel asynchChannel,
                                   String groupName, String exchangeName, String nodeID,
                                   MessageStore ms, IdManager im, QueuedExecutorPool pool,
                                   PersistenceManager pm) throws Exception
   { 
      this.syncChannel = syncChannel;
      
      this.asyncChannel = asynchChannel;
       
      this.nodeIdAddressMap = new HashMap();
      
      this.holdingArea = new HashMap();
      
      this.pm = pm;
             
      //We don't want to receive local messages on any of the channels
      syncChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
      asynchChannel.setOpt(Channel.LOCAL, Boolean.FALSE);

      this.controlMessageListener = new ControlMessageListener();
        
      this.requestHandler = new ExchangeRequestHandler();
      
      this.controlMembershipListener = new ControlMembershipListener();
      
      this.controlMessageDispatcher = new MessageDispatcher(syncChannel, controlMessageListener,
                                                            controlMembershipListener, requestHandler, true);      
      this.dataReceiver = new DataReceiver();
      
      asyncChannel.setReceiver(dataReceiver);               
      
      syncChannel.connect(groupName);
      
      asynchChannel.connect(groupName);

      currentAddress = syncChannel.getLocalAddress();
      
      super.injectAttributes(exchangeName, nodeID, ms, im, pool);      
        
      handleAddressNodeMapping(currentAddress, nodeId);
      
      syncSendRequest(new SendNodeIdRequest(currentAddress, nodeId));
   }
   
   // ServiceMBeanSupport overrides ---------------------------------
   
   protected void startService() throws Exception
   {      
      super.startService();
   }
   
   protected void stopService() throws Exception
   {
      super.stopService();
      
      syncChannel.close();
      
      asyncChannel.close();
   }
   
   // Exchange implementation ---------------------------------------        
   
   public Binding bindQueue(String queueName, String condition, Filter filter, boolean noLocal, boolean durable,
                            MessageStore ms, PersistenceManager pm,
                            int fullSize, int pageSize, int downCacheSize) throws Exception
   {           
      Binding binding = super.bindQueue(queueName, condition, filter, noLocal, durable,
                                        ms, pm, fullSize, pageSize, downCacheSize);
      
      BindRequest request =
         new BindRequest(nodeId, queueName, condition, filter == null ? null : filter.getFilterString(),
                         noLocal, binding.getChannelId(), durable);
      
      syncSendRequest(request);
      
      return binding;
   }
   
   public Binding unbindQueue(String queueName) throws Throwable
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
      this.asyncSendRequest(new CheckMessage(nodeId));
   }
   
   // ExchangeInternal implementation ------------------------------------------------------------------
   
   /*
    * Called when another node adds a binding
    */
   public void addBindingFromCluster(String nodeId, String queueName, String condition,
                                      String filterString, boolean noLocal, long channelID, boolean durable)
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
         
         binding = new DefaultBinding(nodeId, queueName, condition, filterString,
                                     noLocal, channelID, durable); 
         
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
   
   public void routeFromCluster(org.jboss.messaging.core.Message message, String routingKey) throws Exception
   {
      // Need to reference the message
      MessageReference ref = null;
      try
      {
         ref = ms.reference(message);
         
         //Route immediately
         routeFromCluster(ref, routingKey); 
      }
      finally
      {
         if (ref != null)
         {
            ref.releaseMemoryReference();
         }
      }
   }
   
   public void asyncSendRequest(ExchangeRequest request) throws Exception
   {            
      //TODO - handle serialization more efficiently
      asyncChannel.send(new Message(null, null, request));
   }
   
   public void addToHoldingArea(TransactionId id, List messageHolders) throws Exception
   {
      synchronized (holdingArea)
      {
         holdingArea.put(id, messageHolders);
      }      
   }
         
   public void commitTransaction(TransactionId id) throws Exception
   {
      List messageHolders = null;
      
      synchronized (holdingArea)
      {
         messageHolders = (List)holdingArea.remove(id);
      }
      
      if (messageHolders == null)
      {
         throw new IllegalStateException("Cannot find messages for transaction id: " + id);
      }
      
      Iterator iter = messageHolders.iterator();
      
      while (iter.hasNext())
      {
         MessageHolder holder = (MessageHolder)iter.next();
         
         routeFromCluster(holder.getMessage(), holder.getRoutingKey());
      }
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
                     
                     routeFromCluster(holder.getMessage(), holder.getRoutingKey());
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
      
      List bindings = listBindingsForWildcard(holder.getRoutingKey());
      
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
   
   //ExchangeSupport overrides -------------------------------------------------
   
   protected void loadBindings() throws Exception
   {
      // TODO I need to know whether this call times out - how do I know this??
      boolean isState = syncChannel.getState(null, GET_STATE_TIMEOUT);
                              
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
   
   
   protected void syncSendRequest(ExchangeRequest request) throws Exception
   {            
      //TODO - handle serialization more efficiently
      
      Message message = new Message(null, null, request);      
      
      controlMessageDispatcher.castMessage(null, message, GroupRequest.GET_ALL, CAST_TIMEOUT);
   }
   
   protected abstract void routeFromCluster(MessageReference ref, String routingKey) throws Exception;      
       
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
            MessageRequest request = (MessageRequest)message.getObject();
            
            request.execute(ClusteredExchangeSupport.this);
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
   private class ExchangeRequestHandler implements RequestHandler
   {
      public Object handle(Message message)
      {
         //TODO handle deserialization more efficiently
         
         MessageRequest request = (MessageRequest)message.getObject();
              
         try
         {            
            request.execute(ClusteredExchangeSupport.this);
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