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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.DefaultBinding;
import org.jboss.messaging.core.plugin.postoffice.DefaultPostOffice;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.StreamUtils;
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
import org.w3c.dom.Element;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A DefaultClusteredPostOffice
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultClusteredPostOffice extends DefaultPostOffice implements ClusteredPostOffice, PostOfficeInternal
{
   private static final Logger log = Logger.getLogger(DefaultClusteredPostOffice.class);
                        
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
   
   private Element syncChannelConfigE;
   
   private Element asyncChannelConfigE;
   
   private String syncChannelConfigS;
   
   private String asyncChannelConfigS;
   
   private long stateTimeout;
   
   private long castTimeout;
   
   private MessagePullPolicy messagePullPolicy;
   
   private ClusterRouterFactory routerFactory;
   
   private int pullSize;
   
   private Map routerMap;
   
   private StatsSender statsSender;
   
   private boolean started;
      
   public DefaultClusteredPostOffice()
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
   public DefaultClusteredPostOffice(DataSource ds, TransactionManager tm, Properties sqlProperties,
            boolean createTablesOnStartup,
            int nodeId, String officeName, MessageStore ms,
            PersistenceManager pm,
            TransactionRepository tr,
            FilterFactory filterFactory,
            QueuedExecutorPool pool,                              
            String groupName,
            Element syncChannelConfig,
            Element asyncChannelConfig,
            long stateTimeout, long castTimeout,
            MessagePullPolicy redistributionPolicy,
            ClusterRouterFactory rf,
            int pullSize,
            long statsSendPeriod) throws Exception
   {            
      this(ds, tm, sqlProperties, createTablesOnStartup, nodeId, officeName, ms,
           pm, tr, filterFactory, pool, groupName, stateTimeout, castTimeout, redistributionPolicy,
           rf, pullSize, statsSendPeriod);
      
      this.syncChannelConfigE = syncChannelConfig;      
      this.asyncChannelConfigE = asyncChannelConfig;     
   }
     
   /*
    * Constructor using String for configuration
    */
   public DefaultClusteredPostOffice(DataSource ds, TransactionManager tm, Properties sqlProperties,
                              boolean createTablesOnStartup,
                              int nodeId, String officeName, MessageStore ms,
                              PersistenceManager pm,
                              TransactionRepository tr,
                              FilterFactory filterFactory,
                              QueuedExecutorPool pool,                              
                              String groupName,
                              String syncChannelConfig,
                              String asyncChannelConfig,
                              long stateTimeout, long castTimeout,
                              MessagePullPolicy redistributionPolicy,                      
                              ClusterRouterFactory rf,
                              int pullSize,
                              long statsSendPeriod) throws Exception
   {            
      this(ds, tm, sqlProperties, createTablesOnStartup, nodeId, officeName, ms,
           pm, tr, filterFactory, pool, groupName, stateTimeout, castTimeout, redistributionPolicy,
           rf, pullSize, statsSendPeriod);

      this.syncChannelConfigS = syncChannelConfig;      
      this.asyncChannelConfigS = asyncChannelConfig;     
   }
   
   private DefaultClusteredPostOffice(DataSource ds, TransactionManager tm, Properties sqlProperties,
                               boolean createTablesOnStartup,
                               int nodeId, String officeName, MessageStore ms,
                               PersistenceManager pm,                               
                               TransactionRepository tr,
                               FilterFactory filterFactory,
                               QueuedExecutorPool pool,
                               String groupName,
                               long stateTimeout, long castTimeout,                             
                               MessagePullPolicy redistributionPolicy,                               
                               ClusterRouterFactory rf,
                               int pullSize,
                               long statsSendPeriod)
   {
      super (ds, tm, sqlProperties, createTablesOnStartup, nodeId, officeName, ms, pm, tr, filterFactory,
             pool);
                   
      this.pm = pm;
      
      this.groupName = groupName;
      
      this.stateTimeout = stateTimeout;
      
      this.castTimeout = castTimeout;
      
      this.messagePullPolicy = redistributionPolicy;
      
      this.routerFactory = rf;
      
      this.pullSize = pullSize;
       
      routerMap = new HashMap();
      
      statsSender = new StatsSender(this, statsSendPeriod);
      
      init();
   }

   // MessagingComponent overrides
   // --------------------------------------------------------------
   
   public synchronized void start() throws Exception
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
      
      log.info(this.nodeId + " address is " + currentAddress);
             
      handleAddressNodeMapping(currentAddress, nodeId);
      
      syncSendRequest(new SendNodeIdRequest(currentAddress, nodeId));           
      
      statsSender.start();
      
      started = true;      
   }

   public synchronized void stop() throws Exception
   {
      super.stop();
      
      statsSender.stop();
         
      syncChannel.close();
      
      asyncChannel.close();
      
      started = false;
   }  
   
   // PostOffice implementation ---------------------------------------        

   public Binding bindClusteredQueue(String condition, LocalClusteredQueue queue) throws Exception
   {           
      log.info(this.nodeId + " binding clustered queue: " + queue + " with condition: " + condition);
      
      if (queue.getNodeId() != this.nodeId)
      {
         throw new IllegalArgumentException("Queue node id does not match office node id");
      }
      
      Binding binding = (Binding)super.bindQueue(condition, queue);
      
      BindRequest request =
         new BindRequest(nodeId, queue.getName(), condition, queue.getFilter() == null ? null : queue.getFilter().getFilterString(),
                         binding.getQueue().getChannelID(), queue.isRecoverable());
      
      syncSendRequest(request);
        
      return binding;
   }
   
   public Binding unbindClusteredQueue(String queueName) throws Throwable
   {
      Binding binding = (Binding)super.unbindQueue(queueName);
      
      UnbindRequest request = new UnbindRequest(nodeId, queueName);
      
      syncSendRequest(request);
      
      return binding;
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
         ClusteredBindings cb = (ClusteredBindings)conditionMap.get(condition);
         
         boolean startInternalTx = false;
         
         int lastNodeId = -1;
         
         if (cb != null)
         {
            if (tx == null && ref.isReliable())
            {                
               if (!(cb.getDurableCount() == 0 || (cb.getDurableCount() == 1 && cb.getLocalDurableCount() == 1)))
               {
                  // When routing a persistent message without a transaction then we may need to start an 
                  // internal transaction in order to route it.
                  // This is so we can guarantee the message is delivered to all or none of the subscriptions.
                  // We need to do this if there is anything other than
                  // No durable subs or exactly one local durable sub
                  startInternalTx = true;
               }
            }                        
            
            if (startInternalTx)
            {
               tx = tr.createTransaction();
            }
                
            int numberRemote = 0;
            
            Map queueNameNodeIdMap = null;
            
            long lastChannelId = -1;
            
            Collection routers = cb.getRouters();

            Iterator iter = routers.iterator();
                     
            while (iter.hasNext())
            {
               ClusterRouter router = (ClusterRouter)iter.next();
               
               Delivery del = router.handle(null, ref, tx);
               
               if (del != null && del.isSelectorAccepted())
               {
                  routed = true;
               
                  ClusteredQueue queue = (ClusteredQueue)del.getObserver();
                  
                  log.info("Routing message to queue:" + queue.getName() + " on node " + queue.getNodeId());
                  
                  if (router.numberOfReceivers() > 1)
                  {
                     //We have now chosen which one will receive the message so we need to add this
                     //information to a map which will get sent when casting - so the the queue
                     //on the receiving node knows whether to receive the message
                     if (queueNameNodeIdMap == null)
                     {
                        queueNameNodeIdMap = new HashMap();
                     }
                     
                     queueNameNodeIdMap.put(queue.getName(), new Integer(queue.getNodeId()));
                  }
                  
                  if (!queue.isLocal())
                  {
                     //We need to send the message remotely
                     numberRemote++;
                     
                     lastNodeId = queue.getNodeId();                                                               
                                          
                     lastChannelId = queue.getChannelID();
                  }
               }
            }
            
            //Now we've sent the message to any local queues, we might also need
            //to send the message to the other office instances on the cluster if there are
            //queues on those nodes that need to receive the message
            
            //TODO - there is an innefficiency here, numberRemote does not take into account that more than one
            //of the number remote may be on the same node, so we could end up multicasting
            //when unicast would do
            if (numberRemote > 0)
            {
               if (tx == null)
               {
                  if (numberRemote == 1)
                  {
                  //   log.info("unicast no tx");
                     //Unicast - only one node is interested in the message
                     
                     //TODO - temporarily commented out until can get unicast to work                     
                     //asyncSendRequest(new MessageRequest(condition, ref.getMessage(), null), lastNodeId);
                     asyncSendRequest(new MessageRequest(condition, ref.getMessage(), queueNameNodeIdMap));
                  }
                  else
                  {
                  //   log.info("multicast no tx");
                     //Multicast - more than one node is interested
                     asyncSendRequest(new MessageRequest(condition, ref.getMessage(), queueNameNodeIdMap));
                  }                                 
               }
               else
               {
                  CastMessagesCallback callback = (CastMessagesCallback)tx.getCallback(this);
                  
                  if (callback == null)
                  {
                     callback = new CastMessagesCallback(nodeId, tx.getId(), DefaultClusteredPostOffice.this);
                     
                     //This callback MUST be executed first
                     
                     //Execution order is as follows:
                     //Before commit:
                     //1. Cast messages across network - get added to holding area (if persistent) on receiving
                     //nodes
                     //2. Persist messages in persistent store
                     //After commit
                     //1. Cast commit message across network
                     tx.addFirstCallback(callback, this);
                  }
                      
                  callback.addMessage(condition, ref.getMessage(), queueNameNodeIdMap,
                                      numberRemote == 1 ? lastNodeId : -1,
                                      lastChannelId);    
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
         
      return routed; 
   }
   
   public boolean isLocal()
   {
      return false;
   }
   
   // PostOfficeInternal implementation ------------------------------------------------------------------
   
   /*
    * Called when another node adds a binding
    */
   public void addBindingFromCluster(int nodeId, String queueName, String condition,
                                     String filterString, long channelID, boolean durable)
      throws Exception
   {
      lock.writeLock().acquire();
      
      log.info(this.nodeId + " adding binding from node: " + nodeId +" queue: " + queueName + " with condition: " + condition);
            
      try
      {                     
         //Sanity

         if (!nodeIdAddressMap.containsKey(new Integer(nodeId)))
         {
            throw new IllegalStateException("Cannot find address for node: " + nodeId);
         }
         
         // We currently only allow one binding per name per node
         Map nameMap = (Map)nameMaps.get(new Integer(nodeId));
         
         Binding binding = null;
         
         if (nameMap != null)
         {
            binding = (Binding)nameMap.get(queueName);
         }
         
         if (binding != null)
         {
            throw new IllegalArgumentException(this.nodeId + "Binding already exists for node Id " + nodeId + " queue name " + queueName);
         }
            
         binding = this.createBinding(nodeId, condition, queueName, channelID, filterString, durable);
         
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
   public void removeBindingFromCluster(int nodeId, String queueName) throws Exception
   {
      lock.writeLock().acquire();
      
      try
      {         
         // Sanity
         if (!nodeIdAddressMap.containsKey(new Integer(nodeId)))
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
   
   public void handleAddressNodeMapping(Address address, int nodeId) throws Exception
   {
      lock.writeLock().acquire();
      
      try
      { 
         nodeIdAddressMap.put(new Integer(nodeId), address);
      }
      finally
      {
         lock.writeLock().release();
      }
   }
   
   public void routeFromCluster(org.jboss.messaging.core.Message message, String routingKey,
                                Map queueNameNodeIdMap) throws Exception
   {
      log.info(this.nodeId + " received route from cluster, ref = " + message.getMessageID() + " routing key " +
               routingKey + " map " + queueNameNodeIdMap);
      
      lock.readLock().acquire();  
      
      // Need to reference the message
      MessageReference ref = null;
      try
      {
         if (message.isReliable())
         {
            // It will already have been persisted on the sender's side
            message.setPersisted(true);
         }
         
         ref = ms.reference(message);
              
         // We route on the condition
         DefaultClusteredBindings cb = (DefaultClusteredBindings)conditionMap.get(routingKey);
         
         if (cb != null)
         {                      
            Collection bindings = cb.getAllBindings();
            
            Iterator iter = bindings.iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
                                                     
               if (binding.getNodeId() == this.nodeId)
               {  
                  boolean handle = true;
                  
                  if (queueNameNodeIdMap != null)
                  {           
                     Integer in = (Integer)queueNameNodeIdMap.get(binding.getQueue().getName());
                     
                     //When there are more than one queues with the same name across the cluster we only
                     //want to chose one of them
                     
                     if (in != null)
                     {
                        handle = in.intValue() == nodeId;
                     }
                  }
                  
                  if (handle)
                  {                     
                     //It's a local binding so we pass the message on to the subscription
                     
                     LocalClusteredQueue queue = (LocalClusteredQueue)binding.getQueue();
                     
                     Delivery del = queue.handleFromCluster(ref);         
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
   
   /*
    * Multicast a message to all members of the group
    */
   public void asyncSendRequest(ClusterRequest request) throws Exception
   {            
      byte[] bytes = writeRequest(request);
         
      asyncChannel.send(new Message(null, null, bytes));
   }
   
   /*
    * Unicast a message to one members of the group
    */
   public void asyncSendRequest(ClusterRequest request, int nodeId) throws Exception
   {               
      Address address = this.getAddressForNodeId(nodeId);
      
      if (address == null)
      {
         throw new IllegalArgumentException("Cannot find address for node " + nodeId);
      }
      
      byte[] bytes = writeRequest(request);
            
      Message m = new Message(address, null, bytes);
      
      asyncChannel.send(m);
   }
   
   /*
    * Unicast a sync request
    */
   public Object syncSendRequest(ClusterRequest request, int nodeId, boolean ignoreNoAddress) throws Exception
   {              
      Address address = this.getAddressForNodeId(nodeId);
      
      if (address == null)
      {
         if (ignoreNoAddress)
         {
            return null;
         }
         else
         {
            throw new IllegalArgumentException("Cannot find address for node " + nodeId);
         }
      }
      
      byte[] bytes = writeRequest(request);
            
      Message message = new Message(address, null, bytes);      
      
      Object result = controlMessageDispatcher.sendMessage(message, GroupRequest.GET_FIRST, castTimeout);
       
      return result;
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
   
   public void commitTransaction(TransactionId id) throws Throwable
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
   
   /**
    * Check for any transactions that need to be committed or rolled back
    */
   public void check(Integer nodeId) throws Throwable
   {
      lock.readLock().acquire();
      
      synchronized (holdingArea)
      {
         Iterator iter = holdingArea.entrySet().iterator();
         
         List toRemove = new ArrayList();
         
         while (iter.hasNext())
         {
            Map.Entry entry = (Map.Entry)iter.next();
            
            TransactionId id = (TransactionId)entry.getKey();
            
            if (id.getNodeId() == nodeId.intValue())
            {
               ClusterTransaction tx = (ClusterTransaction)iter.next();
               
               boolean commit = tx.check(this);
               
               if (commit)
               {
                  tx.commit(this);
               }
               else
               {
                  tx.rollback(this);
               }
               
               toRemove.add(tx);
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
   
   public synchronized void sendQueueStats() throws Exception
   {
      if (!started)
      {
         return;
      }
      
      lock.readLock().acquire();
      
      List statsList = null;      
      
      try
      {         
         Map nameMap = (Map)nameMaps.get(new Integer(nodeId));
         
         if (nameMap != null)
         {            
            Iterator iter = nameMap.values().iterator();
                     
            while (iter.hasNext())
            {
               Binding bb = (Binding)iter.next();
               
               LocalClusteredQueue q = (LocalClusteredQueue)bb.getQueue();
                             
               if (q.isActive())
               {                                                      
                  QueueStats stats = q.getStats();
                                             
                  //We don't bother sending the stats if there's no significant change in the values
                  
                  if (q.changedSignificantly())
                  {
                     if (statsList == null)
                     {
                        statsList = new ArrayList();
                     }

                     statsList.add(stats);
                  } 
               }
            }
         }
      }
      finally
      {
         lock.readLock().release();
      }
      
      if (statsList != null)
      {
         ClusterRequest req = new QueueStatsRequest(nodeId, statsList);
         
         asyncSendRequest(req);
      }
   }
   
   public void updateQueueStats(int nodeId, List statsList) throws Exception
   {
      lock.readLock().acquire();
      
      try
      {      
         if (nodeId == this.nodeId)
         {
            //Sanity check
            throw new IllegalStateException("Cannot update queue stats for current node");
         }
         
         Map nameMap = (Map)nameMaps.get(new Integer(nodeId));
         
         if (nameMap == null)
         {
            //This is ok, the node might have left
            log.info("But I have no bindings for " + nodeId);
         }
         else
         {     
            Iterator iter = statsList.iterator();
            
            while (iter.hasNext())
            {
               QueueStats st = (QueueStats)iter.next();
               
               Binding bb = (Binding)nameMap.get(st.getQueueName());
               
               if (bb == null)
               {
                  throw new IllegalStateException("Cannot find binding for queue name: " + st.getQueueName());
               }
               
               RemoteQueueStub stub = (RemoteQueueStub)bb.getQueue();
               
               stub.setStats(st);
               
               ClusterRouter router = (ClusterRouter)routerMap.get(st.getQueueName());
               
               LocalClusteredQueue localQueue = router.getLocalQueue();
               
               if (localQueue != null)
               {               
                  RemoteQueueStub toQueue = (RemoteQueueStub)messagePullPolicy.chooseQueue(router.getQueues());
                  
                  if (toQueue != null)
                  {
                     localQueue.setPullInfo(toQueue, pullSize);
                     
                     //We now trigger delivery - this may cause a pull event
                     localQueue.deliver(false);
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
   
   public boolean referenceExistsInStorage(long channelID, long messageID) throws Exception
   {
      return pm.referenceExists(channelID, messageID);
   } 
   
   public List getDeliveries(String queueName, int numMessages) throws Exception
   {
      Binding binding = getBindingForQueueName(queueName);
      
      if (binding == null)
      {
         throw new IllegalArgumentException("Cannot find binding for queue " + queueName);
      }
      
      LocalClusteredQueue queue = (LocalClusteredQueue)binding.getQueue();
      
      List dels = queue.getDeliveries(numMessages);
      
      return dels;
   }
   
   public Address getAddressForNodeId(int nodeId) throws Exception
   {
      lock.readLock().acquire();
      
      try
      {
         return (Address)nodeIdAddressMap.get(new Integer(nodeId));
      }
      finally
      {
         lock.readLock().release();      
      }
   }
   
  
               
   // Public ------------------------------------------------------------------------------------------
      
   // Protected ---------------------------------------------------------------------------------------
        
   protected void addToConditionMap(Binding binding)
   {
      String condition = binding.getCondition();
      
      ClusteredBindings bindings = (ClusteredBindings)conditionMap.get(condition);
      
      if (bindings == null)
      {
         bindings = new DefaultClusteredBindings(nodeId);
         
         conditionMap.put(condition, bindings);
      }
      
      bindings.addBinding(binding);
      
      String queueName = binding.getQueue().getName();
      
      ClusterRouter router = (ClusterRouter)routerMap.get(queueName);
      
      if (router == null)
      {
         router = routerFactory.createRouter();
         
         routerMap.put(queueName, router);
         
         bindings.addRouter(queueName, router);
      }
      
      router.add(binding.getQueue());                  
   }

   protected void removeFromConditionMap(Binding binding)
   {
      ClusteredBindings bindings = (ClusteredBindings)conditionMap.get(binding.getCondition());
      
      if (bindings == null)
      {
         throw new IllegalStateException("Cannot find condition bindings for " + binding.getCondition());
      }
      
      boolean removed = bindings.removeBinding(binding);
      
      if (!removed)
      {
         throw new IllegalStateException("Cannot find binding in condition binding list");
      }           
      
      if (bindings.isEmpty())
      {
         conditionMap.remove(binding.getCondition());
      }        
      
      String queueName = binding.getQueue().getName();
      
      ClusterRouter router = (ClusterRouter)routerMap.get(queueName);
      
      if (router == null)
      {
         throw new IllegalStateException("Cannot find router with name " + queueName);
      }
      
      removed = router.remove(binding.getQueue());
      
      if (!removed)
      {
         throw new IllegalStateException("Cannot find router in map");
      }
      
      if (router.getQueues().isEmpty())
      {
         routerMap.remove(queueName);
      }      
   }

   protected void loadBindings() throws Exception
   {
      // TODO I need to know whether this call times out - how do I know this??
      boolean isState = syncChannel.getState(null, stateTimeout);
                              
      if (!isState)
      {       
         //Must be first member in group or non clustered- we load the state ourself from the database
         
         log.info("First member - so loading bindings from db");
         
         super.loadBindings();      
      }
      else
      {
         //The state will be set in due course via the MessageListener - we must wait until this happens
         
         log.info("Not first member - so loading state from group.. waiting");
         
         synchronized (setStateLock)
         {
            //TODO we should implement a timeout on this
            while (!stateSet)
            {
               setStateLock.wait();
            } 
         }
         
         log.info("Got state");
      }
   }
   
   protected Binding createBinding(int nodeId, String condition, String queueName, long channelId, String filterString, boolean durable) throws Exception
   {            
      Filter filter = filterFactory.createFilter(filterString);
         
      Queue queue;
      if (nodeId == this.nodeId)
      {
         QueuedExecutor executor = (QueuedExecutor)pool.get();
         
         queue = new LocalClusteredQueue(this, nodeId, queueName, channelId, ms, pm, true,
                                         durable, executor, filter, tr);
      }
      else
      {
         queue = new RemoteQueueStub(nodeId, queueName, channelId, durable, pm, filter);
      }
      
      Binding binding = new DefaultBinding(nodeId, condition, queue);
      
      return binding;
   }
   
   
   
   // Private ------------------------------------------------------------------------------------------
           
   
   
   
   /*
    * Multicast a sync request
    */
   private void syncSendRequest(ClusterRequest request) throws Exception
   {            
      byte[] bytes = writeRequest(request);
            
      Message message = new Message(null, null, bytes);      
      
      controlMessageDispatcher.castMessage(null, message, GroupRequest.GET_ALL, castTimeout);
   }
   

   private Integer getNodeIdForAddress(Address address) throws Exception
   {
      lock.readLock().acquire();
      try
      { 
         Iterator iter = nodeIdAddressMap.entrySet().iterator();
         
         Integer nodeId = null;
         while (iter.hasNext())
         {
            Map.Entry entry = (Map.Entry)iter.next();
            
            Address adr = (Address)entry.getValue();
            
            if (adr.equals(address))
            {
               nodeId = (Integer)entry.getKey();
            }
         }
         return nodeId;
      }
      finally
      {
         lock.readLock().release();
      }
   }
       
   private void removeBindingsForAddress(Integer nodeId) throws Exception
   {
      lock.writeLock().acquire();
      
      try
      {          
         Map nameMap = (Map)nameMaps.get(nodeId);

         if (nameMap != null)
         {
            List toRemove = new ArrayList();
            
            Iterator iter = nameMap.values().iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
               
               if (!binding.getQueue().isRecoverable())
               {
                  //We only remove the non durable bindings - we still need to be able to handle
                  //messages for a durable subscription "owned" by a node that is not active any more!
                  toRemove.add(binding);
               }
            }
            
            iter = toRemove.iterator();
            
            while (iter.hasNext())
            {
               Binding binding = (Binding)iter.next();
               
               removeBinding(nodeId.intValue(), binding.getQueue().getName());
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
            Binding binding = (Binding)iter2.next();
                      
            Queue queue = binding.getQueue();        
            
            BindingInfo info = new BindingInfo(binding.getNodeId(), queue.getName(),
                                               binding.getCondition(),
                                               queue.getFilter() == null ? null : queue.getFilter().getFilterString(),
                                               queue.getChannelID(),
                                               queue.isRecoverable());    
            bindings.add(info);
         }
      }
      
      SharedState state = new SharedState(bindings, nodeIdAddressMap);
      
      byte[] bytes = StreamUtils.toBytes(state); 
           
      return bytes;
   }
   
   private void processStateBytes(byte[] bytes) throws Exception
   {
      log.info("Receiving state from group...");
      
      SharedState state = new SharedState();
      
      StreamUtils.fromBytes(state, bytes);
      
      nameMaps.clear();
      
      conditionMap.clear();
                 
      List bindings = state.getBindings();
      
      Iterator iter = bindings.iterator();
      
      while (iter.hasNext())
      {
         BindingInfo info = (BindingInfo)iter.next();
         
         Binding binding = this.createBinding(info.getNodeId(), info.getCondition(), info.getQueueName(), info.getChannelId(), info.getFilterString(), info.isDurable());
         
         if (binding.getNodeId() == this.nodeId)
         {
            //We deactivate if this is one of our own bindings - it can only
            //be one of our own durable bindings - and since state is retrieved before we are fully started
            //then the sub hasn't been deployed so must be deactivated
            
            binding.getQueue().deactivate();            
         }
            
         addBinding(binding);         
      }
      
      this.nodeIdAddressMap.clear();
      
      this.nodeIdAddressMap.putAll(state.getNodeIdAddressMap());
   }
   
   
   
   private byte[] writeRequest(ClusterRequest request) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(2048);
      
      DataOutputStream daos = new DataOutputStream(baos);
      
      ClusterRequest.writeToStream(daos, request);
            
      daos.flush();
      
      return baos.toByteArray();
   }
   
   private ClusterRequest readRequest(byte[] bytes) throws Exception
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      
      DataInputStream dais = new DataInputStream(bais);
      
      ClusterRequest request = ClusterRequest.createFromStream(dais);
      
      dais.close();
      
      return request;            
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
            log.error("Caught Exception in MessageListener", e);
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
         //log.info("Received message on control channel: " + message);
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
               log.error("Caught Exception in MessageListener", e);
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
         log.info("Got new view, size=" + view.size());
         
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
                        Integer nodeId = getNodeIdForAddress(address);
                        
                        if (nodeId == null)
                        {
                           throw new IllegalStateException("Cannot find node id for address: " + address);
                        }
                        
                        //Perform a check - the member might have crashed and left uncommitted transactions
                        //we need to resolve this
                        check(nodeId);
                        
                        removeBindingsForAddress(nodeId);
                     }               
                     catch (Throwable e)
                     {
                        log.error("Caught Exception in MembershipListener", e);
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
            byte[] bytes = message.getBuffer();
            
            ClusterRequest request = readRequest(bytes);
            
            request.execute(DefaultClusteredPostOffice.this);            
         }
         catch (Throwable e)
         {
            log.error("Caught Exception in Receiver", e);
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
         try
         {   
            byte[] bytes = message.getBuffer();
            
            ClusterRequest request = readRequest(bytes);
            
            Object result = request.execute(DefaultClusteredPostOffice.this);
            
            return result;
         }
         catch (Throwable e)
         {
            log.error("Caught Exception in RequestHandler", e);
            IllegalStateException e2 = new IllegalStateException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }         
      }      
   }
}