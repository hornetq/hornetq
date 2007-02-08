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
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationListener;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationFilter;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;

import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;
import org.jboss.messaging.core.plugin.contract.Condition;
import org.jboss.messaging.core.plugin.contract.ConditionFactory;
import org.jboss.messaging.core.plugin.contract.FailoverMapper;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.ReplicationListener;
import org.jboss.messaging.core.plugin.contract.Replicator;
import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.DefaultBinding;
import org.jboss.messaging.core.plugin.postoffice.DefaultPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.jchannelfactory.JChannelFactory;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.StreamUtils;
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

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class DefaultClusteredPostOffice extends DefaultPostOffice
   implements ClusteredPostOffice, PostOfficeInternal, Replicator
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(DefaultClusteredPostOffice.class);

   // Key for looking up node id -> address info mapping from replicated data
   public static final String ADDRESS_INFO_KEY = "ADDRESS_INFO";

   // Key for looking up node id -> failed over for node id mapping from replicated data
   public static final String FAILED_OVER_FOR_KEY = "FAILED_OVER_FOR";

   // Static ---------------------------------------------------------------------------------------

   /**
    * @param map - Map<Integer(nodeID)-Integer(failoverNodeID)>
    */
   public static String dumpFailoverMap(Map map)
   {
      StringBuffer sb = new StringBuffer("\n");

      for(Iterator i = map.entrySet().iterator(); i.hasNext(); )
      {
         Map.Entry entry = (Map.Entry)i.next();
         Integer primary = (Integer)entry.getKey();
         Integer secondary = (Integer)entry.getValue();
         sb.append("             ").append(primary).append("->").append(secondary).append("\n");
      }
      return sb.toString();
   }

   /**
    * @param map - Map<Integer(nodeID)-PostOfficeAddressInfo>
    */
   public static String dumpClusterMap(Map map)
   {
      StringBuffer sb = new StringBuffer("\n");

      for(Iterator i = map.entrySet().iterator(); i.hasNext(); )
      {
         Map.Entry entry = (Map.Entry)i.next();
         Integer nodeID = (Integer)entry.getKey();
         PostOfficeAddressInfo info = (PostOfficeAddressInfo)entry.getValue();
         sb.append("             ").append(nodeID).append("->").append(info).append("\n");
      }
      return sb.toString();
   }

   // Attributes -----------------------------------------------------------------------------------

   // Used for failure testing

   private boolean failBeforeCommit;
   private boolean failAfterCommit;
   private boolean failHandleResult;

   // End of failure testing attributes

   private boolean trace = log.isTraceEnabled();

   private String groupName;

   private volatile boolean started;
   
   //FIXME using a stopping flag is not a good approach and introduces a race condition
   //http://jira.jboss.org/jira/browse/JBMESSAGING-819
   //the code can check stopping and find it to be false, then the service can stop, setting stopping to true
   //then actually stopping the post office, then the same thread that checked stopping continues and performs
   //its action only to find the service stopped
   //Should use a read-write lock instead
   //One way to minimise the chance of the race happening is to sleep for a little while after setting stopping to true
   //before actually stopping the service (see below)
   private volatile boolean stopping;

   private JChannelFactory jChannelFactory;

   private Channel syncChannel;

   private Channel asyncChannel;

   private MessageDispatcher controlMessageDispatcher;

   private Object setStateLock = new Object();

   private boolean stateSet;

   private View currentView;

   private Map replicatedData;

   private Set replicationListeners;

   private Map holdingArea;

   // Map <Integer(nodeID)->Integer(failoverNodeID)>
   private Map failoverMap;

   private Set leftSet;

   private long stateTimeout;

   private long castTimeout;

   private MessagePullPolicy messagePullPolicy;

   private ClusterRouterFactory routerFactory;

   private FailoverMapper failoverMapper;

   private Map routerMap;

   /** List of failed over bindings.
    *  Map<int nodeId, Map<channelId,Binding>>*/
   private Map failedBindings;

   private StatsSender statsSender;

   private ReplicationListener nodeAddressMapListener;

   private NotificationBroadcasterSupport nbSupport;

   private QueuedExecutor viewExecutor;


   // Constructors ---------------------------------------------------------------------------------

   /*
    * Constructor using Element for configuration
    */
   public DefaultClusteredPostOffice(DataSource ds,
                                     TransactionManager tm,
                                     Properties sqlProperties,
                                     boolean createTablesOnStartup,
                                     int nodeId,
                                     String officeName,
                                     MessageStore ms,
                                     PersistenceManager pm,
                                     TransactionRepository tr,
                                     FilterFactory filterFactory,
                                     ConditionFactory conditionFactory,
                                     QueuedExecutorPool pool,
                                     String groupName,
                                     JChannelFactory JChannelFactory,
                                     long stateTimeout, long castTimeout,
                                     MessagePullPolicy redistributionPolicy,
                                     ClusterRouterFactory rf,
                                     FailoverMapper failoverMapper,
                                     long statsSendPeriod)
      throws Exception
   {
      super (ds, tm, sqlProperties, createTablesOnStartup, nodeId, officeName, ms, pm, tr,
             filterFactory, conditionFactory, pool);

      this.groupName = groupName;

      this.stateTimeout = stateTimeout;

      this.castTimeout = castTimeout;

      this.messagePullPolicy = redistributionPolicy;

      this.routerFactory = rf;

      this.failoverMapper = failoverMapper;

      routerMap = new HashMap();

      failedBindings = new LinkedHashMap();

      statsSender = new StatsSender(this, statsSendPeriod);

      holdingArea = new HashMap();

      replicatedData = new HashMap();

      replicationListeners = new LinkedHashSet();

      failoverMap = new LinkedHashMap();

      leftSet = new HashSet();

      nbSupport = new NotificationBroadcasterSupport();

      viewExecutor = new QueuedExecutor();

      this.jChannelFactory = JChannelFactory;
   }

   // MessagingComponent overrides -----------------------------------------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         log.warn("Attempt to start() but " + this + " is already started");
      }

      if (trace) { log.trace(this + " starting"); }

      this.syncChannel = jChannelFactory.createSyncChannel();
      this.asyncChannel = jChannelFactory.createASyncChannel();

      // We don't want to receive local messages on any of the channels
      syncChannel.setOpt(Channel.LOCAL, Boolean.FALSE);

      asyncChannel.setOpt(Channel.LOCAL, Boolean.FALSE);

      MessageListener cml = new ControlMessageListener();
      MembershipListener ml = new ControlMembershipListener();
      RequestHandler rh = new PostOfficeRequestHandler();

      // register as a listener for nodeid-adress mapping events
      nodeAddressMapListener = new NodeAddressMapListener();

      registerListener(nodeAddressMapListener);

      this.controlMessageDispatcher = new MessageDispatcher(syncChannel, cml, ml, rh, true);

      Receiver r = new DataReceiver();
      asyncChannel.setReceiver(r);

      syncChannel.connect(groupName);
      asyncChannel.connect(groupName);

      super.start();

      Address syncAddress = syncChannel.getLocalAddress();
      Address asyncAddress = asyncChannel.getLocalAddress();
      PostOfficeAddressInfo info = new PostOfficeAddressInfo(syncAddress, asyncAddress);
      put(ADDRESS_INFO_KEY, info);

      statsSender.start();

      started = true;

      log.debug(this + " started");
   }

   public synchronized void stop(boolean sendNotification) throws Exception
   {
      if (trace) { log.trace(this + " stopping"); }

      if (!started)
      {
         log.warn("Attempt to stop() but " + this + " is not started");
         return;
      }
      
      //Need to send this *before* stopping
      syncSendRequest(new LeaveClusterRequest(getNodeId()));

      stopping = true;
      
      //FIXME http://jira.jboss.org/jira/browse/JBMESSAGING-819 this is a temporary kludge for now
      Thread.sleep(1000);

      statsSender.stop();

      super.stop(sendNotification);

      //  TODO in case of shared channels, we should have some sort of unsetReceiver(r)
      asyncChannel.setReceiver(null);

      unregisterListener(nodeAddressMapListener);

      // TODO - what happens if we share the channel? Don't we mess up the other applications this way?
      syncChannel.close();

      // TODO - what happens if we share the channel? Don't we mess up the other applications this way?
      asyncChannel.close();

      started = false;

      log.debug(this + " stopped");
   }

   // NotificationBroadcaster implementation -------------------------------------------------------

   public void addNotificationListener(NotificationListener listener,
                                       NotificationFilter filter,
                                       Object object) throws IllegalArgumentException
   {
      nbSupport.addNotificationListener(listener, filter, object);
   }

   public void removeNotificationListener(NotificationListener listener)
      throws ListenerNotFoundException
   {
      nbSupport.removeNotificationListener(listener);
   }

   public MBeanNotificationInfo[] getNotificationInfo()
   {
      return new MBeanNotificationInfo[0];
   }

   // Peer implementation --------------------------------------------------------------------------

   public Set getNodeIDView()
   {
      if (syncChannel == null)
      {
         return Collections.EMPTY_SET;
      }

      Map addressInfo = null;

      synchronized(replicatedData)
      {
         addressInfo = new HashMap((Map)replicatedData.get(ADDRESS_INFO_KEY));
      }

      Set nodeIDView = null;

      for (Iterator i = syncChannel.getView().getMembers().iterator(); i.hasNext(); )
      {
         if (nodeIDView == null)
         {
            nodeIDView = new HashSet();
         }

         Address addr = (Address)i.next();

         for(Iterator j = addressInfo.entrySet().iterator(); j.hasNext(); )
         {
            Map.Entry entry = (Map.Entry)j.next();

            if (((PostOfficeAddressInfo)(entry.getValue())).getSyncChannelAddress().equals(addr))
            {
               nodeIDView.add(entry.getKey());
            }
         }
      }

      return nodeIDView;
   }

   // ClusteredPostOffice implementation -----------------------------------------------------------

   public Binding bindClusteredQueue(Condition condition, LocalClusteredQueue queue) throws Exception
   {
      if (trace) { log.trace(this.currentNodeId + " binding clustered queue: " + queue + " with condition: " + condition); }

      if (queue.getNodeId() != this.currentNodeId)
      {
          log.warn("queue.getNodeId is not this node");
         //throw new IllegalArgumentException("Queue node id does not match office node id");
         // todo what to do when HA failing?
      }

      Binding binding = (Binding)super.bindQueue(condition, queue);

      sendBindRequest(condition, queue, binding);

      return binding;
   }

   public Binding unbindClusteredQueue(String queueName) throws Throwable
   {
      if (trace) { log.trace(this.currentNodeId + " unbind clustered queue: " + queueName); }

      Binding binding = (Binding)super.unbindQueue(queueName);

      UnbindRequest request = new UnbindRequest(this.currentNodeId, queueName);

      syncSendRequest(request);

      return binding;
   }

   public Collection listAllBindingsForCondition(Condition condition) throws Exception
   {
      return listBindingsForConditionInternal(condition, false);
   }

   public Binding getBindingforChannelId(long channelId) throws Exception
   {
      lock.readLock().acquire();

      try
      {
         //First look in the failed map
         //Failed bindings are stored in the failed map by channel id
         Map channelMap = (Map)failedBindings.get(new Integer(currentNodeId));
         Binding binding = null;
         if (channelMap != null)
         {
            binding = (Binding)channelMap.get(new Long(channelId));
         }

         if (binding == null)
         {
            binding = super.getBindingforChannelId(channelId);
         }
         return binding;
      }
      finally
      {
         lock.readLock().release();
      }
   }

   // PostOfficeInternal implementation ------------------------------------------------------------

   /*
    * Called when another node adds a binding
    */
   public void addBindingFromCluster(int nodeId, String queueName, String conditionText,
                                     String filterString, long channelID, boolean durable,
                                     boolean failed)
      throws Exception
   {
      lock.writeLock().acquire();

      log.debug(this + " adding binding from node " + nodeId + ", queue " + queueName +
         " with condition " + conditionText);

      Condition condition = conditionFactory.createCondition(conditionText);

      try
      {
         //Sanity

         if (!knowAboutNodeId(nodeId))
         {
            throw new IllegalStateException("Don't know about node id: " + nodeId);
         }

         // We currently only allow one binding per name per node
         Map nameMap = (Map)nameMaps.get(new Integer(nodeId));

         Binding binding = null;

         if (nameMap != null)
         {
            binding = (Binding)nameMap.get(queueName);
         }

         if (binding != null && failed)
         {
            throw new IllegalArgumentException(this + " has already this binding for node " +
               nodeId + ", queue " + queueName);
         }

         binding = createBinding(nodeId, condition, queueName, channelID,
                                 filterString, durable, failed, null);

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

      if (trace) { log.trace(this.currentNodeId + " removing binding from node: " + nodeId + " queue: " + queueName); }

      try
      {
         // Sanity
         if (!knowAboutNodeId(nodeId))
         {
            throw new IllegalStateException("Don't know about node id: " + nodeId);
         }

         removeBinding(nodeId, queueName);
      }
      finally
      {
         lock.writeLock().release();
      }
   }

   public void handleNodeLeft(int nodeId) throws Exception
   {
      synchronized (leftSet)
      {
         leftSet.add(new Integer(nodeId));
      }
   }

   /**
    * @param originatorNodeID - the ID of the node that initiated the modification.
    */
   public void putReplicantLocally(int originatorNodeID, Serializable key, Serializable replicant)
      throws Exception
   {

      synchronized (replicatedData)
      {
         log.debug(this + " puts replicant locally: " + key + "->" + replicant);

         Map m = (Map)replicatedData.get(key);

         if (m == null)
         {
            m = new LinkedHashMap();

            replicatedData.put(key, m);
         }

         m.put(new Integer(originatorNodeID), replicant);

         notifyListeners(key, m, true, originatorNodeID);

         if (trace) { log.trace(this + " putReplicantLocally completed"); }
      }
   }

   /**
    * @param originatorNodeID - the ID of the node that initiated the modification.
    */
   public boolean removeReplicantLocally(int originatorNodeID, Serializable key) throws Exception
   {
      synchronized (replicatedData)
      {
         if (trace) { log.trace(this + " removes " + originatorNodeID + "'s replicant locally for key " + key); }

         Map m = (Map)replicatedData.get(key);

         if (m == null)
         {
            return false;
         }

         Object obj = m.remove(new Integer(originatorNodeID));

         if (obj == null)
         {
            return false;
         }

         if (m.isEmpty())
         {
            replicatedData.remove(key);
         }
         notifyListeners(key, m, false, originatorNodeID);

         return true;
      }
   }

   public void routeFromCluster(org.jboss.messaging.core.message.Message message,
                                String routingKeyText,
                                Map queueNameNodeIdMap) throws Exception
   {
      if (trace) { log.trace(this + " routing from cluster " + message + ", routing key " + routingKeyText + ", map " + queueNameNodeIdMap); }

      Condition routingKey = conditionFactory.createCondition(routingKeyText);

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

               if (binding.getNodeID() == this.currentNodeId)
               {
                  boolean handle = true;

                  if (queueNameNodeIdMap != null)
                  {
                     Integer in = (Integer)queueNameNodeIdMap.get(binding.getQueue().getName());

                     //When there are more than one queues with the same name across the cluster we only
                     //want to chose one of them

                     if (in != null)
                     {
                        handle = in.intValue() == currentNodeId;
                     }
                  }

                  if (handle)
                  {
                     //It's a local binding so we pass the message on to the subscription

                     LocalClusteredQueue queue = (LocalClusteredQueue)binding.getQueue();

                     Delivery del = queue.handleFromCluster(ref);

                     if (trace)
                     {
                        log.trace(this.currentNodeId + " queue " + queue.getName() + " handled reference from cluster " + del);
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

   /*
    * Multicast a message to all members of the group
    */
   public void asyncSendRequest(ClusterRequest request) throws Exception
   {
      if (stopping)
      {
         return;
      }
      
      if (trace) { log.trace(this + " sending asynchronously " + request + " to group"); }

      byte[] bytes = writeRequest(request);
      asyncChannel.send(new Message(null, null, bytes));
   }

   /*
    * Unicast a message to one member of the group
    */
   public void asyncSendRequest(ClusterRequest request, int nodeId) throws Exception
   {
      if (stopping)
      {
         return;
      }
      
      Address address = this.getAddressForNodeId(nodeId, false);

      if (address == null)
      {
         throw new IllegalArgumentException("Cannot find address for node " + nodeId);
      }

      if (trace) { log.trace(this + " sending asynchronously " + request + " to node  " + nodeId + "/" + address); }

      byte[] bytes = writeRequest(request);
      asyncChannel.send(new Message(address, null, bytes));
   }

   /*
    * We put the transaction in the holding area
    */
   public void holdTransaction(TransactionId id, ClusterTransaction tx) throws Exception
   {
      synchronized (holdingArea)
      {
         holdingArea.put(id, tx);

         if (trace) { log.trace(this + " added transaction " + tx + " to holding area as " + id); }
      }
   }

   public void commitTransaction(TransactionId id) throws Throwable
   {
      if (trace) { log.trace(currentNodeId + " committing transaction " + id ); }

      ClusterTransaction tx = null;

      synchronized (holdingArea)
      {
         tx = (ClusterTransaction)holdingArea.remove(id);
      }

      if (tx == null)
      {
         //Commit can come in after the node has left - this is ok
         if (trace) { log.trace("Cannot find transaction in map, node may have already left"); }
      }
      else
      {
         tx.commit(this);
   
         if (trace) { log.trace(this + " committed transaction " + id ); }
      }
   }

   public void rollbackTransaction(TransactionId id) throws Throwable
   {
      if (trace) { log.trace(this + " rolling back transaction " + id ); }

      ClusterTransaction tx = null;

      synchronized (holdingArea)
      {
         tx = (ClusterTransaction)holdingArea.remove(id);
      }

      if (tx == null)
      {
         // Rollback can come in after the node has left - this is ok
         if (trace) { log.trace("Cannot find transaction in map, node may have already left"); }
      }
      else
      {
         tx.rollback(this);
   
         if (trace) { log.trace(this + " committed transaction " + id ); }
      }
   }

   public void updateQueueStats(int nodeId, List statsList) throws Exception
   {
      lock.readLock().acquire();

      if (trace) { log.trace(this + " updating queue stats from node " + nodeId + " stats size: " + statsList.size()); }

      try
      {
         if (nodeId == this.currentNodeId)
         {
            // Sanity check
            throw new IllegalStateException("Received stats from node with ID that matches this " +
               "node's ID. You may have started two or more nodes with the same node ID!");
         }

         Map nameMap = (Map)nameMaps.get(new Integer(nodeId));

         if (nameMap == null)
         {
            // This is ok, the node might have left
            if (trace) { log.trace(this + " cannot find node in name map, the node might have left"); }
         }
         else
         {
            for(Iterator i = statsList.iterator(); i.hasNext(); )
            {
               QueueStats st = (QueueStats)i.next();
               Binding bb = (Binding)nameMap.get(st.getQueueName());

               if (bb == null)
               {
                  // I guess this is possible if the queue was unbound
                  if (trace) { log.trace(this + " cannot find binding for queue " + st.getQueueName() + " it could have been unbound"); }
               }
               else
               {
                  RemoteQueueStub stub = (RemoteQueueStub)bb.getQueue();

                  stub.setStats(st);

                  if (trace) { log.trace(this.currentNodeId + " setting stats: " + st + " on remote stub " + stub.getName()); }

                  ClusterRouter router = (ClusterRouter)routerMap.get(st.getQueueName());

                  // Maybe the local queue now wants to pull message(s) from the remote queue given
                  // that the stats for the remote queue have changed
                  LocalClusteredQueue localQueue = (LocalClusteredQueue)router.getLocalQueue();

                  if (localQueue != null)
                  {
                     //TODO - the call to getQueues is too slow since it creates a new list and adds
                     //       the local queue!!!
                     RemoteQueueStub toQueue =
                        (RemoteQueueStub)messagePullPolicy.chooseQueue(router.getQueues());

                     if (trace) { log.trace(this.currentNodeId + " recalculated pull queue for queue " + st.getQueueName() + " to be " + toQueue); }

                     localQueue.setPullQueue(toQueue);

                     if (toQueue != null && localQueue.getRefCount() == 0)
                     {
                        // We now trigger delivery - this may cause a pull event
                        // We only do this if there are no refs in the local queue

                        localQueue.deliver(false);

                        if (trace) { log.trace(this + " triggered delivery for " + localQueue.getName()); }
                     }
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

   public void sendQueueStats() throws Exception
   {
      if (!started)
      {
         return;
      }

      lock.readLock().acquire();

      List statsList = null;

      try
      {
         Map nameMap = (Map)nameMaps.get(new Integer(currentNodeId));

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

                  if (stats != null)
                  {
                     if (statsList == null)
                     {
                        statsList = new ArrayList();
                     }

                     statsList.add(stats);

                     if (trace) { log.trace(this.currentNodeId + " adding stat for send " + stats); }
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
         ClusterRequest req = new QueueStatsRequest(currentNodeId, statsList);

         asyncSendRequest(req);

         if (trace) { log.trace(this.currentNodeId + " Sent stats"); }
      }
   }

   public boolean referenceExistsInStorage(long channelID, long messageID) throws Exception
   {
      return pm.referenceExists(channelID, messageID);
   }

   public void handleMessagePullResult(int remoteNodeId, long holdingTxId,
                                       String queueName, org.jboss.messaging.core.message.Message message) throws Throwable
   {
      if (trace) { log.trace(this.currentNodeId + " handling pull result " + message + " for " + queueName); }

      Binding binding = getBindingForQueueName(queueName);

      //The binding might be null if the queue was unbound

      boolean handled = false;

      if (!failHandleResult && binding != null)
      {
         LocalClusteredQueue localQueue = (LocalClusteredQueue)binding.getQueue();

         RemoteQueueStub remoteQueue = localQueue.getPullQueue();

         if (remoteNodeId != remoteQueue.getNodeId())
         {
            //It might have changed since the request was sent
            Map bindings = (Map)nameMaps.get(new Integer(remoteNodeId));

            if (bindings != null)
            {
               binding = (Binding)bindings.get(queueName);

               if (binding != null)
               {
                 remoteQueue = (RemoteQueueStub)binding.getQueue();
               }
            }
         }

         if (remoteQueue != null)
         {
            localQueue.handlePullMessagesResult(remoteQueue, message, holdingTxId,
                                                failBeforeCommit, failAfterCommit);

            handled = true;
         }
      }

      if (!handled)
      {
         //If we didn't handle it for what ever reason, then we might have to send a rollback
         //message to the other node otherwise the transaction might end up in the holding
         //area for ever
         if (message.isReliable())
         {
            //Only reliable messages will be in holding area
            this.asyncSendRequest(new RollbackPullRequest(this.currentNodeId, holdingTxId), remoteNodeId);

            if (trace) { log.trace(this.currentNodeId + " send rollback pull request"); }
         }
      }
   }

   // Replicator implementation --------------------------------------------------------------------

   public void put(Serializable key, Serializable replicant) throws Exception
   {
      putReplicantLocally(currentNodeId, key, replicant);

      PutReplicantRequest request = new PutReplicantRequest(currentNodeId, key, replicant);

      syncSendRequest(request);
   }

   public Map get(Serializable key) throws Exception
   {
      synchronized (replicatedData)
      {
         Map m = (Map)replicatedData.get(key);

         return m == null ? Collections.EMPTY_MAP : Collections.unmodifiableMap(m);
      }
   }

   public boolean remove(Serializable key) throws Exception
   {
      if (removeReplicantLocally(this.currentNodeId, key))
      {
         RemoveReplicantRequest request = new RemoveReplicantRequest(this.currentNodeId, key);

         syncSendRequest(request);

         return true;
      }
      else
      {
         return false;
      }
   }

   public void registerListener(ReplicationListener listener)
   {
      synchronized (replicationListeners)
      {
         if (replicationListeners.contains(listener))
         {
            throw new IllegalArgumentException("Listener " + listener + " is already registered");
         }
         replicationListeners.add(listener);
      }
   }

   public void unregisterListener(ReplicationListener listener)
   {
      synchronized (replicationListeners)
      {
         boolean removed = replicationListeners.remove(listener);

         if (!removed)
         {
            throw new IllegalArgumentException("Cannot find listener " + listener + " to remove");
         }
      }
   }

   public FailoverMapper getFailoverMapper()
   {
      return failoverMapper;
   }

   // Public ---------------------------------------------------------------------------------------

   public boolean route(MessageReference ref, Condition condition, Transaction tx) throws Exception
   {
      if (trace) { log.trace(this + " routing " + ref + " with condition '" + condition + "'" + (tx == null ? "" : " transactionally in " + tx)); }

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

         int lastNodeId = -1;
         boolean startInternalTx = false;

         if (cb != null)
         {
            if (trace) { log.trace(this + " found " + cb); }

            if (tx == null && ref.getMessage().isReliable())
            {
               if (!(cb.getDurableCount() == 0 ||
                    (cb.getDurableCount() == 1 && cb.getLocalDurableCount() == 1)))
               {
                  // When routing a persistent message without a transaction then we may need to
                  // start an internal transaction in order to route it. This is so we can guarantee
                  // the message is delivered to all or none of the subscriptions. We need to do
                  // this if there is anything other than. No durable subscriptions or exactly one
                  // local durable subscription.

                  startInternalTx = true;

                  if (trace) { log.trace(this + " starting internal transaction since it needs to deliver persistent message to more than one durable sub or remote durable subs"); }
               }
            }

            if (startInternalTx)
            {
               tx = tr.createTransaction();
            }

            int numberRemote = 0;
            long lastChannelId = -1;
            Map queueNameNodeIdMap = null;

            for(Iterator i = cb.getRouters().iterator(); i.hasNext(); )
            {
               ClusterRouter router = (ClusterRouter)i.next();

               if (trace) { log.trace(this + " sending " + ref + " to " + router); }

               Delivery del = router.handle(null, ref, tx);

               if (del != null && del.isSelectorAccepted())
               {
                  routed = true;

                  ClusteredQueue queue = (ClusteredQueue)del.getObserver();

                  if (trace) { log.trace(this + " successfully routed message to " + (queue.isLocal() ? "LOCAL"  : "REMOTE") + " destination '" + queue.getName() + "' on node " + queue.getNodeId()); }

                  if (router.getNumberOfReceivers() > 1)
                  {
                     // We have now chosen which one will receive the message so we need to add this
                     // information to a map which will get sent when casting - so the the queue on
                     // the receiving node knows whether to receive the message.

                     if (queueNameNodeIdMap == null)
                     {
                        queueNameNodeIdMap = new HashMap();
                     }

                     queueNameNodeIdMap.put(queue.getName(), new Integer(queue.getNodeId()));
                  }

                  if (!queue.isLocal())
                  {
                     // We need to send the message remotely, count recipients so we know whether
                     // to unicast or multicast
                     numberRemote++;
                     lastNodeId = queue.getNodeId();
                     lastChannelId = queue.getChannelID();
                  }
               }
            }

            // Now we've sent the message to any local queues, we might also need to send the
            // message to the other office instances on the cluster if there are queues on those
            // nodes that need to receive the message.

            //TODO - there is an innefficiency here, numberRemote does not take into account that
            //       more than one of the number remote may be on the same node, so we could end up
            //       multicasting when unicast would do

            if (numberRemote > 0)
            {
               if (tx == null)
               {
                  if (numberRemote == 1)
                  {
                     if (trace) { log.trace(this + " unicasting message to " + lastNodeId); }

                     // Unicast - only one node is interested in the message
                     asyncSendRequest(new MessageRequest(condition.toText(),
                                                         ref.getMessage(), null), lastNodeId);
                  }
                  else
                  {
                     if (trace) { log.trace(this + " multicasting message to group"); }

                     // Multicast - more than one node is interested
                     asyncSendRequest(new MessageRequest(condition.toText(),
                                                         ref.getMessage(), queueNameNodeIdMap));
                  }
               }
               else
               {
                  CastMessagesCallback callback = (CastMessagesCallback)tx.getCallback(this);

                  if (callback == null)
                  {
                     callback = new CastMessagesCallback(currentNodeId, tx.getId(),
                                                         DefaultClusteredPostOffice.this,
                                                         failBeforeCommit, failAfterCommit);

                     // This callback MUST be executed first

                     // Execution order is as follows:
                     //
                     // Before commit:
                     // 1. Cast messages across network - get added to holding area (if persistent)
                     //    on receiving nodes.
                     // 2. Persist messages in persistent store.
                     //
                     // After commit
                     //
                     // 1. Cast commit message across network.

                     tx.addFirstCallback(callback, this);
                  }

                  callback.addMessage(condition, ref.getMessage(), queueNameNodeIdMap,
                                      numberRemote == 1 ? lastNodeId : -1, lastChannelId);
               }
            }

            if (startInternalTx)
            {
               if (trace) { log.trace(this + " committing " + tx); }
               tx.commit();
               if (trace) { log.trace(this + " committed " + tx); }
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

   /**
    * Check for any transactions that need to be committed or rolled back
    */
   public void checkTransactions(Integer nodeId) throws Throwable
   {
      if (trace) { log.trace(this + " checking for any stranded transactions for node " + nodeId); }

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
               ClusterTransaction tx = (ClusterTransaction)entry.getValue();

               if (trace) { log.trace("found transaction " + tx + " in holding area"); }

               boolean commit = tx.check(this);

               if (trace) { log.trace("transaction " + tx + " will be " + (commit ? "COMMITTED" : "ROLLED BACK")); }

               if (commit)
               {
                  tx.commit(this);
               }
               else
               {
                  tx.rollback(this);
               }

               toRemove.add(id);

               if (trace) { log.trace("resolved " + tx); }
            }
         }

         // Remove the transactions from the holding area

         iter = toRemove.iterator();

         while (iter.hasNext())
         {
            TransactionId id = (TransactionId)iter.next();

            holdingArea.remove(id);
         }
      }
      if (trace) { log.trace(this + " transaction check complete"); }
   }

   public int getNodeId()
   {
      return currentNodeId;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ClusteredPostOffice[");
      sb.append(currentNodeId).append(":").append(getOfficeName()).append(":");

      if (syncChannel == null)
      {
         sb.append("UNINITIALIZED");
      }
      else
      {
         Address addr = syncChannel.getLocalAddress();
         if (addr == null)
         {
            sb.append("UNCONNECTED");
         }
         else
         {
            sb.append(addr);
         }
      }

      sb.append("]");
      return sb.toString();
   }

   public String printBindingInformation()
   {
      StringWriter buffer = new StringWriter();
      PrintWriter out = new PrintWriter(buffer);
      out.print(super.printBindingInformation());

      out.println("<br>FailOver bindings");
      out.println("<table border=1><tr><td>Node</td><td>ChannelID</td><td>Binding</td>");

      for (Iterator iter = this.failedBindings.entrySet().iterator(); iter.hasNext();)
      {
         Map.Entry entry = (Map.Entry)iter.next();

         int count=0;
         Map bindings = (Map)entry.getValue();
         for (Iterator iterValues = bindings.entrySet().iterator();iterValues.hasNext();)
         {
            Map.Entry entry2 = (Map.Entry)iterValues.next();
            if ( count++ == 0 )
            {
               out.print("<tr><td>" + entry.getKey() + "</td>");
            }
            else
            {
               out.print("<tr><td>&nbsp;</td>");
            }
            out.println("<td>" + entry2.getKey() + "</td><td>" + entry2.getValue() + "</td></tr>");
         }
      }

      out.println("</table>");

      out.println("<br>Router Information");

      out.println("<table border=1><tr><td>Queue Route</td><td>Local Queue</td><td>Elements</td></tr>");

      for (Iterator iterRouter = routerMap.entrySet().iterator();iterRouter.hasNext();)
      {
         Map.Entry entry = (Map.Entry)iterRouter.next();
         ClusterRouter router = (ClusterRouter)entry.getValue();
         out.println("<tr><td>" + entry.getKey() + "</td><td>" + router.getLocalQueue() + "</td>");

         out.println("<td>");

         out.println("<table border=1>");

         if (!router.getFailedQueues().isEmpty())
         {
            out.println("<tr><td><b>Failed Over Queues</b></td><</tr>");
            for (Iterator queuesIterator = router.getFailedQueues().iterator();queuesIterator.hasNext();)
            {
               Object queue = queuesIterator.next();
               out.println("<tr><td>" + queue + "</td></tr>");
            }
         }

         out.println("<tr><td><b>Queues</b></td><</tr>");

         for (Iterator queuesIterator = router.getQueues().iterator();queuesIterator.hasNext();)
         {
            Object queueRouted = queuesIterator.next();
            out.println("<tr><td>" + queueRouted + "</td></tr>");
         }

         out.println("</table>");

         out.println("</td></tr>");

      }

      out.println("</table>");

      out.println("Replicator's Information");

      out.println("<table border=1><tr><td>Node</td><td>Key</td><td>Value</td></tr>");

      for (Iterator iter = replicatedData.entrySet().iterator(); iter.hasNext();)
      {
         Map.Entry entry = (Map.Entry) iter.next();
         Map subMap = (Map)entry.getValue();

         for (Iterator subIterator = subMap.entrySet().iterator(); subIterator.hasNext();)
         {
            Map.Entry subValue = (Map.Entry) subIterator.next();
            out.println("<tr><td>" + entry.getKey() + "</td>");
            out.println("<td>" + subValue.getKey() + "</td><td>" + subValue.getValue() + "</td></tr>" );
         }

      }

      out.println("</table>");


      out.println("View Information");

      out.println("<table border=1><tr><td>Members</td></tr>");


      for (Iterator iterMembers = currentView.getMembers().iterator(); iterMembers.hasNext();)
      {
         Address address = (Address) iterMembers.next();
         out.println("<tr><td>" + address + "</td></tr>");
      }

      out.println("</table>");

      return buffer.toString();
   }

   /**
    * MUST ONLY be used for testing!
    */
   public void setFail(boolean beforeCommit, boolean afterCommit, boolean handleResult)
   {
      this.failBeforeCommit = beforeCommit;
      this.failAfterCommit = afterCommit;
      this.failHandleResult = handleResult;
   }

   /**
    * MUST ONLY be used for testing!
    */
   public Collection getHoldingTransactions()
   {
      return holdingArea.values();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void addToNameMap(Binding binding)
   {
      if (!binding.isFailed())
      {
         super.addToNameMap(binding);
      }
      else
      {
         addIntoFailedMaps(binding);
      }
   }

   protected void addToConditionMap(Binding binding)
   {
      Condition condition = binding.getCondition();

      ClusteredBindings bindings = (ClusteredBindings)conditionMap.get(condition);

      if (bindings == null)
      {
         bindings = new DefaultClusteredBindings(currentNodeId);

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

      // todo: Maybe we should have isFailed as a property of Queue instead of Binding, so we won't need to change this signature.
      router.add(binding.getQueue(),binding.isFailed());
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
      if (trace) { log.trace(this + " loading bindings"); }

      boolean isState = syncChannel.getState(null, stateTimeout);

      if (!isState)
      {
         // Must be first member in group or non clustered, we load the state ourself from
         // the database.

         if (trace) { log.trace(this + " is the first member of group, so will load bindings from database"); }
         super.loadBindings();
      }
      else
      {
         // The state will be set in due course via the MessageListener, we must wait until this
         // happens.

         if (trace) { log.trace(this + " not first member of group, so waiting for state to arrive...."); }

         synchronized (setStateLock)
         {
            //TODO we should implement a timeout on this
            while (!stateSet)
            {
               setStateLock.wait();
            }
         }
         if (trace) { log.trace(this + " received state"); }
      }
   }

   protected Binding createBinding(int nodeId, Condition condition, String queueName,
                                   long channelId, String filterString, boolean durable,
                                   boolean failed, Integer failedNodeID) throws Exception
   {
      Filter filter = filterFactory.createFilter(filterString);
      return createBinding(nodeId, condition, queueName, channelId,
                           filter, durable, failed, failedNodeID);
   }

   //FIXME - we should not create queues here
   //Queues should only be created at destination deployment time since it is only then
   //we know all the params
   protected Binding createBinding(int nodeID, Condition condition, String queueName,
                                   long channelId, Filter filter, boolean durable,
                                   boolean failed, Integer failedNodeID)
   {
      Queue queue;

      if (nodeID == currentNodeId)
      {
         QueuedExecutor executor = (QueuedExecutor)pool.get();

         if (failedNodeID == null)
         {
            queue = new LocalClusteredQueue(this, nodeID, queueName, channelId, ms, pm, true,
                                            durable, executor, -1, filter, tr);
         }
         else
         {
            queue = new FailedOverQueue(this, nodeID, queueName, channelId, ms, pm, true,
                                        durable, executor, filter, tr, failedNodeID.intValue());
         }
      }
      else
      {
         queue = new RemoteQueueStub(nodeID, queueName, channelId, durable, pm, filter);
      }

      return new DefaultBinding(nodeID, condition, queue, failed);
   }

   // Private --------------------------------------------------------------------------------------

   private void sendBindRequest(Condition condition, LocalClusteredQueue queue, Binding binding)
      throws Exception
   {
      BindRequest request =
         new BindRequest(this.currentNodeId, queue.getName(), condition.toText(),
                         queue.getFilter() == null ? null : queue.getFilter().getFilterString(),
                         binding.getQueue().getChannelID(), queue.isRecoverable(),
                         binding.isFailed());

      syncSendRequest(request);
   }

   private boolean leaveMessageReceived(Integer nodeId) throws Exception
   {
      synchronized (leftSet)
      {
         return leftSet.remove(nodeId);
      }
   }

   /*
    * Removes all non durable binding data, and any local replicant data for the specified node.
    */
   private void cleanLocalDataForNode(Integer nodeToRemove) throws Exception
   {
      log.debug(this + " cleaning local data for node " + nodeToRemove);

      lock.writeLock().acquire();

      try
      {
         Map nameMap = (Map)nameMaps.get(nodeToRemove);

         if (nameMap != null)
         {
            List toRemove = new ArrayList();

            for(Iterator i = nameMap.values().iterator(); i.hasNext(); )
            {
               Binding binding = (Binding)i.next();

               if (!binding.getQueue().isRecoverable())
               {
                  // We only remove the non durable bindings - we still need to be able to handle
                  // messages for a durable subscription "owned" by a node that is not active any
                  // more!
                  toRemove.add(binding);
               }
            }

            for(Iterator i = toRemove.iterator(); i.hasNext(); )
            {
               Binding binding = (Binding)i.next();
               removeBinding(nodeToRemove.intValue(), binding.getQueue().getName());
            }
         }
      }
      finally
      {
         lock.writeLock().release();
      }

      synchronized (replicatedData)
      {
         // We need to remove any replicant data for the node. This includes the node-address info.
         for(Iterator i = replicatedData.entrySet().iterator(); i.hasNext(); )
         {
            Map.Entry entry = (Map.Entry)i.next();
            String key = (String)entry.getKey();
            Map replicants = (Map)entry.getValue();

            replicants.remove(nodeToRemove);

            if (replicants.isEmpty())
            {
               i.remove();
            }

            notifyListeners(key, replicants, false, nodeToRemove.intValue());
         }
      }
   }

   /**
    * @param updatedReplicantMap - the updated replicant map. It contains ALL current replicants for
    *        the given key.
    */
   private void notifyListeners(Serializable key, Map updatedReplicantMap, boolean added,
                                int originatorNodeId)
   {
      synchronized (replicationListeners)
      {
         for (Iterator i = replicationListeners.iterator(); i.hasNext(); )
         {
            ReplicationListener listener = (ReplicationListener)i.next();
            listener.onReplicationChange(key, updatedReplicantMap, added, originatorNodeId);
         }
      }
   }

   /*
    * Multicast a sync request
    */
   private void syncSendRequest(ClusterRequest request) throws Exception
   {
      if (stopping)
      {
         return;
      }
      
      if (trace) { log.trace(this + " sending synch request " + request); }

      Message message = new Message(null, null, writeRequest(request));

      controlMessageDispatcher.castMessage(null, message, GroupRequest.GET_ALL, castTimeout);

      if (trace) { log.trace(this + " request sent OK"); }
   }

   //TODO - this is a bit tortuous - needs optimising
   private Integer getNodeIDForSyncAddress(Address address) throws Exception
   {
      synchronized (replicatedData)
      {
         Map map = get(ADDRESS_INFO_KEY);

         if (map == null)
         {
            throw new IllegalStateException("Cannot find node id -> address mapping");
         }

         Integer nid = null;

         for(Iterator i = map.entrySet().iterator(); i.hasNext(); )
         {
            Map.Entry entry = (Map.Entry)i.next();
            PostOfficeAddressInfo info = (PostOfficeAddressInfo)entry.getValue();

            if (info.getSyncChannelAddress().equals(address))
            {
               nid = (Integer)entry.getKey();
               break;
            }
         }
         return nid;
      }
   }

   private boolean knowAboutNodeId(int nodeId)
   {
      //The nodeid->Address info mapping is stored in the replicated data

      synchronized (replicatedData)
      {
         Map nodeIdAddressMapping = (Map)replicatedData.get(ADDRESS_INFO_KEY);

         if (nodeIdAddressMapping == null)
         {
            return false;
         }
         else
         {
            Object obj = nodeIdAddressMapping.get(new Integer(nodeId));

            return obj != null;
         }
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

            BindingInfo info = new BindingInfo(binding.getNodeID(), queue.getName(),
                                               binding.getCondition().toText(),
                                               queue.getFilter() == null ? null : queue.getFilter().getFilterString(),
                                               queue.getChannelID(),
                                               queue.isRecoverable(),
                                               binding.isFailed());
            bindings.add(info);
         }
      }

      //Need to copy

      Map copy;

      synchronized (replicatedData)
      {
         copy = copyReplicatedData(replicatedData);
      }

      SharedState state = new SharedState(bindings, copy);

      return StreamUtils.toBytes(state);
   }

   private void processStateBytes(byte[] bytes) throws Exception
   {
      if (trace) { log.trace(this + " received state from group"); }

      SharedState state = new SharedState();

      StreamUtils.fromBytes(state, bytes);

      if (trace) { log.trace(this + " received " + state.getBindings().size() + " bindings and map " + state.getReplicatedData()); }

      nameMaps.clear();
      conditionMap.clear();

      List bindings = state.getBindings();
      Iterator iter = bindings.iterator();

      while (iter.hasNext())
      {
         BindingInfo info = (BindingInfo)iter.next();
         Condition condition = conditionFactory.createCondition(info.getConditionText());
         Binding binding =
            createBinding(info.getNodeId(), condition, info.getQueueName(), info.getChannelId(),
                          info.getFilterString(), info.isDurable(), info.isFailed(), null);

         if (binding.getNodeID() == this.currentNodeId)
         {
            //We deactivate if this is one of our own bindings - it can only
            //be one of our own durable bindings - and since state is retrieved before we are fully started
            //then the sub hasn't been deployed so must be deactivated

            binding.getQueue().deactivate();
         }

         addBinding(binding);
      }

      //Update the replicated data

      synchronized (replicatedData)
      {
         replicatedData = copyReplicatedData(state.getReplicatedData());
      }
   }

   private Map copyReplicatedData(Map toCopy)
   {
      Map copy = new HashMap();

      Iterator iter = toCopy.entrySet().iterator();

      while (iter.hasNext())
      {
         Map.Entry entry = (Map.Entry)iter.next();

         Serializable key = (Serializable)entry.getKey();

         Map replicants = (Map)entry.getValue();

         Map m = new LinkedHashMap();

         m.putAll(replicants);

         copy.put(key, m);
      }

      return copy;
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

   private Address getAddressForNodeId(int nodeId, boolean sync) throws Exception
   {
      synchronized (replicatedData)
      {
         Map map = this.get(ADDRESS_INFO_KEY);

         if (map == null)
         {
            throw new IllegalStateException("Cannot find address mapping");
         }

         PostOfficeAddressInfo info = (PostOfficeAddressInfo)map.get(new Integer(nodeId));

         if (info != null)
         {
            if (sync)
            {
               return info.getSyncChannelAddress();
            }
            else
            {
               return info.getAsyncChannelAddress();
            }
         }
         else
         {
            return null;
         }
      }
   }
   /*
    * A new node has joined the group
    */
   private void nodeJoined(Address address) throws Exception
   {
      log.debug(this + ": " + address + " joined");

      // Currently does nothing
   }

   /*
    * A node has left the group
    */
   private void nodeLeft(Address address) throws Throwable
   {
      log.debug(this + ": " + address + " left");

      Integer leftNodeID = getNodeIDForSyncAddress(address);

      if (leftNodeID == null)
      {
         throw new IllegalStateException(this + " cannot find node ID for address " + address);
      }

      boolean crashed = !this.leaveMessageReceived(leftNodeID);

      log.debug(this + ": node " + leftNodeID + " has " +
         (crashed ? "crashed" : "cleanly left the group"));

      // Cleanup any hanging transactions - we do this irrespective of whether we crashed
      checkTransactions(leftNodeID);

      synchronized (failoverMap)
      {
         // Need to evaluate this before we regenerate the failover map
         Integer failoverNode = (Integer)failoverMap.get(leftNodeID);

         if (failoverNode == null)
         {
            throw new IllegalStateException(this + " cannot find failover node for node " + leftNodeID);
         }

         // Remove any replicant data and non durable bindings for the node - again we need to do
         // this irrespective of whether we crashed. This will notify any listeners which will
         // recalculate the connection factory delegates and failover delegates.

         cleanLocalDataForNode(leftNodeID);

         if (currentNodeId == failoverNode.intValue() && crashed)
         {
            // The node crashed and we are the failover node so let's perform failover

            log.info(this + ": I am the failover node for node " + leftNodeID + " that crashed");

            //TODO server side valve

            performFailover(leftNodeID);
         }
      }
   }

   /**
    * This method fails over all the queues from node <failedNodeId> onto this node. It is triggered
    * when a JGroups view change occurs due to a member leaving and it's determined the member
    * didn't leave cleanly.
    */
   private void performFailover(Integer failedNodeID) throws Exception
   {
      // Need to lock
      lock.writeLock().acquire();

      try
      {
         log.debug(this + " performing failover for failed node " + failedNodeID);

         // We make sure a FailoverStatus object is put in the replicated data for the node. The
         // real failover node will always add this in. This means that each node knows which node
         // has really started the failover for another node, and which node did failover for other
         // nodes in the past.
         // We cannot rely on the failoverMap for this, since that will regenerated once failover is
         // done, because of the change in membership. And clients may failover after that and need
         // to know if they have the correct node. Since this is the first thing we do after
         // detecting failover, it should be very quick that all nodes know, however there is still
         // a chance that a client tries to failover before the information is replicated.

         Map failoverData = (Map)get(FAILED_OVER_FOR_KEY);
         FailoverStatus status = (FailoverStatus)failoverData.get(new Integer(currentNodeId));

         if (status == null)
         {
            status = new FailoverStatus();
         }

         status.startFailingOverForNode(failedNodeID);

         log.debug(this + " announcing the cluster it is starting failover procedure");

         put(FAILED_OVER_FOR_KEY, status);

         log.debug(this + " announced the cluster it is starting failover procedure");

         // Get the map of queues for the failed node

         Map subMaps = (Map)nameMaps.get(failedNodeID);

         if (subMaps == null || subMaps.size() == 0)
         {
            log.warn(this + " couldn't find any binding to fail over from server " + failedNodeID);
         }
         else
         {
            // Compile a list of the queue names to remove. Note that any non durable bindings will
            // already have been removed (in removeDataForNode()) when the node leave was detected,
            // so if there are any non durable bindings left here then this is an error.

            // We iterate through twice to avoid ConcurrentModificationException

            ArrayList namesToRemove = new ArrayList();

            for (Iterator i = subMaps.entrySet().iterator(); i.hasNext();)
            {
               Map.Entry entry = (Map.Entry)i.next();
               Binding binding = (Binding )entry.getValue();

               // Sanity check
               if (!binding.getQueue().isRecoverable())
               {
                  throw new IllegalStateException("Found non recoverable queue " +
                     binding.getQueue().getName() + "in map, these should have been removed!");
               }

               // Sanity check
               if (!binding.getQueue().isClustered())
               {
                  throw new IllegalStateException("Queue " + binding.getQueue().getName() +
                     " is not clustered!");
               }

               ClusteredQueue queue = (ClusteredQueue)binding.getQueue();

               // Sanity check
               if (queue.isLocal())
               {
                  throw new IllegalStateException("Queue " + queue.getName() + " is local!");
               }

               namesToRemove.add(entry);
            }

            if (trace) { log.trace("deleting " + namesToRemove.size() + " bindings from old node"); }

            for (Iterator i = namesToRemove.iterator(); i.hasNext(); )
            {
               Map.Entry entry = (Map.Entry)i.next();
               Binding binding = (Binding)entry.getValue();
               RemoteQueueStub stub = (RemoteQueueStub)binding.getQueue();
               String queueName = (String)entry.getKey();

               // First the binding is removed from the in memory condition and name maps ...
               removeBinding(failedNodeID.intValue(), queueName);

               // ... then deleted from the database
               deleteBinding(failedNodeID.intValue(), queueName);

               log.debug(this + " deleted binding for " + queueName);

               // Note we do not need to send an unbind request across the cluster - this is because
               // when the node crashes a view change will hit the other nodes and that will cause
               // all binding data for that node to be removed anyway.

               // If there is already a queue registered with the same name, then we set a flag
               // "failed" on the binding and then the queue will go into a special list of failed
               // bindings otherwise we treat at as a normal queue.
               // This is because we cannot deal with more than one queue with the same name. Any
               // new consumers will always only connect to queues in the main name map. This may
               // mean that queues in the failed map have messages stranded in them if consumers
               // disconnect (since no more can reconnect). However we message redistribution
               // activated other queues will be able to consume from them.

               //TODO allow message redistribution for queues in the failed list

               boolean failed = internalGetBindingForQueueName(queueName) != null;

               if (!failed)
               {
                  log.debug(this + " did not have a " + queueName +
                     " queue so it's assuming it as a regular queue");
               }
               else
               {
                  log.debug(this + " has already a " + queueName + " queue so adding to failed map");
               }

               // Create a new binding
               Binding newBinding =
                  createBinding(currentNodeId, binding.getCondition(), stub.getName(),
                                stub.getChannelID(), stub.getFilter(), stub.isRecoverable(),
                                failed, failedNodeID);

               // Insert it into the database
               insertBinding(newBinding);

               LocalClusteredQueue clusteredQueue = (LocalClusteredQueue)newBinding.getQueue();

               clusteredQueue.deactivate();
               clusteredQueue.load();
               clusteredQueue.activate();

               log.debug(this + " loaded " + clusteredQueue);

               // Add the new binding in memory
               addBinding(newBinding);

               // Send a bind request so other nodes add it too
               sendBindRequest(binding.getCondition(), clusteredQueue,newBinding);

               //FIXME there is a problem in the above code.
               //If the server crashes between deleting the binding from the database
               //and creating the new binding in the database, then the binding will be completely
               //lost from the database when the server is resurrected.
               //To remedy, both db operations need to be done in the same JBDC tx
            }
         }

         log.debug(this + " finished to fail over destinations");

         //TODO - should this be in a finally? I'm not sure
         status.finishFailingOver();

         log.debug(this + " announcing the cluster that failover procedure is complete");

         put(FAILED_OVER_FOR_KEY, status);

         log.debug(this + " announced the cluster that failover procedure is complete");

         sendJMXNotification(FAILOVER_COMPLETED_NOTIFICATION);

         log.info(this + ": server side fail over is now complete");
      }
      finally
      {
         lock.writeLock().release();
      }
   }

   private void addIntoFailedMaps(Binding binding)
   {
      Map channelMap = (Map)failedBindings.get(new Integer(binding.getNodeID()));

      if (channelMap == null)
      {
         channelMap = new LinkedHashMap();

         failedBindings.put(new Integer(binding.getNodeID()), channelMap);
      }

      channelMap.put(new Long(binding.getQueue().getChannelID()), binding);
   }

   private void sendJMXNotification(String notificationType)
   {
      Notification n = new Notification(notificationType, "", 0l);
      nbSupport.sendNotification(n);
      log.debug(this + " sent " + notificationType + " JMX notification");
   }

   // Inner classes --------------------------------------------------------------------------------

   /*
    * This class is used to manage state on the control channel
    */
   private class ControlMessageListener implements MessageListener
   {
      public byte[] getState()
      {
         if (stopping)
         {
            return null;
         }

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
            if (trace) { log.trace(DefaultClusteredPostOffice.this + ".ControlMessageListener got state"); }
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
         if (stopping)
         {
            return;
         }
      }

      public void setState(byte[] bytes)
      {
         if (stopping)
         {
            return;
         }

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
               if (trace) { log.trace(DefaultClusteredPostOffice.this + ".ControlMessageListener has set state"); }
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
         // NOOP
      }

      public void suspect(Address address)
      {
         // NOOP
      }

      public void viewAccepted(View newView)
      {
         if (stopping)
         {
            return;
         }

         try
         {
            // We queue up changes and execute them asynchronously.
            // This is because JGroups will not let us do stuff like send synch messages using the
            // same thread that delivered the view change and this is what we need to do in
            // failover, for example.

            viewExecutor.execute(new HandleViewAcceptedRunnable(newView));
         }
         catch (InterruptedException e)
         {
            log.warn("Caught InterruptedException", e);
         }
      }

      public byte[] getState()
      {
         // NOOP
         return null;
      }
   }

   private class HandleViewAcceptedRunnable implements Runnable
   {
      private View newView;

      HandleViewAcceptedRunnable(View newView)
      {
         this.newView = newView;
      }

      public void run()
      {
         log.info(DefaultClusteredPostOffice.this  + " got new view " + newView);

         // JGroups will make sure this method is never called by more than one thread concurrently

         View oldView = currentView;
         currentView = newView;

         try
         {
            // Act on membership change, on both cases when an old member left or a new member joined

            if (oldView != null)
            {
               for (Iterator i = oldView.getMembers().iterator(); i.hasNext(); )
               {
                  Address address = (Address)i.next();
                  if (!newView.containsMember(address))
                  {
                     // this is where the failover happens, if necessary
                     nodeLeft(address);
                  }
               }
            }

            for (Iterator i = newView.getMembers().iterator(); i.hasNext(); )
            {
               Address address = (Address)i.next();
               if (oldView == null || !oldView.containsMember(address))
               {
                  nodeJoined(address);
               }
            }

            sendJMXNotification(VIEW_CHANGED_NOTIFICATION);
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

   /*
    * This class is used to listen for messages on the async channel
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
         if (trace) { log.trace(this + " received " + message + " on the ASYNC channel"); }

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
         if (stopping)
         {
            return null;
         }

         if (trace) { log.trace(DefaultClusteredPostOffice.this + ".RequestHandler received " + message + " on the SYNC channel"); }

         try
         {
            byte[] bytes = message.getBuffer();

            ClusterRequest request = readRequest(bytes);

            return request.execute(DefaultClusteredPostOffice.this);
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

   /*
    * We use this class to respond to node address mappings being added or removed from the cluster
    * and then recalculate the node->failover node mapping.
    */
   private class NodeAddressMapListener implements ReplicationListener
   {
      public void onReplicationChange(Serializable key, Map updatedReplicantMap,
                                      boolean added, int originatorNodeID)
      {
         log.debug(DefaultClusteredPostOffice.this + " received " + key +
            " replication change from node " + originatorNodeID + ", new map " + updatedReplicantMap);

         if (key instanceof String && ((String)key).equals(ADDRESS_INFO_KEY))
         {
            log.debug("Updated cluster map:\n" + dumpClusterMap(updatedReplicantMap));

               // A node-address mapping has been added/removed from global state, we need to update
               // the failover map.
               failoverMap = failoverMapper.generateMapping(updatedReplicantMap.keySet());
               log.debug("Updated failover map:\n" + dumpFailoverMap(failoverMap));
            }
         }
      }
}