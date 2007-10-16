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
package org.jboss.messaging.core.impl.postoffice;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.client.container.JMSClientVMIdentifier;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.ChannelFactory;
import org.jboss.messaging.core.contract.ClusterNotification;
import org.jboss.messaging.core.contract.ClusterNotifier;
import org.jboss.messaging.core.contract.Condition;
import org.jboss.messaging.core.contract.ConditionFactory;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.Filter;
import org.jboss.messaging.core.contract.FilterFactory;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.MessagingComponent;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.contract.Replicator;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.JDBCSupport;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.messaging.core.impl.tx.TxCallback;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.StreamUtils;
import org.jgroups.Address;
import org.jgroups.View;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;
import EDU.oswego.cs.dl.util.concurrent.ReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.ReentrantWriterPreferenceReadWriteLock;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision: 2782 $</tt>
 *
 * $Id: DefaultClusteredPostOffice.java 2782 2007-06-14 12:16:17Z timfox $
 *
 */
public class MessagingPostOffice extends JDBCSupport
   implements PostOffice, RequestTarget, GroupListener, Replicator
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(MessagingPostOffice.class);

   //This are only used in testing
   
   public static final String VIEW_CHANGED_NOTIFICATION = "VIEW_CHANGED";
   
   public static final String FAILOVER_COMPLETED_NOTIFICATION = "FAILOVER_COMPLETED";
   
   //End only used in testing

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

   // End of failure testing attributes

   private boolean trace = log.isTraceEnabled();
      
   private MessageStore ms;
   
   private PersistenceManager pm;
   
   private TransactionRepository tr;
   
   private FilterFactory filterFactory;
   
   private ConditionFactory conditionFactory;
   
   private int thisNodeID;

   // Map <Condition, List <Queue>> - for ALL nodes
   private Map mappings;
   
   // Map <node, Map < queue name, binding> >
   private Map nameMaps;
   
   //We cache a reference to the local name map for fast lookup
   private Map localNameMap;
   
   // Map <channel id, Binding> - only for the current node
   private Map channelIDMap;

   private ReadWriteLock lock;
   
   private String officeName;
   
   private boolean clustered;
   
   //Started still needs to be volatile since the ReadWriteLock won't synchronize between threads
   private volatile boolean started;
      
   private GroupMember groupMember;

   private Map replicatedData;

   // Map <Integer(nodeID)->Integer(failoverNodeID)>
   private Map failoverMap;

   private Set leftSet;

   private NotificationBroadcasterSupport nbSupport;
   
   private IDManager channelIDManager;
   
   private ClusterNotifier clusterNotifier;
   
   // Map <node id, PostOfficeAddressInfo>
   private Map nodeIDAddressMap;
   
   private Object waitForBindUnbindLock;   
   
   private Map loadedBindings;

   private boolean supportsFailover = true;
   
   //TODO - this does not belong here - it is only here so we can handle the session replication stuff
   //we should abstract out the group member stuff and an abstract request handle message framework so any component can
   //use it
   private ServerPeer serverPeer;
   
   //Note this MUST be a queued executor to ensure replicate repsonses arrive back in order
   private QueuedExecutor replyExecutor;
   
   private volatile int failoverNodeID = -1;
   
   private volatile boolean firstNode;
   
   //We keep use a semaphore to limit the number of concurrent replication requests to avoid
   //overwhelming JGroups
   private Semaphore replicateSemaphore;
      
   // Constructors ---------------------------------------------------------------------------------

   /*
    * Constructor for a non clustered post office
    */
   public MessagingPostOffice(DataSource ds,
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
                              IDManager channelIDManager,
                              ClusterNotifier clusterNotifier)
      throws Exception
   {
   	super (ds, tm, sqlProperties, createTablesOnStartup);

      this.thisNodeID = nodeId;
      
      this.ms = ms;
      
      this.pm = pm;
      
      this.tr = tr;
      
      this.filterFactory = filterFactory;
      
      this.conditionFactory = conditionFactory;
      
      this.officeName = officeName;
      
      this.clustered = false;
      
      this.channelIDManager = channelIDManager;
      
      this.clusterNotifier = clusterNotifier;

      lock = new ReentrantWriterPreferenceReadWriteLock();
       
      waitForBindUnbindLock = new Object();
   }
   
   /*
    * Constructor for a clustered post office
    */
   public MessagingPostOffice(DataSource ds,
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
                              IDManager channelIDManager,
                              ClusterNotifier clusterNotifier,
                              String groupName,
                              ChannelFactory jChannelFactory,
                              long stateTimeout, long castTimeout,
                              boolean supportsFailover,
                              int maxConcurrentReplications)
      throws Exception
   {
   	this(ds, tm, sqlProperties, createTablesOnStartup, nodeId, officeName, ms, pm, tr,
   		  filterFactory, conditionFactory, channelIDManager, clusterNotifier);
     
      this.clustered = true;
      
      groupMember = new GroupMember(groupName, stateTimeout, castTimeout, jChannelFactory, this, this);

      this.supportsFailover = supportsFailover;
      
      nbSupport = new NotificationBroadcasterSupport();
      
      replicateSemaphore = new Semaphore(maxConcurrentReplications, true);
   }
      
   // MessagingComponent overrides -----------------------------------------------------------------
   
   public MessagingComponent getInstance()
   {
   	return this;
   }

   public void start() throws Exception
   {
   	if (started)
   	{
   		log.warn(this + " is already started");
   		return;
   	}
   	
      log.debug(this + " starting");
      
      super.start();
      
      init();
      
      loadedBindings = getBindingsFromStorage();
      
      if (clustered)
      {
	      groupMember.start();

	      //Sanity check - we check there aren't any other nodes already in the cluster with the same node id
	      if (knowAboutNodeId(thisNodeID))
	      {
	      	throw new IllegalArgumentException("Cannot start post office since there is already a post office in the " +
	      			"cluster with the same node id (" + thisNodeID + "). " +
	      			"Are you sure you have given each node a unique node id during installation?");
	      }
	
	      PostOfficeAddressInfo info = new PostOfficeAddressInfo(groupMember.getSyncAddress(), groupMember.getAsyncAddress());
	      
	      nodeIDAddressMap.put(new Integer(thisNodeID), info);	     
	      
	      //calculate the failover map
	      calculateFailoverMap();
	      
	      //add our vm identifier to the replicator
	      put(Replicator.JVM_ID_KEY, JMSClientVMIdentifier.instance);
	      
	      groupMember.multicastControl(new JoinClusterRequest(thisNodeID, info), true);
	      
	      checkStartReaper();
      }
      else
      {
      	pm.startReaper();
      }
   
      //Now load the bindings for this node
      
      loadBindings();  
      
      started = true;

      log.debug(this + " started");      
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
   	{
   		log.warn(this + " is not started");
   		
   		return;
   	}
   	
      if (trace) { log.trace(this + " stopping"); }
            
      super.stop();      
      
      if (clustered)
      {	       
	      //Need to send this *before* stopping
      	groupMember.multicastControl(new LeaveClusterRequest(thisNodeID), true);
	
	      groupMember.stop();
      }
      
      deInit();
      
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
   
   // PostOffice implementation -------------------------------------------------------------------------
   
   public String getOfficeName()
   {
   	return officeName;
   }
    
   
   public boolean addBinding(Binding binding, boolean allNodes) throws Exception
   {
   	if (allNodes && !binding.queue.isClustered())
   	{
   		throw new IllegalArgumentException("Cannot bind a non clustered queue on all nodes");
   	}
   		
   	boolean added = internalAddBinding(binding, allNodes, true);
   	
   	if (added && allNodes && clustered && binding.queue.isClustered())
   	{
	   	//Now we must wait for all the bindings to appear in state
	   	//This is necessary since the second bind in an all bind is sent asynchronously to avoid deadlock
   		
   		waitForBindUnbind(binding.queue.getName(), true);
   	}
   	
   	if (added)
   	{
   		requestDeliveries(binding.queue);
   	}
   	
   	return added;
   }
   
   
          
   public Binding removeBinding(String queueName, boolean allNodes) throws Throwable
   {
   	Binding binding = internalRemoveBinding(queueName, allNodes, true);
   	
   	if (binding != null && allNodes && clustered && binding.queue.isClustered())
   	{
	   	//Now we must wait for all the bindings to be removed from state
	   	//This is necessary since the second unbind in an all unbind is sent asynchronously to avoid deadlock
   	
   		waitForBindUnbind(queueName, false);
   	}
   	
   	return binding;
   }      
                 
   public boolean route(MessageReference ref, Condition condition, Transaction tx) throws Exception
   {
      if (ref == null)
      {
         throw new IllegalArgumentException("Message reference is null");
      }

      if (condition == null)
      {
         throw new IllegalArgumentException("Condition is null");
      }
      
      return routeInternal(ref, condition, tx, false, null);
   }

   public Collection getQueuesForCondition(Condition condition, boolean localOnly) throws Exception
   {
   	if (condition == null)
   	{
   		throw new IllegalArgumentException("Condition is null");
   	}
   	
   	if (!localOnly && !clustered)
   	{
   		throw new IllegalArgumentException("Cannot request clustered queues on non clustered post office");
   	}

   	lock.readLock().acquire();

   	try
   	{
   		//We should only list the bindings for the local node

   		List queues = (List)mappings.get(condition);

   		if (queues == null)
   		{
   			return Collections.EMPTY_LIST;
   		}
   		else
   		{
   			List list = new ArrayList();

   			Iterator iter = queues.iterator();

   			while (iter.hasNext())
   			{
   				Queue queue = (Queue)iter.next();

   				if (!localOnly || (queue.getNodeID() == thisNodeID))
   				{
   					list.add(queue);
   				}
   			}

   			return list;
   		}
   	}
   	finally
   	{
   		lock.readLock().release();
   	}
   }
   
   public Binding getBindingForQueueName(String queueName) throws Exception
   {
   	if (queueName == null)
   	{
   		throw new IllegalArgumentException("Queue name is null");
   	}
   	
   	lock.readLock().acquire();

   	try
   	{
   		if (localNameMap != null)
   		{
   			Binding binding = (Binding)localNameMap.get(queueName);
      		
      		return binding;
   		}
   		else
   		{
   			return null;
   		}   		   		
   	}
   	finally
   	{
   		lock.readLock().release();
   	}
   }
   
   public Binding getBindingForChannelID(long channelID) throws Exception
   {
   	lock.readLock().acquire();

   	try
   	{
   		Binding binding = (Binding)channelIDMap.get(new Long(channelID));
   		
   		return binding;
   	}
   	finally
   	{
   		lock.readLock().release();
   	}
   }
              
   public boolean isClustered()
   {
      return clustered;
   }
   
   public Map getFailoverMap()
   {
   	synchronized (failoverMap)
   	{
	   	Map map = new HashMap(failoverMap);
	   	
	   	return map;
   	}
   }
   
   
   public Collection getAllBindingsForQueueName(String queueName) throws Exception
   {
   	return getBindings(queueName);
   }
      
   public Collection getAllBindings() throws Exception
   {
   	return getBindings(null);
   }
   
   public Set nodeIDView()
   {
   	return new HashSet(nodeIDAddressMap.keySet());
   }
   
   //TODO - these don't belong here
       
   public void sendReplicateDeliveryMessage(String queueName, String sessionID, long messageID, long deliveryID,
   		                                   boolean reply, boolean sync)
   	throws Exception
   {
   	//We use a semaphore to limit the number of outstanding replicates we can send without getting a response
   	//This is to prevent overwhelming JGroups
   	//See http://jira.jboss.com/jira/browse/JBMESSAGING-1112
   	
   	replicateSemaphore.acquire();
   	
   	try
   	{	   	   	
	   	//There is no need to lock this while failover node change is occuring since the receiving node is tolerant to duplicate
			//adds or acks
	   	   	   		   
	   	Address replyAddress = null;
	   	
	   	if (reply)
	   	{
	   		//TODO optimise this
	   		
	   		PostOfficeAddressInfo info = (PostOfficeAddressInfo)nodeIDAddressMap.get(new Integer(thisNodeID));
	   		
	   		replyAddress = info.getDataChannelAddress();
	   	}
	   	
	   	ClusterRequest request = new ReplicateDeliveryMessage(thisNodeID, queueName, sessionID, messageID, deliveryID, replyAddress);
	   	
	   	if (trace) { log.trace(this + " sending replicate delivery message " + queueName + " " + sessionID + " " + messageID); }
				   
	   	//TODO could be optimised too
		   Address address = getFailoverNodeDataChannelAddress();
		   	
		   if (address != null)
		   {	   
		   	groupMember.unicastData(request, address);
		   }
   	}
   	catch (Exception e)
   	{
   		replicateSemaphore.release();
   		
   		throw e;
   	}
   }

	public void sendReplicateAckMessage(String queueName, long messageID) throws Exception
	{
		//There is no need to lock this while failover node change is occuring since the receiving node is tolerant to duplicate
		//adds or acks
	
	   ClusterRequest request = new ReplicateAckMessage(thisNodeID, queueName, messageID);		   
   	
	   Address address = getFailoverNodeDataChannelAddress();
	   	
	   if (address != null)
	   {	   
	   	groupMember.unicastData(request, address);
	   }
	}
	
	public void injectServerPeer(ServerPeer serverPeer)
	{
		this.serverPeer = serverPeer;
	}
	
	public boolean isFirstNode()
	{
		return firstNode;
	}
	
	
	//	Testing only
   
   public Map getRecoveryArea(String queueName)
   {
   	Binding binding = (Binding)localNameMap.get(queueName);
   	
   	if (binding != null)
   	{
   		return binding.queue.getRecoveryArea();
   	}
   	else
   	{
   		return null;
   	}
   }
   
   public int getRecoveryMapSize(String queueName)
   {
   	Binding binding = (Binding)localNameMap.get(queueName);
   	
   	if (binding != null)
   	{
   		return binding.queue.getRecoveryMapSize();
   	}
   	else
   	{
   		return 0;
   	}
   }
   
   //End testing only
   
   // GroupListener implementation -------------------------------------------------------------
 
   public void setState(byte[] bytes) throws Exception
   {
      if (trace) { log.trace(this + " received state from group"); }

      SharedState state = new SharedState();

      StreamUtils.fromBytes(state, bytes);

      if (trace) { log.trace(this + " received " + state.getMappings().size() + " bindings and map " + state.getReplicatedData()); }

      //No need to lock since only called when starting
      
      mappings.clear();

      List mappings = state.getMappings();
      
      Iterator iter = mappings.iterator();

      while (iter.hasNext())
      {
         MappingInfo mapping = (MappingInfo)iter.next();

         Filter filter = null;
         
         if (mapping.getFilterString() != null)
         {
         	filter = filterFactory.createFilter(mapping.getFilterString());
         }
         
         Queue queue = new MessagingQueue(mapping.getNodeId(), mapping.getQueueName(), mapping.getChannelId(),
                                          mapping.isRecoverable(), filter, true);
         
         Condition condition = conditionFactory.createCondition(mapping.getConditionText());
         
         addBindingInMemory(new Binding(condition, queue, false));
         
         if (mapping.isAllNodes())
         {
         	// insert into db if not already there
         	if (!loadedBindings.containsKey(queue.getName()))
         	{
	         	//Create a local binding too
	         	
	      		long channelID = channelIDManager.getID();
	      			      		
	      		Queue queue2 = new MessagingQueue(thisNodeID, mapping.getQueueName(), channelID, ms, pm,
	   			                                  mapping.isRecoverable(), mapping.getMaxSize(), filter,
	   			                                  mapping.getFullSize(), mapping.getPageSize(), mapping.getDownCacheSize(),
	   			                                  true, mapping.getRecoverDeliveriesTimeout());     
	      		
	      		Binding localBinding = new Binding(condition, queue2, true);
	      			         	
	         	if (mapping.isRecoverable())
	         	{		         	
	         		//We need to insert it into the database
	         		if (trace) { log.trace(this + " got all binding in state for queue " + queue.getName() + " inserting it in DB"); }
	         		
	         		insertBindingInStorage(condition, queue2, true);	         			         				         	
	         	}
	         	
	         	//	Add it to the loaded map
	         	
	            loadedBindings.put(mapping.getQueueName(), localBinding);	         	
         	}  	
         }
      }

      //Update the replicated data

      synchronized (replicatedData)
      {
         replicatedData = copyReplicatedData(state.getReplicatedData());
      }
          
      nodeIDAddressMap = new HashMap(state.getNodeIDAddressMap());      
   }
      
   public byte[] getState() throws Exception
   {
      List list = new ArrayList();
      
      lock.readLock().acquire();
      
      try
      {
      	Iterator iter = nameMaps.values().iterator();
      	
      	while (iter.hasNext())
      	{
      		Map map = (Map)iter.next();
      		
      		Iterator iter2 = map.values().iterator();
      		
      		while (iter2.hasNext())
      		{
      			Binding binding = (Binding)iter2.next();
      		
	      		Queue queue = binding.queue;
	      		
	      		//We only get the clustered queues
	      		if (queue.isClustered())
	      		{		      		
		      		String filterString = queue.getFilter() == null ? null : queue.getFilter().getFilterString();
		      		
		      		MappingInfo mapping;
		      		
		      		if (binding.allNodes)
		      		{
		      			mapping = new MappingInfo(queue.getNodeID(), queue.getName(), binding.condition.toText(), filterString,
		         		                          queue.getChannelID(), queue.isRecoverable(), true, true,
									         		     queue.getFullSize(), queue.getPageSize(), queue.getDownCacheSize(),
									         		     queue.getMaxSize(), queue.getRecoverDeliveriesTimeout());
		      		}
		      		else
		      		{
			      		mapping = new MappingInfo(queue.getNodeID(), queue.getName(), binding.condition.toText(),
			      				                    filterString, queue.getChannelID(), queue.isRecoverable(),
			      				                    true, false);		 
		      		}
		      		list.add(mapping);
	      		}
	      	}
	      }     
      }
      finally
      {
      	lock.readLock().release();
      }

      //Need to copy

      Map copy;

      synchronized (replicatedData)
      {
         copy = copyReplicatedData(replicatedData);
      }

      SharedState state = new SharedState(list, copy, new ConcurrentHashMap(nodeIDAddressMap));

      return StreamUtils.toBytes(state);
   }
   
   /*
    * A new node has joined the group
    */
   public void nodeJoined(Address address) throws Exception
   {
      log.debug(this + ": " + address + " joined");      
   }
   
   private void checkStartReaper()
   {
   	if (groupMember.getCurrentView().size() == 1)
   	{
   		//We are the only member in the group - start the message reaper
   		
   		pm.startReaper();
   	}
   }
   
   public void nodesLeft(List addresses) throws Throwable
   {
   	if (trace) { log.trace("Nodes left " + addresses.size()); }
   	
   	checkStartReaper();
   	
   	Map oldFailoverMap = new HashMap(this.failoverMap);
   	
   	int oldFailoverNodeID = failoverNodeID;
   	
      if (trace) { log.trace("Old failover node id: " + oldFailoverNodeID); }      
   	
   	calculateFailoverMap();
   	
      if (trace) { log.trace("First node is now " + firstNode); }
            
   	Iterator iter = addresses.iterator();
   	
   	while (iter.hasNext())
   	{
   		Address address = (Address)iter.next();

         log.debug(this + ": " + address + " left");

	      Integer leftNodeID = getNodeIDForSyncAddress(address);
	
	      if (leftNodeID == null)
	      {
	         throw new IllegalStateException(this + " cannot find node ID for address " + address);
	      }
	
	      boolean crashed = !leaveMessageReceived(leftNodeID);
	
	      log.debug(this + ": node " + leftNodeID + " has " + (crashed ? "crashed" : "cleanly left the group"));
      
	      Integer fnodeID = (Integer)oldFailoverMap.get(leftNodeID);
      
         log.debug(this + " the failover node for the crashed node is " + fnodeID);
	         
	      boolean doneFailover = false;
	      
	      ClusterNotification notification = new ClusterNotification(ClusterNotification.TYPE_NODE_LEAVE, leftNodeID.intValue(), null);
	      
	      clusterNotifier.sendNotification(notification);
      
	      if (crashed && isSupportsFailover())
	      {	      
		      if (fnodeID == null)
		      {
		      	throw new IllegalStateException("Cannot find failover node for node " + leftNodeID);
		      }
		      
		      if (fnodeID.intValue() == thisNodeID)
		      {
		         // The node crashed and we are the failover node so let's perform failover
		
		         log.debug(this + ": I am the failover node for node " + leftNodeID + " that crashed");
		
		         performFailover(leftNodeID);
		         
		         doneFailover = true;
		      }
	      }
      
	      if (!doneFailover)
	      {
		      // Remove any replicant data and non durable bindings for the node -  This will notify any listeners which will
		      // recalculate the connection factory delegates and failover delegates.
		
		      cleanDataForNode(leftNodeID);
	      }
      
	      if (trace) {log.trace("First node: " + firstNode + " oldFailoverNodeID: " + oldFailoverNodeID + " failoverNodeID: " + failoverNodeID); }
	      
	      if (oldFailoverNodeID != failoverNodeID)
	      {
	      	//Failover node for this node has changed
	      	
	      	failoverNodeChanged(oldFailoverNodeID, firstNode, false);      	
	      }
   	}
	      
	   sendJMXNotification(VIEW_CHANGED_NOTIFICATION);
   }
   
   // RequestTarget implementation ------------------------------------------------------------
   
   /*
    * Called when another node adds a binding
    */
   public void addBindingFromCluster(MappingInfo mapping, boolean allNodes) throws Exception
   {
      log.debug(this + " adding binding from node " + mapping.getNodeId() + ", queue " + mapping.getQueueName() +
                " with condition " + mapping.getConditionText() + " all nodes " + allNodes);
      
      //Sanity
   	
      if (!knowAboutNodeId(mapping.getNodeId()))
      {
         throw new IllegalStateException("Don't know about node id: " + mapping.getNodeId());
      }
      
   	//Create a mapping corresponding to the remote queue
   	Filter filter = null;
      
      if (mapping.getFilterString() != null)
      {
      	filter = filterFactory.createFilter(mapping.getFilterString());
      }
      
      Queue queue = new MessagingQueue(mapping.getNodeId(), mapping.getQueueName(), mapping.getChannelId(),
                                       mapping.isRecoverable(), filter, mapping.isClustered());
      
      Condition condition = conditionFactory.createCondition(mapping.getConditionText());
      
      //addBindingInMemory(new Binding(condition, queue, mapping.isAllNodes()));
      addBindingInMemory(new Binding(condition, queue, false));
      
      if (allNodes)
   	{
   		if (trace) { log.trace("allNodes is true, so also forcing a local bind"); }
   		
   		//There is the possibility that two nodes send a bind all with the same name simultaneously OR
   		//a node starts and sends a bind "ALL" and the other nodes already have a queue with that name
   		//This is ok - but we must check for this and not create the local binding in this case
   					   				   	
   		//Bind locally

   		long channelID = channelIDManager.getID();
   		
   		Queue queue2 = new MessagingQueue(thisNodeID, mapping.getQueueName(), channelID, ms, pm,
			                                  mapping.isRecoverable(), mapping.getMaxSize(), filter,
			                                  mapping.getFullSize(), mapping.getPageSize(), mapping.getDownCacheSize(), true,
			                                  mapping.getRecoverDeliveriesTimeout());

   		//We must cast back asynchronously to avoid deadlock
   		boolean added = internalAddBinding(new Binding(condition, queue2, true), false, false);
   		
   		if (added)
   		{	   		
	   		if (trace) { log.trace(this + " inserted in binding locally"); }			
	   		
	   		queue2.load();
		   		
		      queue2.activate();	   		
   		}
   	}
      
      synchronized (waitForBindUnbindLock)
      {
      	if (trace) { log.trace(this + " notifying bind unbind lock"); }
      	waitForBindUnbindLock.notifyAll();
      }
   }
 
   /*
    * Called when another node removes a binding
    */
   public void removeBindingFromCluster(MappingInfo mapping, boolean allNodes) throws Throwable
   {
      log.debug(this + " removing binding from node " + mapping.getNodeId() + ", queue " + mapping.getQueueName() +
                " with condition " + mapping.getConditionText());
      // Sanity
      if (!knowAboutNodeId(mapping.getNodeId()))
      {
      	throw new IllegalStateException("Don't know about node id: " + mapping.getNodeId());
      }

      removeBindingInMemory(mapping.getNodeId(), mapping.getQueueName());      
      
      synchronized (waitForBindUnbindLock)
      {
      	if (trace) { log.trace(this + " notifying bind unbind lock"); }
      	waitForBindUnbindLock.notifyAll();
      }
      
      if (allNodes)
      {
      	//Also unbind locally
      	
   		if (trace) { log.trace("allNodes is true, so also forcing a local unbind"); }
   		         	
   		// We must cast back asynchronously to avoid deadlock
   		internalRemoveBinding(mapping.getQueueName(), false, false);
      }
   }

   public void handleNodeLeft(int nodeId) throws Exception
   {
   	//No need to remove the nodeid-address map info, this will be removed when data cleaned for node
   	
      leftSet.add(new Integer(nodeId));      
      
      //We don't update the failover map here since this doesn't get called if the node crashed
   }
   
   public void handleNodeJoined(int nodeId, PostOfficeAddressInfo info) throws Exception
   {	   	   	   	
   	nodeIDAddressMap.put(new Integer(nodeId), info);
   	
   	log.debug(this + " handleNodeJoined: " + nodeId + " size: " + nodeIDAddressMap.size());
   	   
   	final int oldFailoverNodeID = this.failoverNodeID;
   	
   	boolean wasFirstNode = this.firstNode;
   	
   	calculateFailoverMap();
   	
   	//Note - when a node joins, we DO NOT send it replicated data - this is because it won't have deployed it's queues
   	//the data is requested by the new node when it deploys its queues      
   	
   	if (!wasFirstNode && oldFailoverNodeID != this.failoverNodeID)
   	{
   		//Need to execute this on it's own thread since it uses the MessageDispatcher
   		
   		new Thread(
	   		new Runnable() { 
	   			public void run()
	   			{
	   				try
	   				{
	   					failoverNodeChanged(oldFailoverNodeID, firstNode, true);
	   				}
	   				catch (Exception e)
	   				{
	   					log.error("Failed to process failover node changed", e);
	   				}
	   			}
	   		}).start();   		   		
   	}
   	
      // Send a notification
      
      ClusterNotification notification = new ClusterNotification(ClusterNotification.TYPE_NODE_JOIN, nodeId, null);
      
      clusterNotifier.sendNotification(notification);
      
      sendJMXNotification(VIEW_CHANGED_NOTIFICATION);
   }

   /**
    * @param originatorNodeID - the ID of the node that initiated the modification.
    */
   public void putReplicantLocally(int originatorNodeID, Serializable key, Serializable replicant) throws Exception
   {
      Map m = null;
      
      synchronized (replicatedData)
      {
         log.debug(this + " puts replicant locally: " + key + "->" + replicant);

         m = (Map)replicatedData.get(key);

         if (m == null)
         {
            m = new LinkedHashMap();

            replicatedData.put(key, m);
         }

         m.put(new Integer(originatorNodeID), replicant);

         if (trace) { log.trace(this + " putReplicantLocally completed"); }
      }
      
      ClusterNotification notification = new ClusterNotification(ClusterNotification.TYPE_REPLICATOR_PUT, originatorNodeID, key);
      
      clusterNotifier.sendNotification(notification);
   }

   /**
    * @param originatorNodeID - the ID of the node that initiated the modification.
    */
   public boolean removeReplicantLocally(int originatorNodeID, Serializable key) throws Exception
   {
      Map m = null;
      
      synchronized (replicatedData)
      {
         if (trace) { log.trace(this + " removes " + originatorNodeID + "'s replicant locally for key " + key); }

         m = (Map)replicatedData.get(key);

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
      }
      
      ClusterNotification notification = new ClusterNotification(ClusterNotification.TYPE_REPLICATOR_REMOVE, originatorNodeID, key);
      
      clusterNotifier.sendNotification(notification);
      
      return true;
   }

   public void routeFromCluster(Message message, String routingKeyText, Set queueNames) throws Exception
   {
      if (trace) { log.trace(this + " routing from cluster " + message + ", routing key " + routingKeyText + ", queue names " + queueNames); }

      Condition routingKey = conditionFactory.createCondition(routingKeyText);

      MessageReference ref = message.createReference();
         
      routeInternal(ref, routingKey, null, true, queueNames);        
   }
      
   //TODO - these do not belong here
   
   public void handleReplicateDelivery(int nodeID, String queueName, String sessionID, long messageID,
   		                              long deliveryID, final Address replyAddress) throws Exception
   {
   	if (trace) { log.trace(this + " handleReplicateDelivery for queue " + queueName + " session " + sessionID + " message " + messageID); }
   	
   	Binding binding = getBindingForQueueName(queueName);
   	
   	if (binding == null)
   	{
   		throw new IllegalStateException("Cannot find queue with name " + queueName +" has it been deployed?");
   	}
   	
   	Queue queue = binding.queue;
   	
   	queue.addToRecoveryArea(nodeID, messageID, sessionID);   	
   	
   	if (trace) { log.trace(this + " reply address is " + replyAddress); }
   	
   	if (replyAddress != null)
   	{   	
	   	//Now we send back a response saying we have added it ok
   		
   		if (trace) { log.trace("Sending back response"); }
	   	
	   	final ClusterRequest request = new ReplicateDeliveryAckMessage(sessionID, deliveryID);
	   		   		   	
	   	//need to execute this on another thread
	   	
	   	replyExecutor.execute(
		   	new Runnable()
		   	{
		   		public void run()
		   		{
		   			try
		   			{
		   				groupMember.unicastData(request, replyAddress);
		   			}
		   			catch (Exception e)
		   			{
		   				log.error("Failed to cast message", e);
		   			}
		   		}
		   	});	   		   	   
   	}
   } 

   public void handleGetReplicatedDeliveries(String queueName, Address returnAddress) throws Exception
   {
   	if (trace) { log.trace(this + " handleGetReplicateDelivery for queue " + queueName); }

   	Binding binding = getBindingForQueueName(queueName);

   	if (binding == null)
   	{
   		//This is ok -the queue might have been undeployed since we thought it was deployed and sent the request
   		
   		if (trace) { log.trace("Binding has not been deployed"); }
   	}
   	else
   	{
	   	//Needs to be executed on a different thread
	  
	   	replyExecutor.execute(new SendReplicatedDeliveriesRunnable(queueName, returnAddress));
   	}
   }          
   
   public void handleReplicateAck(int nodeID, String queueName, long messageID) throws Exception
   {
      Binding binding = getBindingForQueueName(queueName);
   	
   	if (binding == null)
   	{
   		throw new IllegalStateException("Cannot find queue with name " + queueName +" has it been deployed?");
   	}
   	
   	Queue queue = binding.queue;
   	
   	queue.removeFromRecoveryArea(nodeID, messageID);  
   }
   
   
   public void handleReplicateDeliveryAck(String sessionID, long deliveryID) throws Exception
   {
   	if (trace) { log.trace(this + " handleReplicateDeliveryAck " + sessionID + " " + deliveryID); }
   	  	
   	//TODO - this does not belong here
   	ServerSessionEndpoint session = serverPeer.getSession(sessionID);
   	
   	replicateSemaphore.release();
   	
   	if (session == null)
   	{
   		log.warn("Cannot find session " + sessionID);
   		
   		return;
   	}   	   	
   	
   	session.replicateDeliveryResponseReceived(deliveryID);
   }
   
   public void handleAckAllReplicatedDeliveries(int nodeID) throws Exception
   {
   	if (trace) { log.trace(this + " handleAckAllDeliveries " + nodeID); }
   	
   	lock.readLock().acquire();
   	
   	try
   	{
   		if (localNameMap != null)
   		{
   			Iterator iter = localNameMap.values().iterator();
   			
   			while (iter.hasNext())
   			{
   				Binding binding = (Binding)iter.next();
   				
   				binding.queue.removeAllFromRecoveryArea(nodeID);
   			}
   		}
   	}
   	finally
   	{
   		lock.readLock().release();
   	}   	
   }
   
   public void handleAddAllReplicatedDeliveries(int nodeID, Map deliveries) throws Exception
   {
   	if (trace) { log.trace(this + " handleAddAllReplicatedDeliveries " + nodeID); }
   	
   	lock.readLock().acquire();
   	
   	try
   	{
   		if (localNameMap == null)
   		{
   			throw new IllegalStateException("Cannot add all replicated deliveries since there are no bindings - probably the queues aren't deployed");
   		}
   		
   		if (localNameMap != null)
   		{
   			Iterator iter = deliveries.entrySet().iterator();

   			while (iter.hasNext())
   			{
   				Map.Entry entry = (Map.Entry)iter.next();
   				
   				String queueName = (String)entry.getKey();
   				
   				Map ids = (Map)entry.getValue();
   				
		
   				Binding binding = (Binding)localNameMap.get(queueName);
   				
   				if (binding == null)
   				{
   					throw new IllegalStateException("Cannot find binding with name " + queueName + " maybe it hasn't been deployed");
   				}
   				
   				binding.queue.addAllToRecoveryArea(nodeID, ids);
   			}   			   			
   		}
   	}
   	finally
   	{
   		lock.readLock().release();
   	}   
   }
  

   // Replicator implementation --------------------------------------------------------------------

   public void put(Serializable key, Serializable replicant) throws Exception
   {
      putReplicantLocally(thisNodeID, key, replicant);

      PutReplicantRequest request = new PutReplicantRequest(thisNodeID, key, replicant);

      groupMember.multicastControl(request, true);
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
      if (removeReplicantLocally(thisNodeID, key))
      {
         RemoveReplicantRequest request = new RemoveReplicantRequest(thisNodeID, key);

         groupMember.multicastControl(request, true);

         return true;
      }
      else
      {
         return false;
      }
   }

   // JDBCSupport overrides ------------------------------------------------------------------------
   
   protected Map getDefaultDMLStatements()
   {
      Map map = new LinkedHashMap();

      map.put("INSERT_BINDING",
              "INSERT INTO JBM_POSTOFFICE (" +
                 "POSTOFFICE_NAME, " +
                 "NODE_ID, " +
                 "QUEUE_NAME, " +
                 "CONDITION, " +
                 "SELECTOR, " +
                 "CHANNEL_ID, " +
                 "CLUSTERED, " +
                 "ALL_NODES) " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

      map.put("DELETE_BINDING",
              "DELETE FROM JBM_POSTOFFICE WHERE POSTOFFICE_NAME=? AND NODE_ID=? AND QUEUE_NAME=?");

      map.put("LOAD_BINDINGS",
              "SELECT " +                
                 "QUEUE_NAME, " +
                 "CONDITION, " +
                 "SELECTOR, " +
                 "CHANNEL_ID, " +
                 "CLUSTERED, " +
                 "ALL_NODES " +
                 "FROM JBM_POSTOFFICE WHERE POSTOFFICE_NAME=? AND NODE_ID=?");

      return map;
   }

   protected Map getDefaultDDLStatements()
   {
      Map map = new LinkedHashMap();
      map.put("CREATE_POSTOFFICE_TABLE",
              "CREATE TABLE JBM_POSTOFFICE (POSTOFFICE_NAME VARCHAR(255), NODE_ID INTEGER," +
              "QUEUE_NAME VARCHAR(255), CONDITION VARCHAR(1023), " +
              "SELECTOR VARCHAR(1023), CHANNEL_ID BIGINT, " +
              "CLUSTERED CHAR(1), ALL_NODES CHAR(1), PRIMARY KEY(POSTOFFICE_NAME, NODE_ID, QUEUE_NAME))");
      return map;
   }

   // Public ---------------------------------------------------------------------------------------

   public boolean isSupportsFailover()
   {
      return supportsFailover;
   }

   public String printBindingInformation()
   {
//      StringWriter buffer = new StringWriter();
//      PrintWriter out = new PrintWriter(buffer);
//      out.println("Ocurrencies of nameMaps:");
//      out.println("<table border=1>");
//      for (Iterator mapIterator = nameMaps.entrySet().iterator();mapIterator.hasNext();)
//      {
//          Map.Entry entry = (Map.Entry)mapIterator.next();
//          out.println("<tr><td colspan=3><b>Map on node " + entry.getKey() + "</b></td></tr>");
//          Map valuesOnNode = (Map)entry.getValue();
//
//          out.println("<tr><td>Key</td><td>Value</td><td>Class of Value</td></tr>");
//          for (Iterator valuesIterator=valuesOnNode.entrySet().iterator();valuesIterator.hasNext();)
//          {
//              Map.Entry entry2 = (Map.Entry)valuesIterator.next();
//
//              out.println("<tr>");
//              out.println("<td>" + entry2.getKey() + "</td><td>" + entry2.getValue()+
//                 "</td><td>" + entry2.getValue().getClass().getName() + "</td>");
//              out.println("</tr>");
//
//              if (entry2.getValue() instanceof Binding &&
//                 ((Binding)entry2.getValue()).getQueue() instanceof Queue)
//              {
//                  Queue queue =
//                     ((Binding)entry2.getValue()).getQueue();
//                  List undelivered = queue.undelivered(null);
//                  if (!undelivered.isEmpty())
//                  {
//                      out.println("<tr><td>List of undelivered messages on Paging</td>");
//
//                      out.println("<td colspan=2><table border=1>");
//                      out.println("<tr><td>Reference#</td><td>Message</td></tr>");
//                      for (Iterator i = undelivered.iterator();i.hasNext();)
//                      {
//                          SimpleMessageReference reference = (SimpleMessageReference)i.next();
//                          out.println("<tr><td>" + reference.getInMemoryChannelCount() +
//                             "</td><td>" + reference.getMessage() +"</td></tr>");
//                      }
//                      out.println("</table></td>");
//                      out.println("</tr>");
//                  }
//              }
//          }
//      }
//
//      out.println("</table>");
//      out.println("<br>Ocurrencies of conditionMap:");
//      out.println("<table border=1>");
//      out.println("<tr><td>EntryName</td><td>Value</td>");
//
//      for (Iterator iterConditions = conditionMap.entrySet().iterator();iterConditions.hasNext();)
//      {
//          Map.Entry entry = (Map.Entry)iterConditions.next();
//          out.println("<tr><td>" + entry.getKey() + "</td><td>" + entry.getValue() + "</td></tr>");
//
//          if (entry.getValue() instanceof Bindings)
//          {
//              out.println("<tr><td>Binding Information:</td><td>");
//              out.println("<table border=1>");
//              out.println("<tr><td>Binding</td><td>Queue</td></tr>");
//              Bindings bindings = (Bindings)entry.getValue();
//              for (Iterator i = bindings.getAllBindings().iterator();i.hasNext();)
//              {
//
//                  Binding binding = (Binding)i.next();
//                  out.println("<tr><td>" + binding + "</td><td>" + binding.getQueue() +
//                     "</td></tr>");
//              }
//              out.println("</table></td></tr>");
//          }
//      }
//      out.println("</table>");
//          
//      out.println("Replicator's Information");
//
//      out.println("<table border=1><tr><td>Node</td><td>Key</td><td>Value</td></tr>");
//
//      for (Iterator iter = replicatedData.entrySet().iterator(); iter.hasNext();)
//      {
//         Map.Entry entry = (Map.Entry) iter.next();
//         Map subMap = (Map)entry.getValue();
//
//         for (Iterator subIterator = subMap.entrySet().iterator(); subIterator.hasNext();)
//         {
//            Map.Entry subValue = (Map.Entry) subIterator.next();
//            out.println("<tr><td>" + entry.getKey() + "</td>");
//            out.println("<td>" + subValue.getKey() + "</td><td>" + subValue.getValue() + "</td></tr>" );
//         }
//
//      }
//
//      out.println("</table>");
//      
//      return buffer.toString();
   	
   	return "";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
   
   // Private ------------------------------------------------------------------------------------
     
   private void init()
   {
      mappings = new HashMap();
      
      nameMaps = new HashMap();
      
      channelIDMap = new HashMap(); 
      
      nodeIDAddressMap = new ConcurrentHashMap();           
      
      if (clustered)
      {
      	replicatedData = new HashMap();

         failoverMap = new ConcurrentHashMap();

         leftSet = new ConcurrentHashSet();
      }
      
      //NOTE, MUST be a QueuedExecutor so we ensure that responses arrive back in order
      replyExecutor = new QueuedExecutor(new LinkedQueue());
   }
   
   private void deInit()
   {
   	mappings = null;
   	
   	nameMaps = null;
   	
   	channelIDMap = null;
   	
   	nodeIDAddressMap = null;
   	
   	if (clustered)
      {
      	replicatedData = null;

         failoverMap = null;

         leftSet = null;
      }
   	
   	replyExecutor.shutdownNow();   	
   }
   
   private void requestDeliveries(Queue queue) throws Exception
   {
   	if (!firstNode && supportsFailover && clustered && queue.isClustered())
   	{
	   	// reverse lookup in failover map
			
			Integer masterNodeID = getMasterForFailoverNodeID(thisNodeID);
			   		   		
			if (masterNodeID != null)
			{
				Map nameMap = (Map)nameMaps.get(masterNodeID);
				
				if (nameMap != null)
				{
					Binding b = (Binding)nameMap.get(queue.getName());
					
					if (b != null)
					{
						//Already deployed on master node - tell the master to send us the deliveries
						//This copes with the case when queues were deployed on the failover before being deployed on the master
						
						if (trace) { log.trace("Telling master to send us deliveries"); }
						
						dumpFailoverMap(this.failoverMap);
						
						PostOfficeAddressInfo info = (PostOfficeAddressInfo)nodeIDAddressMap.get(new Integer(thisNodeID));
			   		
			   		Address replyAddress = info.getDataChannelAddress();
						
						ClusterRequest request = new GetReplicatedDeliveriesRequest(queue.getName(), replyAddress);
						
					   info = (PostOfficeAddressInfo)nodeIDAddressMap.get(masterNodeID);
					   			   
					   Address address = info.getDataChannelAddress();
					      	
					   if (address != null)
					   {	   
					   	groupMember.unicastData(request, address);
					   }
					}
				}
			}
   	}
   }
   
   private Integer getMasterForFailoverNodeID(long failoverNodeID)
   {
   	//reverse lookup of master node id given failover node id
   	
   	Iterator iter = failoverMap.entrySet().iterator();
		
		Integer nodeID = null;
		
		while (iter.hasNext())
		{
			Map.Entry entry = (Map.Entry)iter.next();
			
			Integer fnodeID = (Integer)entry.getValue();
			
			nodeID = (Integer)entry.getKey();
			
			if (fnodeID.intValue() == failoverNodeID)
			{
				//We are the failover node for another node
				
				break;
			}
		}
		
   	return nodeID;
   }
   
   private Address getFailoverNodeDataChannelAddress()
   {
   	PostOfficeAddressInfo info = (PostOfficeAddressInfo)nodeIDAddressMap.get(new Integer(failoverNodeID));
   	
   	if (info == null)
   	{
   		return null;
   	}
   	
   	Address address = info.getDataChannelAddress();
   	
   	return address;
   }

   
   private void waitForBindUnbind(String queueName, boolean bind) throws Exception
   {
   	if (trace) { log.trace(this + " waiting for " + (bind ? "bind" : "unbind") + " of "+ queueName + " on all nodes"); }
   	
   	Set nodesToWaitFor = new HashSet(nodeIDAddressMap.keySet());
		
		long timeToWait = groupMember.getCastTimeout();
		
		long start = System.currentTimeMillis();
		
		boolean boundAll = true;
		
		boolean unboundAll = true;
				
		synchronized (waitForBindUnbindLock)
		{			
			do
			{		
				boundAll = true;
				
				unboundAll = true;
								
				lock.readLock().acquire();
						
				try
				{
					// Refresh the to wait for map - a node might have left
					
					Iterator iter = nodesToWaitFor.iterator();
					
					while (iter.hasNext())
					{
						Integer node = (Integer)iter.next();
						
						if (!nodeIDAddressMap.containsKey(node))
						{
							iter.remove();
						}
						else
						{
							Map nameMap = (Map)nameMaps.get(node);
							
							if (nameMap != null && nameMap.get(queueName) != null)
							{
								if (trace) { log.trace(this + " queue " + queueName + " exists on node " + node); }
								unboundAll = false;
							}
							else
							{
								if (trace) { log.trace(this + " queue " + queueName + " does not exist on node " + node); }
								boundAll = false;
							}
						}
					}
				}
				finally
				{
					lock.readLock().release();
				}	
					
				if ((bind && !boundAll) || (!bind && !unboundAll))
				{	
					try
					{
						if (trace) { log.trace(this + " waiting for bind unbind lock, timeout=" + groupMember.getCastTimeout()); }
						
						waitForBindUnbindLock.wait(groupMember.getCastTimeout());

						if (trace) { log.trace(this + " woke up"); }
					}
					catch (InterruptedException e)
					{
						//Ignore
					}
					timeToWait -= System.currentTimeMillis() - start;
				}												
			}
			while (((bind && !boundAll) || (!bind && !unboundAll)) && timeToWait > 0);
			
			if (trace) { log.trace(this + " waited ok"); }
		}
		if ((bind && !boundAll) || (!bind && !unboundAll))
		{
			throw new IllegalStateException(this + " timed out waiting for " + (bind ? " bind " : " unbind ") + "ALL to occur");
		}
   }
   
   private boolean internalAddBinding(Binding binding, boolean allNodes, boolean sync) throws Exception
   {
   	if (trace) { log.trace(thisNodeID + " binding " + binding.queue + " with condition " + binding.condition + " all nodes " + allNodes); }

   	if (binding == null)
      {
         throw new IllegalArgumentException("Binding is null");
      }
   	
   	Condition condition = binding.condition;
   	
   	Queue queue = binding.queue;
   	
   	if (queue == null)
      {
         throw new IllegalArgumentException("Queue is null");
      }
   	
   	if (queue.getNodeID() != thisNodeID)
   	{
   		throw new IllegalArgumentException("Cannot bind a queue from another node");
   	}

      if (condition == null)
      {
         throw new IllegalArgumentException("Condition is null");
      }
           
   	//The binding might already exist - this could happen if the queue is bind all simultaneously from more than one node of the cluster
      boolean added = addBindingInMemory(binding);  	
      
      if (added)
      {
      	if (queue.isRecoverable())
      	{
      		// Need to write the mapping to the database
      		insertBindingInStorage(condition, queue, binding.allNodes);
      	}
      	
      	if (clustered && queue.isClustered())
         {
         	String filterString = queue.getFilter() == null ? null : queue.getFilter().getFilterString();      	
         	
         	MappingInfo info = new MappingInfo(thisNodeID, queue.getName(), condition.toText(), filterString, queue.getChannelID(),
         			                             queue.isRecoverable(), true,
         			                             binding.allNodes,
         			                             queue.getFullSize(), queue.getPageSize(), queue.getDownCacheSize(),
         			                             queue.getMaxSize(),
         			                             queue.getRecoverDeliveriesTimeout());
         	
            ClusterRequest request = new BindRequest(info, allNodes);

            groupMember.multicastControl(request, sync);
         }      	
      }      
      
      return added;
   }   
   
   private Binding internalRemoveBinding(String queueName, boolean allNodes, boolean sync) throws Throwable
   {
      if (trace) { log.trace(thisNodeID + " unbind queue: " + queueName + " all nodes " + allNodes); }

      if (queueName == null)
      {
         throw new IllegalArgumentException("Queue name is null");
      }

      Binding removed = removeBindingInMemory(thisNodeID, queueName);

      //The queue might not be removed (it's already removed) if two unbind all requests are sent simultaneously on the cluster
      if (removed != null)
      {
	      Queue queue = removed.queue;

	      Condition condition = removed.condition;

	      if (queue.isRecoverable())
	      {
	         //Need to remove from db too

	         deleteBindingFromStorage(queue);
	      }

	      if (clustered && queue.isClustered())
	      {
	      	String filterString = queue.getFilter() == null ? null : queue.getFilter().getFilterString();

	      	MappingInfo info = new MappingInfo(thisNodeID, queue.getName(), condition.toText(), filterString, queue.getChannelID(),
	      			                             queue.isRecoverable(), true, allNodes);

		      UnbindRequest request = new UnbindRequest(info, allNodes);

		      groupMember.multicastControl(request, sync);
	      }

         queue.removeAllReferences();

      }

      return removed;
   }

   private synchronized void calculateFailoverMap()
   {
   	failoverMap.clear();
   	
   	View view = groupMember.getCurrentView();
   	
   	Vector members = view.getMembers();
   	
   	for (int i = 0; i < members.size(); i++)
   	{
   		Address address = (Address)members.get(i);
   		
   		Integer theNodeID = findNodeIDForAddress(address);
   		
   		if (theNodeID == null)
   		{
   			throw new IllegalStateException("Cannot find node id for address " + address);
   		}
   		
   		int j;
   		
   		if (i != members.size() - 1)
   		{
   			j = i + 1;
   		}
   		else
   		{
   			j = 0;
   		}
   		
   		Address failoverAddress = (Address)members.get(j);
   		
   		Integer failoverNodeID = this.findNodeIDForAddress(failoverAddress);
   		
   		if (failoverNodeID == null)
   		{
   			throw new IllegalStateException("Cannot find node id for address " + failoverAddress);
   		}
   		
   		failoverMap.put(theNodeID, failoverNodeID);	   			   		   
   	}   	
   	
   	int fid = ((Integer)failoverMap.get(new Integer(thisNodeID))).intValue();
   	
   	//if we are the first node in the cluster we don't want to be our own failover node!
   	
   	if (fid == thisNodeID)
   	{
   		firstNode = true;
   		failoverNodeID = -1;
   	}
   	else
   	{
   		failoverNodeID = fid;
   		firstNode = false;	   		
   	}	   	   	 
   	
      log.debug("Updated failover map:\n" + dumpFailoverMap(failoverMap));   	      
   }
   
   private Integer findNodeIDForAddress(Address address)
   {
   	Integer theNodeID = null;
		
   	Iterator iter = this.nodeIDAddressMap.entrySet().iterator();
   	
		while (iter.hasNext())
		{
			Map.Entry entry = (Map.Entry)iter.next();
			
			Integer nodeID = (Integer)entry.getKey();
			
			PostOfficeAddressInfo info = (PostOfficeAddressInfo)entry.getValue();
			
			if (info.getControlChannelAddress().equals(address))
			{
				theNodeID = nodeID;
				
				break;
			}
		}
		
		return theNodeID;
   }
   
   private Collection getBindings(String queueName) throws Exception
   {
   	lock.readLock().acquire();

   	try
   	{
   		Iterator iter = nameMaps.values().iterator();
   		
   		List bindings = new ArrayList();
   		
   		while (iter.hasNext())
   		{
   			Map nameMap = (Map)iter.next();
   			
   			if (queueName != null)
   			{
	   			Binding binding = (Binding)nameMap.get(queueName);
	   			
	   			if (binding != null)
	   			{
	   				bindings.add(binding);
	   			}
   			}
   			else
   			{
   				bindings.addAll(nameMap.values());
   			}
   		}
   		
   		return bindings;
   	}
   	finally
   	{
   		lock.readLock().release();
   	}
   }
   
   private boolean routeInternal(MessageReference ref, Condition condition, Transaction tx, boolean fromCluster, Set names) throws Exception
   {
   	if (trace) { log.trace(this + " routing " + ref + " with condition '" +
   			                 condition + "'" + (tx == null ? "" : " transactionally in " + tx) + 
   			                 " from cluster " + fromCluster); }
   	
      boolean routed = false;

      lock.readLock().acquire();
      
      try
      {
         List queues = (List)mappings.get(condition);
         
         if (queues != null)
         {
         	Iterator iter = queues.iterator();
         	
         	int localReliableCount = 0;
         	
         	Set remoteSet = null;
         	
         	List targets = new ArrayList();
         	
         	while (iter.hasNext())
         	{
         		Queue queue = (Queue)iter.next();
         		
         		if (trace) { log.trace(this + " considering queue " + queue); }
         		
         		if (queue.getNodeID() == thisNodeID)
         		{
         			if (trace) { log.trace(this + " is a local queue"); }
         		
         			//Local queue

         			boolean routeLocal = false;
         			
         			if (!fromCluster)
         			{
         				//Same node
         				routeLocal = true;
         			}
         			else
         			{
         				//From the cluster
         				if (!queue.isRecoverable() && queue.isClustered())
         				{
         					//When routing from the cluster we only route to non recoverable queues
         					//who haven't already been routed to on the sending node (same name)
         					//Also we don't route to non clustered queues
         					if (names == null || !names.contains(queue.getName()))
         					{
         						routeLocal = true;
         					}
         				}
         			}
         			
         			if (routeLocal)
         			{
         				//If we're not routing from the cluster OR the queue is unreliable then we consider it
         				
         				//When we route from the cluster we never route to reliable queues
         				
	         			Filter filter = queue.getFilter();
	         			
	         			if (filter == null || filter.accept(ref.getMessage()))
	         			{                		
	         				if (trace) { log.trace(this + " Added queue " + queue + " to list of targets"); }
	         				
	      					targets.add(queue);
	      					
	      					if (ref.getMessage().isReliable() && queue.isRecoverable())
	         				{
	         					localReliableCount++;
	         				}         				
	         			}   
         			}
         		}
         		else if (!fromCluster)
         		{
         			//Remote queue
         			
         			if (trace) { log.trace(this + " is a remote queue"); }
         			
         			if (!queue.isRecoverable() && queue.isClustered())
         			{	         			
         				//When we send to the cluster we never send to reliable queues
         				
	         			Filter filter = queue.getFilter();
	         			
	         			if (filter == null || filter.accept(ref.getMessage()))
	         			{
	         				if (remoteSet == null)
	         				{
	         					remoteSet = new HashSet();
	         				}
	         				
	         				remoteSet.add(new Integer(queue.getNodeID()));
	         				
	         				if (trace) { log.trace(this + " added it to the remote set for casting"); }
	         			}
         			}
         			else
         			{
         				if (trace) { log.trace(this + " is recoverable so not casting"); }
         			}
         		}
         	}
         	         	         	
         	//If the ref is reliable and there is more than one reliable local queue that accepts the message then we need
         	//to route in a transaction to guarantee once and only once reliability guarantee
         	
         	boolean startedTx = false;
         	
         	if (tx == null && localReliableCount > 1)
         	{
         		if (trace) { log.trace("Starting internal tx, reliableCount = " + localReliableCount); }
         		
         		tx = tr.createTransaction();
         		
         		startedTx = true;
         	}
         	
         	//Now actually route the ref
         	
         	iter = targets.iterator();
         	
         	Set queueNames = null;
         	
         	while (iter.hasNext())
         	{
         		Queue queue = (Queue)iter.next();
         		
         		if (trace) { log.trace(this + " Routing ref to queue " + queue); }
         		         	
         		Delivery del = queue.handle(null, ref, tx);
         		
         		if (trace) { log.trace("Queue returned " + del); }

               if (del != null && del.isSelectorAccepted())
               {
                  routed = true;
                  
                  if (remoteSet != null)
                  {
                  	if (queueNames == null)
                  	{
                  		queueNames = new HashSet();
                  	}
                  	
                  	//We put the queue name in a set - this is used on other nodes after routing from the cluster so it
                  	//doesn't route to queues with the same name on other nodes
                  	queueNames.add(queue.getName());
                  }
               }
         	}
         	            
            if (remoteSet != null)
         	{
         		//There are queues on other nodes that want the message too
         		
         		//If the message is non reliable then we can unicast or multicast the message to the group so it
         		//can get picked up by other nodes         		
         		
         		ClusterRequest request = new MessageRequest(condition.toText(), ref.getMessage(), queueNames);
         		
         		if (trace) { log.trace(this + " casting message to other node(s)"); }
         		
         		Integer nodeID = null;
         		
         		if (remoteSet.size() == 1)
         		{
         			//Only one node requires the message, so we can unicast
         			
         			nodeID = (Integer)remoteSet.iterator().next();
         		}
         		
         		TxCallback callback = new CastMessageCallback(nodeID, request);
         		
         		if (tx != null)
         		{
         			tx.addCallback(callback, this);
         		}
         		else
         		{
         			//Execute it now
         			callback.afterCommit(true);
         		}
         		
         		routed = true;
         	}          
            
            if (startedTx)
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

   private Binding removeBindingInMemory(int nodeID, String queueName) throws Exception
   {
   	lock.writeLock().acquire();
   	
   	Binding binding = null;
   	
   	try
   	{
   		Integer nid = new Integer(nodeID);
   		
	   	Map nameMap = (Map)nameMaps.get(nid);
	   	
	   	if (nameMap == null)
	   	{
	   		return null;
	   	}
	   	
	   	binding = (Binding)nameMap.remove(queueName);
	   	
	   	if (binding == null)
	   	{
	   		return null;
	   	}
	   	
	   	if (nameMap.isEmpty())
	   	{
	   		nameMaps.remove(nid);
	   		
	   		if (nodeID == thisNodeID)
	   		{
	   			localNameMap = null;
	   		}
	   	}
	   	
	   	binding = (Binding)channelIDMap.remove(new Long(binding.queue.getChannelID()));
			
			if (binding == null)
			{
				throw new IllegalStateException("Cannot find binding in channel id map for queue " + queueName);
			}
	   	   	
	   	List queues = (List)mappings.get(binding.condition);
		      
	      if (queues == null)
	      {
	      	throw new IllegalStateException("Cannot find queues in condition map for condition " + binding.condition);
	      }	     
	      
	      boolean removed = queues.remove(binding.queue);
	      
	      if (!removed)
	      {
	      	throw new IllegalStateException("Cannot find queue in list for queue " + queueName);
	      }
	      
	      if (queues.isEmpty())
	      {
	      	mappings.remove(binding.condition);
	      }
	      
   	}
   	finally
   	{
   		lock.writeLock().release();
   	}
   	
      // Send a notification
      ClusterNotification notification = new ClusterNotification(ClusterNotification.TYPE_UNBIND, nodeID, queueName);
      
      clusterNotifier.sendNotification(notification);
      
      return binding;
   }
   
   private boolean addBindingInMemory(Binding binding) throws Exception
   {
   	Queue queue = binding.queue;
   	   	
   	if (trace) { log.trace(this + " Adding binding in memory " + binding); }
   	
   	lock.writeLock().acquire();
   	
   	try
   	{	  
   		Integer nid = new Integer(queue.getNodeID());
   		
   		Map nameMap = (Map)nameMaps.get(nid);
   		   		
   		if (nameMap != null && nameMap.containsKey(queue.getName()))
   		{
   			return false;
   		}
   		
   		Long cid = new Long(queue.getChannelID());
   		
   		if (channelIDMap.containsKey(cid))
   		{
   			throw new IllegalStateException("Channel id map for node " + nid + " already contains binding for queue " + cid);
   		}
   		
   		if (nameMap == null)
   		{
   			nameMap = new HashMap();
   			
   			nameMaps.put(nid, nameMap);
   			
   			if (queue.getNodeID() == thisNodeID)
   			{
   				localNameMap = nameMap;
   			}
   		}
   		
   		nameMap.put(queue.getName(), binding);
   		
   		channelIDMap.put(cid, binding);
   			   	
	   	Condition condition = binding.condition;   	
	   	
	   	List queues = (List)mappings.get(condition);
	   	
	   	if (queues == null)
	   	{
	   		queues = new ArrayList();
	   		
	   		if (queues.contains(queue))
	   		{
	   			throw new IllegalArgumentException("Queue is already bound with condition " + condition);
	   		}
	   		
	   		mappings.put(condition, queues);
	   	}
	   	
	   	queues.add(queue);  
   	}
   	finally
   	{
   		lock.writeLock().release();
   	}
   	
   	if (trace) { log.trace(this + " Sending cluster notification"); }
   	
      //Send a notification
      ClusterNotification notification = new ClusterNotification(ClusterNotification.TYPE_BIND, queue.getNodeID(), queue.getName());
      
      clusterNotifier.sendNotification(notification);
      
      return true;
   }
   
   /*
    * Multicast a message on the data channel to all members of the group
    */
   private void multicastRequest(ClusterRequest request) throws Exception
   {
      if (trace) { log.trace(this + " Unicasting request " + request); }
      
      groupMember.multicastData(request);
   }

   /*
    * Unicast a message on the data channel to one member of the group
    */
   private void unicastRequest(ClusterRequest request, int nodeId) throws Exception
   {
      Address address = getAddressForNodeId(nodeId, false);

      if (address == null)
      {
         throw new IllegalArgumentException("Cannot find address for node " + nodeId);
      }
      
      if (trace) { log.trace(this + "Unicasting request " + request + " to node " + nodeId); }

      groupMember.unicastData(request, address);
   }
   
   
   private Map getBindingsFromStorage() throws Exception
   {
      class LoadBindings extends JDBCTxRunner<Map>
      {
         public Map doTransaction() throws Exception
         {
            PreparedStatement ps  = null;
            ResultSet rs = null;

            Map bindings = new HashMap();

            try
            {
               ps = conn.prepareStatement(getSQLStatement("LOAD_BINDINGS"));

               ps.setString(1, officeName);

               ps.setInt(2, thisNodeID);

               rs = ps.executeQuery();

               while (rs.next())
               {
                  String queueName = rs.getString(1);
                  String conditionText = rs.getString(2);
                  String selector = rs.getString(3);

                  if (rs.wasNull())
                  {
                     selector = null;
                  }

                  long channelID = rs.getLong(4);

                  boolean bindingClustered = rs.getString(5).equals("Y");

                  boolean allNodes = rs.getString(6).equals("Y");

                  //If the node is not clustered then we load the bindings as non clustered

                  Filter filter = null;

                  if (selector != null)
                  {
                     filter = filterFactory.createFilter(selector);
                  }

                  Queue queue = new MessagingQueue(thisNodeID, queueName, channelID, ms, pm,
                                                   true, filter, bindingClustered && clustered);

                  if (trace) { log.trace(this + " loaded binding from storage: " + queueName); }

                  Condition condition = conditionFactory.createCondition(conditionText);

                  Binding binding = new Binding(condition, queue, allNodes);

                  bindings.put(queueName, binding);
               }

               return bindings;
            }
            finally
            {
               closeResultSet(rs);

               closeStatement(ps);
            }
         }
      }

      return new LoadBindings().executeWithRetry();
   }
   
   private void loadBindings() throws Exception
   {   	
      Iterator iter = loadedBindings.values().iterator();
      
      while (iter.hasNext())
      {
      	Binding binding = (Binding)iter.next();
      	
      	addBindingInMemory(binding);    
      	
      	Queue queue = binding.queue;
         
         //Need to broadcast it too
         if (clustered && queue.isClustered())
         {
         	String filterString = queue.getFilter() == null ? null : queue.getFilter().getFilterString();      	            	
         	
         	MappingInfo info = new MappingInfo(thisNodeID, queue.getName(), binding.condition.toText(), filterString, queue.getChannelID(),
         			                             queue.isRecoverable(), true,
         			                             binding.allNodes,
         			                             queue.getFullSize(), queue.getPageSize(), queue.getDownCacheSize(),
         			                             queue.getMaxSize(),
         			                             queue.getRecoverDeliveriesTimeout());
         	
            ClusterRequest request = new BindRequest(info, binding.allNodes);

            groupMember.multicastControl(request, false);         
         }      	
         
         requestDeliveries(queue);
      }
   }
    

   private void insertBindingInStorage(final Condition condition, final Queue queue, final boolean allNodes) throws Exception
   {
      class InsertBindings extends JDBCTxRunner
      {
         public Object doTransaction() throws Exception
         {
            PreparedStatement ps  = null;

            try
            {
               ps = conn.prepareStatement(getSQLStatement("INSERT_BINDING"));

               ps.setString(1, officeName);
               ps.setInt(2, thisNodeID);
               ps.setString(3, queue.getName());
               ps.setString(4, condition.toText());
               String filterString = queue.getFilter() != null ? queue.getFilter().getFilterString() : null;
               if (filterString != null)
               {
                  ps.setString(5, filterString);
               }
               else
               {
                  ps.setNull(5, Types.VARCHAR);
               }
               ps.setLong(6, queue.getChannelID());
               if (queue.isClustered())
               {
                  ps.setString(7, "Y");
               }
               else
               {
                  ps.setString(7, "N");
               }
               if (allNodes)
               {
                  ps.setString(8, "Y");
               }
               else
               {
                  ps.setString(8, "N");
               }

               ps.executeUpdate();
            }
            finally
            {
               closeStatement(ps);
            }

            return null;
         }
      }

      new InsertBindings().executeWithRetry();
   }

   private boolean deleteBindingFromStorage(final Queue queue) throws Exception
   {
      class DeleteBindings extends JDBCTxRunner<Boolean>
      {
         public Boolean doTransaction() throws Exception
         {
            PreparedStatement ps  = null;

            try
            {
               ps = conn.prepareStatement(getSQLStatement("DELETE_BINDING"));

               ps.setString(1, officeName);
               ps.setInt(2, queue.getNodeID());
               ps.setString(3, queue.getName());

               int rows = ps.executeUpdate();

               return rows == 1;
            }
            finally
            {
               closeStatement(ps);
            }
         }
      }

      return new DeleteBindings().executeWithRetry();
   }

   private boolean leaveMessageReceived(Integer nodeId) throws Exception
   {
      return leftSet.remove(nodeId);      
   }

   /*
    * Removes all binding data, and any replicant data for the specified node.
    */
   private void cleanDataForNode(Integer nodeToRemove) throws Exception
   {
      log.debug(this + " cleaning data for node " + nodeToRemove);

      lock.writeLock().acquire();

      if (trace) { log.trace(this + " cleaning data for node " + nodeToRemove); }
      
      try
      {
      	Iterator iter = mappings.entrySet().iterator();
      	
      	List toRemove = new ArrayList();
      	
      	while (iter.hasNext())
      	{
      		Map.Entry entry = (Map.Entry)iter.next();
      		
      		Condition condition = (Condition)entry.getKey();
      		
      		List queues = (List)entry.getValue();
      		
      		Iterator iter2 = queues.iterator();
      		
      		while (iter2.hasNext())
      		{
      			Queue queue = (Queue)iter2.next();
      			
      			if (queue.getNodeID() == nodeToRemove.intValue())
      			{
      				toRemove.add(new Binding(condition, queue, false));
      			}
      		}
      	}
      	
      	iter = toRemove.iterator();
      	
      	while (iter.hasNext())
      	{
      		Binding binding = (Binding)iter.next();

      		removeBindingInMemory(nodeToRemove.intValue(), binding.queue.getName());
      	}
      }
      finally
      {
         lock.writeLock().release();
      }

      Map toNotify = new HashMap();
      
      synchronized (replicatedData)
      {
         // We need to remove any replicant data for the node.
         for (Iterator i = replicatedData.entrySet().iterator(); i.hasNext(); )
         {
            Map.Entry entry = (Map.Entry)i.next();
            String key = (String)entry.getKey();
            Map replicants = (Map)entry.getValue();

            replicants.remove(nodeToRemove);

            if (replicants.isEmpty())
            {
               i.remove();
            }

            toNotify.put(key, replicants);           
         }
      }
      
      //remove node id - address info
      nodeIDAddressMap.remove(nodeToRemove);  
      
      synchronized (waitForBindUnbindLock)
      {
      	if (trace) { log.trace(this + " notifying bind unbind lock"); }
      	waitForBindUnbindLock.notifyAll();
      }     
      
      //Notify outside the lock to prevent deadlock
      
      //Send notifications for the replicant data removed
      
      for (Iterator i = toNotify.entrySet().iterator(); i.hasNext(); )
      {
         Map.Entry entry = (Map.Entry)i.next();
         String key = (String)entry.getKey();

         ClusterNotification notification = new ClusterNotification(ClusterNotification.TYPE_REPLICATOR_REMOVE, nodeToRemove.intValue(), key);
         
         clusterNotifier.sendNotification(notification);
      }
   }

   //TODO - can optimise this with a reverse map
   private Integer getNodeIDForSyncAddress(Address address) throws Exception
   {
   	Iterator iter = nodeIDAddressMap.entrySet().iterator();
   	
   	Integer nodeID = null;
   	
   	while (iter.hasNext())
   	{
   		Map.Entry entry = (Map.Entry)iter.next();
   			
   		PostOfficeAddressInfo info = (PostOfficeAddressInfo)entry.getValue();
   		
   		if (info.getControlChannelAddress().equals(address))
   		{
   			nodeID = (Integer)entry.getKey();
   			
   			break;
   		}
   	}   	
   	
   	return nodeID;   	
   }
   	      
   private boolean knowAboutNodeId(int nodeID)
   {
   	return nodeIDAddressMap.get(new Integer(nodeID)) != null;   	
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

   private Address getAddressForNodeId(int nodeId, boolean sync) throws Exception
   {   	
   	PostOfficeAddressInfo info = (PostOfficeAddressInfo)nodeIDAddressMap.get(new Integer(nodeId));
   	
   	if (info == null)
   	{
   		return null;
   	}
   	else if (sync)
   	{
   		return info.getControlChannelAddress();
   	}
   	else
   	{
   		return info.getDataChannelAddress();
   	}     	
   }
   
   private void failoverNodeChanged(int oldFailoverNodeID, boolean firstNode, boolean joined) throws Exception
   {   	   	
   	//The failover node has changed - we need to move our replicated deliveries
   	
   	if (trace) { log.trace("Failover node has changed from " + oldFailoverNodeID + " to " + failoverNodeID); }
   	   	
   	if (!firstNode)
   	{	   	
	   	//If the old node still exists we need to send a message to remove any replicated deliveries
	   	
	   	PostOfficeAddressInfo info = (PostOfficeAddressInfo)nodeIDAddressMap.get(new Integer(oldFailoverNodeID));
	   	
	   	if (info != null)
	   	{
	   		if (trace) { log.trace("Old failover node still exists, telling it remove replicated deliveries"); }
	   		
	   		ClusterRequest request = new AckAllReplicatedDeliveriesMessage(thisNodeID);
	   		
	   		groupMember.unicastData(request, info.getDataChannelAddress());
	   		
	   		if (trace) { log.trace("Sent AckAllReplicatedDeliveriesMessage"); }
	   	}
   	}
   	
   	//Now send the deliveries to the new node - we only do this if the new failover node came about by
   	//another node LEAVING, we DON'T do this if the new failover node has just joined - this is because it won't have deployed
   	//it's queues yet - the new failover node will request its replicated data when it deploys its queues
   	
   	if (!joined)
   	{
	   	//We must lock any responses to delivery adds coming in in this period - otherwise we could end up with the same
	   	//message being delivered more than once
	   		
	   	if (localNameMap != null)
	   	{
	   		Map deliveries = new HashMap();
	   		
				//FIXME - this is ugly
				//Find a better way of getting the sessions
	   		//We shouldn't know abou the server peer
	   		
	   		if (serverPeer != null)
	   		{
					
					Collection sessions = serverPeer.getSessions();
					
					Iterator iter2 = sessions.iterator();
					
					while (iter2.hasNext())
					{
						ServerSessionEndpoint session = (ServerSessionEndpoint)iter2.next();
						
						session.deliverAnyWaitingDeliveries(null);
						
						session.collectDeliveries(deliveries, firstNode, null);				
					}   				  
					
					if (!firstNode)
					{			
			   		PostOfficeAddressInfo info = (PostOfficeAddressInfo)nodeIDAddressMap.get(new Integer(failoverNodeID));
			   		
			   		if (info == null)
			   		{
			   			throw new IllegalStateException("Cannot find address for failover node " + failoverNodeID);
			   		}		   		
						
						ClusterRequest request = new AddAllReplicatedDeliveriesMessage(thisNodeID, deliveries);
						
						groupMember.unicastData(request, info.getDataChannelAddress());
			   		
			   		if (trace) { log.trace("Sent AddAllReplicatedDeliveriesMessage"); }
					}
	   		}
	   	}  	
   	}
   }
   

   /**
    * This method fails over all the queues from node <failedNodeId> onto this node. It is triggered
    * when a JGroups view change occurs due to a member leaving and it's determined the member
    * didn't leave cleanly.
    * 
    * On failover we basically merge any queues with the same name on the failed node into any corresponding queue
    * on this node
    */
   private void performFailover(Integer failedNodeID) throws Exception
   {
      log.debug(this + " performing failover for failed node " + failedNodeID);
      
      ClusterNotification notification = new ClusterNotification(ClusterNotification.TYPE_FAILOVER_START, failedNodeID.intValue(), null);
      
      clusterNotifier.sendNotification(notification);

      log.debug(this + " announced it is starting failover procedure");
   	
      pm.mergeTransactions(failedNodeID.intValue(), thisNodeID);
      
      // Need to lock
      lock.writeLock().acquire();

      try
      {
      	Map nameMap = (Map)nameMaps.get(failedNodeID);
      	
      	List toRemove = new ArrayList();
      	
      	if (nameMap != null)
      	{
      		Iterator iter = nameMap.values().iterator();
      		
      		while (iter.hasNext())
      		{
      			Binding binding = (Binding)iter.next();
      			
      			Queue queue = binding.queue;
      			
      			if (queue.isRecoverable() && queue.getNodeID() == failedNodeID.intValue())
      			{
      				toRemove.add(binding);
      			}      			
      		}
      	}
      	         	
      	Iterator iter = toRemove.iterator();

      	while (iter.hasNext())
      	{
      		Binding binding = (Binding)iter.next();
      		
      		Condition condition = binding.condition;
      		
      		Queue queue = binding.queue;
      		
      		// Sanity check
            if (!queue.isRecoverable())
            {
               throw new IllegalStateException("Found non recoverable queue " +
                                               queue.getName() + " in map, these should have been removed!");
            }

            // Sanity check
            if (!queue.isClustered())
            {
               throw new IllegalStateException("Queue " + queue.getName() + " is not clustered!");
            }
            
            //Remove from the in-memory map - no need to broadcast anything - they will get removed from other nodes in memory
            //maps when the other nodes detect failure
            removeBindingInMemory(binding.queue.getNodeID(), binding.queue.getName());
      		
      		//Delete from storage
      		deleteBindingFromStorage(queue);
      	
            log.debug(this + " deleted binding for " + queue.getName());

            // Note we do not need to send an unbind request across the cluster - this is because
            // when the node crashes a view change will hit the other nodes and that will cause
            // all binding data for that node to be removed anyway.
            
            //Find if there is a local queue with the same name
            
            Queue localQueue = null;
            
            if (localNameMap != null)
            {
            	Binding b = (Binding)localNameMap.get(queue.getName());
            	localQueue = b.queue;
            }
            	
            if (localQueue != null)
            {
               //need to merge the queues
            	
            	log.debug(this + " has already a queue: " + queue.getName() + " queue so merging queues");
            	  
               localQueue.mergeIn(queue.getChannelID(), failedNodeID.intValue());
               
               log.debug("Merged queue");       
            }
            else
            {
            	//Cannot failover if there is no queue deployed.
            	
            	throw new IllegalStateException("Cannot failover " + queue.getName() + " since it does not exist on this node. " + 
            			                          "You must deploy your clustered destinations on ALL nodes of the cluster");
            }            
         }

         log.debug(this + ": server side fail over is now complete");
      }
      finally
      {
         lock.writeLock().release();
      }
      
      //Now clean the data for the failed node
      
      //TODO - does this need to be inside the lock above?
      cleanDataForNode(failedNodeID);
      
      log.debug(this + " announcing that failover procedure is complete");

      notification = new ClusterNotification(ClusterNotification.TYPE_FAILOVER_END, failedNodeID.intValue(), null);
      
      clusterNotifier.sendNotification(notification);
      
      //for testing only
      sendJMXNotification(FAILOVER_COMPLETED_NOTIFICATION);
   }

   private void sendJMXNotification(String notificationType)
   {
      Notification n = new Notification(notificationType, "", 0l);
      nbSupport.sendNotification(n);
      log.debug(this + " sent " + notificationType + " JMX notification");
   }

   // Inner classes --------------------------------------------------------------------------------
   
   private class SendReplicatedDeliveriesRunnable implements Runnable
   {
   	private String queueName;
   	
   	private Address address;
   	
   	SendReplicatedDeliveriesRunnable(String queueName, Address address)
   	{
   		this.queueName = queueName;
   		
   		this.address = address;
   	}
   	
   	public void run()
   	{
			try
			{
      		if (serverPeer != null)
      		{			
      			Collection sessions = serverPeer.getSessions();
      			
      			Iterator iter = sessions.iterator();
      			
      			Map dels = new HashMap();			
      			
      			boolean gotSome = false;
      			
      			while (iter.hasNext())
      			{
      				ServerSessionEndpoint session = (ServerSessionEndpoint)iter.next();
      				
      				session.deliverAnyWaitingDeliveries(queueName);
      				
      				if (session.collectDeliveries(dels, firstNode, queueName))
      				{
      					gotSome = true;
      				}
      			}   				  
      			
      			if (gotSome)
      			{
   	   			ClusterRequest req = new AddAllReplicatedDeliveriesMessage(thisNodeID, dels);
   	   			
   	   			groupMember.unicastData(req, address);
      			}
      			   			
      		}
			}
			catch (Exception e)
			{
				log.error("Failed to collect and send request", e);
			}
   	}	
   }
   
   private class CastMessageCallback implements TxCallback
   {
   	private Integer nodeID;
   	
   	private ClusterRequest request;
   	
   	CastMessageCallback(Integer nodeID, ClusterRequest request)
   	{
   		this.nodeID = nodeID;
   		
   		this.request = request;
   	}

		public void afterCommit(boolean onePhase) throws Exception
		{
//			if (nodeID == null)
//			{
//				multicastRequest(request);
//			}
//			else
//			{
//				unicastRequest(request, nodeID.intValue());
//			}
			
			//For now we always multicast otherwise there is the possibility that messages send unicast arrive in a different order
			//to messages send multicast
			//We might be able to fix this using anycast
			multicastRequest(request);
		}

		public void afterPrepare() throws Exception
		{	
			//NOOP
		}

		public void afterRollback(boolean onePhase) throws Exception
		{
			//NOOP
		}

		public void beforeCommit(boolean onePhase) throws Exception
		{
			//NOOP
		}

		public void beforePrepare() throws Exception
		{
			//NOOP
		}

		public void beforeRollback(boolean onePhase) throws Exception
		{
			//NOOP
		}
   	
   }

}
