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
package org.jboss.messaging.core.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.jms.server.MessagingTimeoutFactory;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Distributor;
import org.jboss.messaging.core.contract.Filter;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.contract.Receiver;
import org.jboss.messaging.core.impl.clusterconnection.MessageSucker;
import org.jboss.messaging.core.impl.tx.Transaction;
import org.jboss.util.timeout.Timeout;
import org.jboss.util.timeout.TimeoutTarget;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * 
 * A MessagingQueue
 * 
 * Can be used to implement a point to point queue, or a subscription fed from a topic
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1295 $</tt>
 *
 * $Id: Queue.java 1295 2006-09-15 17:44:02Z timfox $
 *
 */
public class MessagingQueue extends PagingChannelSupport implements Queue
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
	
	private static final Logger log = Logger.getLogger(MessagingQueue.class);
	
   private static final long DEFAULT_RECOVER_DELIVERIES_TIMEOUT = 5 * 60 * 10000;
         
   // Attributes ----------------------------------------------------
   
   private int nodeID;
   
   protected String name;
   
   protected Filter filter;
   
   protected boolean clustered;
   
   protected Distributor remoteDistributor;
   
   protected Distributor localDistributor;

   private boolean trace = log.isTraceEnabled();
   
   private Set suckers;
   
   private boolean handleFlowControlForConsumers;
   
   private Map recoveryArea;
   
   private Map recoveryMap;
   
   private long recoverDeliveriesTimeout;
   
   // Constructors --------------------------------------------------
       
   public MessagingQueue(int nodeID, String name, long id, MessageStore ms, PersistenceManager pm,             
                         boolean recoverable, int maxSize, Filter filter,
                         int fullSize, int pageSize, int downCacheSize, boolean clustered,
                         long recoverDeliveriesTimeout)
   {
      super(id, ms, pm, recoverable, maxSize, fullSize, pageSize, downCacheSize);
            
      setup(nodeID, name, filter, clustered, recoverDeliveriesTimeout);
   }
   
   /** This constructor is used when loading queue from storage - the paging params, maxSize and recoverDeliveriesTimeout don't matter
    * they are injected when the queue or topic service is started
    */
   public MessagingQueue(int nodeID, String name, long id, MessageStore ms, PersistenceManager pm,             
					          boolean recoverable, Filter filter,
					          boolean clustered)
	{
		super(id, ms, pm, recoverable, -1, 100000, 2000, 2000); //paging params etc are actually ignored
		
		setup(nodeID, name, filter, clustered, DEFAULT_RECOVER_DELIVERIES_TIMEOUT);
	}
   
   /* This constructor is only used in tests - should we remove it? */
   public MessagingQueue(int nodeID, String name, long id, MessageStore ms, PersistenceManager pm,             
   		                boolean recoverable, int maxSize, Filter filter, boolean clustered)
   {
   	super(id, ms, pm, recoverable, maxSize);

   	setup(nodeID, name, filter, clustered, DEFAULT_RECOVER_DELIVERIES_TIMEOUT);
   }
   
   /* Constructor for a remote queue representation in a cluster */
   public MessagingQueue(int nodeID, String name, long id, boolean recoverable,
   		                Filter filter, boolean clustered)
   {
   	super(id, null, null, recoverable, -1);
   	
   	setup(nodeID, name, filter, clustered, DEFAULT_RECOVER_DELIVERIES_TIMEOUT);   	
   }
   
   private void setup(int nodeID, String name, Filter filter, boolean clustered, long recoverDeliveriesTimeout)
   {
   	this.nodeID = nodeID;

   	this.name = name;

   	this.filter = filter;

   	this.clustered = clustered;
   	
   	this.recoverDeliveriesTimeout = recoverDeliveriesTimeout;
   	
   	localDistributor = new DistributorWrapper(new RoundRobinDistributor());
   	
   	remoteDistributor = new DistributorWrapper(new RoundRobinDistributor());
   	
   	distributor = new ClusterRoundRobinDistributor(localDistributor, remoteDistributor);
   	
   	suckers = new HashSet();
   	
   	recoveryArea = new ConcurrentReaderHashMap();
   	
   	recoveryMap = Collections.synchronizedMap(new LinkedHashMap());
   }
   
   // Queue implementation
   // ---------------------------------------------------------------
      
   public int getNodeID()
   {
   	return nodeID;
   }
   
   public String getName()
   {
      return name;
   }
      
   public Filter getFilter()
   {
      return filter;
   }
   
   public boolean isClustered()
   {
      return clustered;
   }
   
   public Distributor getLocalDistributor()
   {
   	return localDistributor;
   }
   
   public Distributor getRemoteDistributor()
   {
   	return remoteDistributor;
   }
   
   /**
    * Merge the contents of one queue with another - this happens at failover when a queue is failed
    * over to another node, but a queue with the same name already exists. In this case we merge the
    * two queues.
    */
   public void mergeIn(long theChannelID, int nodeID) throws Exception
   {
      if (trace) { log.trace("Merging queue " + channelID + " node id " + nodeID + " into " + this + 
      		                 " initially refs:" + messageRefs.size()); }
           
      synchronized (lock)
      {
         flushDownCache();
                  
         PersistenceManager.InitialLoadInfo ili =
            pm.mergeAndLoad(theChannelID, this.channelID, fullSize - messageRefs.size(),
                            firstPagingOrder, nextPagingOrder);
            
         if (trace) { log.trace("Loaded " + ili.getRefInfos().size() + " refs"); }            

         doLoad(ili);         
         
         Map toRecover = (Map)recoveryArea.remove(new Integer(nodeID));
         
         if (trace) { log.trace("To recover is: " + toRecover); }
         
         LinkedList toTimeout = new LinkedList();
         
         if (toRecover != null)
         {         	
            //TODO this can be optimised to avoid a second scan - we could do this in load
         
         	if (trace) { log.trace("Recovery area is not empty, putting refs in recovery map"); }
         	
         	Iterator iter = messageRefs.iterator();
         	
         	while (iter.hasNext())
         	{
         		MessageReference ref = (MessageReference)iter.next();
         		
         		Message message = ref.getMessage();
         		
         		String sessionID = (String)toRecover.remove(new Long(message.getMessageID()));
         		
         		if (sessionID != null)
         		{
         			if (trace) { log.trace("Added ref " + ref + " to recovery map"); }
         			
         			RecoveryEntry re = new RecoveryEntry();
         			re.ref = ref;
         			re.sessionID = sessionID;
         			
         			recoveryMap.put(new Long(message.getMessageID()), re);
         			
         			iter.remove();
         			
         			toTimeout.addLast(ref);
         		}
         	}
         	         	
         	//Note!
      		//It is possible there are message ids in the toRecover that are not in the queue
      		//This can happen if a delivery is replicated, the message delivered, then acked, then the node crashes
      		//before the ack is replicated.      		
      		//This is ok
         	
         	
         	//Set up a timeout to put the refs back in the queue if they don't get claimed by failed over consumers
         	         	                   
            MessagingTimeoutFactory.instance.getFactory().
            	schedule(System.currentTimeMillis() + recoverDeliveriesTimeout, new ClearRecoveryMapTimeoutTarget(toTimeout));       
            
            if (trace) { log.trace("Set timeout to fire in " + recoverDeliveriesTimeout); }
         }
                           
         deliverInternal();
      }
   }  
   
   public List recoverDeliveries(List messageIds)
   {
   	if (trace) { log.trace("Recovering deliveries"); }
   	
   	List refs = new ArrayList();
   	
   	Iterator iter = messageIds.iterator();
   	
   	while (iter.hasNext())
   	{
   		Long messageID = (Long)iter.next();
   		
   		RecoveryEntry re = (RecoveryEntry)recoveryMap.remove(messageID);
   		
   		//This can actually be null - e.g. if failure happened right after a successful Ack so the client doesn't
   		//remove the delivery but is no longer on the server
   		if (re != null)
   		{   			   		
	   		Delivery del = new SimpleDelivery(this, re.ref);
	   		
	   		if (trace) { log.trace("Recovered ref " + re.ref); }
	   		
	   		refs.add(del);
   		}
   	}
   	   
      return refs;
   }
   
   public void removeStrandedReferences(String sessionID)
   {
   	if (trace) { log.trace("Removing stranded references for session " + sessionID); }
   	
   	// TODO this can be optimised - remove any remaining deliveries for the session id
   	//these correspond to messages buffered on the client side for that client and should go back on the queue
   	
   	Iterator iter = recoveryMap.values().iterator();
   	
   	if (trace) { log.trace("Scanning recovery map for stray entries for session"); }
   	
   	List toCancel = new ArrayList();
   	
   	while (iter.hasNext())
   	{
   		RecoveryEntry re = (RecoveryEntry)iter.next();
   		
   		if (trace) { log.trace("Session id id " + re.sessionID); }
   		
   		if (re.sessionID.equals(sessionID))
   		{
   			MessageReference ref = re.ref;
   			
   			iter.remove();
   			
   			//Put back on queue
   			
   			toCancel.add(ref);   			   						
   		}
   	}
   	
   	for (int i = toCancel.size() - 1; i >= 0; i--)
   	{
   		MessageReference ref = (MessageReference)toCancel.get(i);
   		
   		synchronized (lock)
			{
				messageRefs.addFirst(ref, ref.getMessage().getPriority());
			}
							
			if (trace) { log.trace("Found one, added back on queue"); }   
   	}
   	
   }
   
   public void registerSucker(MessageSucker sucker)
   {
   	if (trace) { log.trace(this + " Registering sucker " + sucker); }
   	
   	synchronized (lock)
   	{
   		if (!suckers.contains(sucker))
   		{
   			suckers.add(sucker);
	   	
   			handleFlowControlForConsumers = true;
   			
   			if (getReceiversReady() && localDistributor.getNumberOfReceivers() > 0)
   			{   				
   				if (trace) { log.trace(this + " receivers ready so setting consumer to true"); }
   			
   				sucker.setConsuming(true);
   			}
   		}
   	}
   }
   
   public boolean unregisterSucker(MessageSucker sucker)
   {
   	synchronized (lock)
   	{
	   	boolean removed = suckers.remove(sucker);
	   	
	   	if (removed)
	   	{
	   		handleFlowControlForConsumers = false;
	   	}
	   	
	   	return removed;
   	}
   }
   
   public int getFullSize()
   {
   	return fullSize;
   }
   
   public int getPageSize()
   {
   	return pageSize;
   }
   
   public int getDownCacheSize()
   {
   	return downCacheSize;
   }
   
   public void addToRecoveryArea(int nodeID, long messageID, String sessionID)
   {
   	if (trace) { log.trace("Adding message id " + messageID + " to recovery area from node " + nodeID); }
   	
   	//Note. The add might find the id already there
      //This could happen after the failover node has moved
   	//When the batch add happens, then an add comes in shortly after but has already been taken into account
   	
   	Integer nid = new Integer(nodeID);
   	
   	Map ids = (Map)recoveryArea.get(nid);
   	
   	if (ids == null)
   	{
   		ids = new ConcurrentHashMap();
   		
   		recoveryArea.put(nid, ids);
   	}
   	
   	ids.put(new Long(messageID), sessionID);
   }
   
   public void removeFromRecoveryArea(int nodeID, long messageID)
   {
   	if (trace) { log.trace("Removing message id " + messageID + " to recovery area from node " + nodeID); }
   	
   	Integer nid = new Integer(nodeID);
   	
   	Map ids = (Map)recoveryArea.get(nid);
   	
   	//The remove might fail to find the id
      //This can happen if the removal has already be done - this could happen after the failover node has moved
   	//When the batch add happens, then an ack comes in shortly after but has already been taken into account

   	if (ids != null && ids.remove(new Long(messageID)) != null)
   	{
   		if (ids.isEmpty())
      	{
      		recoveryArea.remove(nid);
      	}
   	}
   }
   
   public void removeAllFromRecoveryArea(int nodeID)
   {
   	if (trace) { log.trace("Removing all from recovery area for node " + nodeID); }
   	
   	boolean removed = recoveryArea.remove(new Integer(nodeID)) != null;
   		
   	if (trace) { log.trace("Removed:" + removed); }
   }
   
   public void addAllToRecoveryArea(int nodeID, Map ids)
   {
   	if (trace) { log.trace("Adding all from recovery area for node " + nodeID +" set " + ids); }
   	
   	Integer nid = new Integer(nodeID);
   	
   	//Sanity check
   	if (recoveryArea.get(nid) != null)
   	{
   		throw new IllegalStateException("There are already message ids for node " + nodeID);
   	}
   	   	
   	if (!(ids instanceof ConcurrentHashMap))
   	{
   		ids = new ConcurrentHashMap(ids);
   	}
   	
   	if (trace) { log.trace("Adding " + ids.size() + " ids to recovery area for node " + nodeID); }
   	
   	recoveryArea.put(nid, ids);
   	
   	if (trace) { log.trace("Added"); }
   }
   
   public long getRecoverDeliveriesTimeout()
   {
   	return recoverDeliveriesTimeout;
   }
   
   //testing only
   
   public Map getRecoveryArea()
   {
   	return this.recoveryArea;
   }
   
   public int getRecoveryMapSize()
   {
   	return this.recoveryMap.size();
   }
   
   // ChannelSupport overrides --------------------------------------
   
   protected void deliverInternal()
	{
		super.deliverInternal();
		
		if (trace) { log.trace(this + " deliverInternal"); }
   			
		if (handleFlowControlForConsumers && getReceiversReady() && localDistributor.getNumberOfReceivers() > 0)
		{
			//The receivers are still ready for more messages but there is nothing left in the local queue
			//so we inform the message suckers to start consuming (if they aren't already)
			informSuckers(true);
		}
	}
   
   protected void setReceiversReady(boolean receiversReady)
   {
   	if (trace) { log.trace(this + " setReceiversReady " + receiversReady); }
   	
   	this.receiversReady = receiversReady;
   	
   	if (handleFlowControlForConsumers && receiversReady == false)
   	{
   		//No receivers are ready to accept message so tell the suckers to stop consuming
   		informSuckers(false);
   	}
   }
      
   // Public --------------------------------------------------------
   
   public String toString()
   {
      return "Queue[" + System.identityHashCode(this) + "/" + nodeID + "/" + channelID + "-" + name +  "]";
   }
   
   public boolean equals(Object other)
   {
   	if (!(other instanceof MessagingQueue))
   	{
   		return false;
   	}
   	
   	MessagingQueue queue = (MessagingQueue)other;
   	
   	return (this.nodeID == queue.nodeID && this.name.equals(queue.name));
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private void informSuckers(boolean consume)
   {
   	Iterator iter = suckers.iterator();
   	
   	while (iter.hasNext())
   	{
   		MessageSucker sucker = (MessageSucker)iter.next();
   		
   		sucker.setConsuming(consume);
   	}
   }
   
   // Inner classes -------------------------------------------------   
   
   protected class DistributorWrapper implements Distributor
   {
   	private Distributor distributor;
   	
   	protected DistributorWrapper(Distributor distributor)
   	{
   		this.distributor = distributor;
   	}

		public Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
		{
			return distributor.handle(observer, reference, tx);
		}

		public boolean add(Receiver receiver)
		{
			if (trace) { log.trace(this + " attempting to add receiver " + receiver); }
	      
	      synchronized (lock)
	      {	   
		      boolean added = distributor.add(receiver);
				     
		      if (trace) { log.trace("receiver " + receiver + (added ? "" : " NOT") + " added"); }
		      
		      setReceiversReady(true);
		      		      	
		      return added;
	      }
		}

		public void clear()
		{
			synchronized (lock)
	   	{
	   		distributor.clear();
	   	}
		}

		public boolean contains(Receiver receiver)
		{
	      synchronized (lock)
	      {
	      	return distributor.contains(receiver);
	      }
		}

		public int getNumberOfReceivers()
		{
			synchronized (lock)
	   	{
	   		return distributor.getNumberOfReceivers();
	   	}
		}

		public Iterator iterator()
		{
			synchronized (lock)
	   	{
	   		return distributor.iterator();
	   	}
		}

		public boolean remove(Receiver receiver)
		{
			synchronized (lock)
	   	{	   	
		      boolean removed = distributor.remove(receiver);
		      
		      if (removed)
		      {
		      	if (localDistributor.getNumberOfReceivers() == 0)
			      {
			      	//Stop pulling from other queues into this one.
			      	informSuckers(false);
			      	
			      	if (remoteDistributor.getNumberOfReceivers() == 0)
				      {
				         setReceiversReady(false);         
				      }
			      }
		      }		     
		
		      if (trace) { log.trace(this + (removed ? " removed " : " did NOT remove ") + receiver); }
		
		      return removed;
	   	}
		}   	
   }
   
   static class RecoveryEntry
   {
   	String sessionID;
   	
   	MessageReference ref;
   }

   
   private class ClearRecoveryMapTimeoutTarget implements TimeoutTarget
   {
   	private List ids;
   	
   	ClearRecoveryMapTimeoutTarget(List ids)
   	{
   		this.ids = ids;
   	}
   	
		public void timedOut(Timeout timeout)
		{
			if (trace) { log.trace("ClearRecoveryMap timeout fired"); }
			
			Iterator iter = ids.iterator();
			
			boolean added = false;
					
			while (iter.hasNext())
			{
				MessageReference ref = (MessageReference)iter.next();
				
				Object obj = recoveryMap.remove(new Long(ref.getMessage().getMessageID()));
				
				if (obj != null)
				{
					if (trace) { log.trace("Adding ref " + ref + " back into queue"); }
						
					synchronized (lock)
					{		
						messageRefs.addFirst(ref, ref.getMessage().getPriority());		
					}					
					
					added = true;
				}
			}
			
			if (added)
			{
				synchronized (lock)
				{		
					deliverInternal();
				}	
			}
		}   	
   }
}
