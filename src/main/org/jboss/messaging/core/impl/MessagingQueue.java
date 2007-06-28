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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Distributor;
import org.jboss.messaging.core.contract.Filter;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.contract.Receiver;
import org.jboss.messaging.core.impl.clusterconnection.MessageSucker;
import org.jboss.messaging.core.impl.tx.Transaction;

/**
 * 
 * A MessagingQueue
 * 
 * Can be used to implement a point to point queue, or a subscription fed from a topic
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
   
   private boolean preserveOrdering;
            
   // Constructors --------------------------------------------------
       
   public MessagingQueue(int nodeID, String name, long id, MessageStore ms, PersistenceManager pm,             
                         boolean recoverable, int maxSize, Filter filter,
                         int fullSize, int pageSize, int downCacheSize, boolean clustered,
                         boolean preserveOrdering)
   {
      super(id, ms, pm, recoverable, maxSize, fullSize, pageSize, downCacheSize);
      
      setup(nodeID, name, filter, clustered, preserveOrdering);
   }
   
   /** This constructor is used when loading queue from storage - the paging params, maxSize and preserveOrdering don't matter
    * they are injected when the queue or topic service is started
    */
   public MessagingQueue(int nodeID, String name, long id, MessageStore ms, PersistenceManager pm,             
					          boolean recoverable, Filter filter,
					          boolean clustered)
	{
		super(id, ms, pm, recoverable, -1, 100000, 2000, 2000); //paging params etc are actually ignored
		
		setup(nodeID, name, filter, clustered, false);
	}
   
   /* This constructor is only used in tests - should we remove it? */
   public MessagingQueue(int nodeID, String name, long id, MessageStore ms, PersistenceManager pm,             
   		                boolean recoverable, int maxSize, Filter filter, boolean clustered,
   		                boolean preserveOrdering)
   {
   	super(id, ms, pm, recoverable, maxSize);

   	setup(nodeID, name, filter, clustered, preserveOrdering);
   }
   
   /* Constructor for a remote queue representation in a cluster */
   public MessagingQueue(int nodeID, String name, long id, boolean recoverable,
   		                Filter filter, boolean clustered)
   {
   	super(id, null, null, recoverable, -1);
   	
   	setup(nodeID, name, filter, clustered, false);   	
   }
   
   private void setup(int nodeID, String name, Filter filter, boolean clustered, boolean preserveOrdering)
   {
   	this.nodeID = nodeID;

   	this.name = name;

   	this.filter = filter;

   	this.clustered = clustered;
   	
   	this.preserveOrdering = preserveOrdering;
   	
   	localDistributor = new DistributorWrapper(new RoundRobinDistributor());
   	
   	remoteDistributor = new DistributorWrapper(new RoundRobinDistributor());
   	
   	distributor = new ClusterRoundRobinDistributor(localDistributor, remoteDistributor, preserveOrdering);
   	
   	suckers = new HashSet();
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
   public void mergeIn(long channelID) throws Exception
   {
      if (trace) { log.trace("Merging queue " + channelID + " into " + this); }
           
      synchronized (lock)
      {
         flushDownCache();
                  
         PersistenceManager.InitialLoadInfo ili =
            pm.mergeAndLoad(channelID, channelID, fullSize - messageRefs.size(),
                            firstPagingOrder, nextPagingOrder);
            
         if (trace) { log.trace("Loaded " + ili.getRefInfos().size() + " refs"); }            

         doLoad(ili);         
         
         deliverInternal();
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
   
   public boolean isPreserveOrdering()
   {
   	return this.preserveOrdering;
   }
   
   public void setPreserveOrdering(boolean preserveOrdering)
   {
      this.preserveOrdering = preserveOrdering;   	
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
      return "Queue[" + nodeID + "/" + channelID + "-" + name +  "]";
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
}
