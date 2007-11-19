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

package org.jboss.messaging.core.impl.clusterconnection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.ClusterNotification;
import org.jboss.messaging.core.contract.ClusterNotificationListener;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.contract.Replicator;

import EDU.oswego.cs.dl.util.concurrent.Callable;
import EDU.oswego.cs.dl.util.concurrent.TimedCallable;

/**
 * 
 * This class handles connections to other nodes that are used to pull messages from remote queues to local queues
 *  
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>20 Jun 2007
 *
 * $Id: $
 *
 */
public class ClusterConnectionManager implements ClusterNotificationListener
{
   private static final Logger log = Logger.getLogger(ClusterConnectionManager.class);
   
   private static final long CLOSE_TIMEOUT = 2000;
		
   private boolean trace = log.isTraceEnabled();	
   
	private Map connections;
	
	private boolean xa;
	
	private boolean started;
	
	private int nodeID;
	
	private String connectionFactoryUniqueName;
	
	private Replicator replicator;
	
	private PostOffice postOffice;
	
	private boolean preserveOrdering;
	
	private String suckerUser;
	
	private String suckerPassword;
	
	public ClusterConnectionManager(boolean xa, int nodeID,
			                          String connectionFactoryUniqueName, boolean preserveOrdering,
			                          String suckerUser,
			                          String suckerPassword)
	{
		connections = new HashMap();
		
		this.xa = xa;
		
		this.nodeID = nodeID;
		
		this.connectionFactoryUniqueName = connectionFactoryUniqueName;
		
		this.preserveOrdering = preserveOrdering;
		
		this.suckerUser = suckerUser;
		
		this.suckerPassword = suckerPassword;
		
		if (trace) { log.trace("Created " + this); }
	}
	
	public void injectReplicator(Replicator replicator)
	{
		this.replicator = replicator;
	}
	
	public void injectPostOffice(PostOffice postOffice)
	{
		this.postOffice = postOffice;
	}
		
	public synchronized void start() throws Exception
	{
		if (started)
		{
			return;
		}
		
		if (trace) { log.trace(this + " started"); }
		
		started = true;
	}
	
	public synchronized void stop()
	{
		if (!started)
		{
			return;
		}
		
		Iterator iter = connections.values().iterator();
		
		while (iter.hasNext())
		{
			ConnectionInfo info = (ConnectionInfo)iter.next();
			
			info.close();
		}
		
		connections.clear();
		
		started = false;
		
      if (trace) { log.trace(this + " stopped"); }
	}
	
	public Map getAllConnections()
	{
		return connections;
	}
	
	public void resetAllSuckers()
	{
		Iterator iter = connections.values().iterator();
		
		while (iter.hasNext())
		{
			ConnectionInfo conn = (ConnectionInfo)iter.next();
			
			conn.resetAllSuckers();
		}
	}
	
	public void setIsXA(boolean xa) throws Exception
	{
		boolean needToClose = this.xa != xa;
		if (needToClose)
		{
			closeAllSuckers();
		}
		this.xa = xa;
		if (needToClose)
		{
			createAllSuckers();
		}		
	}
	
	public void closeAllSuckers()
	{
		Iterator iter = connections.values().iterator();
		
		while (iter.hasNext())
		{
			ConnectionInfo conn = (ConnectionInfo)iter.next();
			
			conn.closeAllSuckers();
		}	
	}
	
	/*
	 * We respond to two types of events -
	 * 
	 * 1) Connection factory deployment / undeployment
	 * 
	 * We need to know the connection factories for the nodes so we can create the connections
	 * 
	 * Which connection factory should we use?
	 * 
	 * Clustering uses a "special connection factory" which is deployed on each node
	 * this connection factory has no JNDI bindings - since it is only replicated via the replicator
	 * it is configured in the connection-factories-service.xml file
	 * 
	 * 2) Binds or unbinds
	 * 
	 * When a bind or unbind (either remote or local) of a durable queue comes in we look to see if we have a local queue of the same
	 * name, then look to see if we have a remote queue of the same name, and if so, create a connection to suck from the remote queue
	 * We need to listen for both remote and local binds since the queue may be deployed on the remote node before the local one
	 * or vice versa, in both cases we need to create the connection
	 * 
	 */
	public synchronized void notify(ClusterNotification notification)
	{	
		if (replicator == null)
		{
			//Non clustered
			
			return;
		}
		
		if (trace) { log.trace(this + " notification received " + notification); }
		
		try
		{
			if (notification.type == ClusterNotification.TYPE_REPLICATOR_PUT && notification.data instanceof String)
			{	
				String key = (String)notification.data;
				
				if (key.startsWith(Replicator.CF_PREFIX))
				{
					//A connection factory has been deployed 
		      	//We are only interested in the deployment of our special connection factory
					
		      	String uniqueName = key.substring(Replicator.CF_PREFIX.length());
		      	
		      	if (uniqueName.equals(connectionFactoryUniqueName))
		      	{	      	
						log.trace(this + " deployment of ClusterConnectionFactory");			      
							
		         	synchronized (this)
		         	{
		         		ensureAllConnectionsCreated();
		         		
         				//Now create any suckers for already deployed remote queues - this copes with the case where the CF
         				//is deployed AFTER the queues are deployed
         				
         				createAllSuckers();			         				         	
		         	}
		      	}
				}
			}
			else if (notification.type == ClusterNotification.TYPE_REPLICATOR_REMOVE && notification.data instanceof String)
			{
				String key = (String)notification.data;
				
				if (key.startsWith(Replicator.CF_PREFIX))
				{
					//A connection factory has been undeployed 
		      	//We are only interested in the undeployment of our special connection factory
		      	
		      	String uniqueName = key.substring(Replicator.CF_PREFIX.length());
	
		      	if (uniqueName.equals(connectionFactoryUniqueName))
		      	{	
		      		Map updatedReplicantMap = replicator.get(key);
		      		
	      			List toRemove = new ArrayList();
	      			
	      			Iterator iter = connections.entrySet().iterator();
	         		
	         		while (iter.hasNext())
	         		{
	         			Map.Entry entry = (Map.Entry)iter.next();
	         			
	         			Integer nid = (Integer)entry.getKey();
	         			
	         			if (updatedReplicantMap.get(nid) == null)
	   					{
	         				toRemove.add(nid);
	   					}
	         		}
	         		
	         		iter = toRemove.iterator();
	         		
	         		while (iter.hasNext())
	         		{
	         			Integer nid = (Integer)iter.next();
	         			        			
	         			ConnectionInfo info = (ConnectionInfo)connections.remove(nid);
	         				         			
	         			info.close();
	         		}
	      		}         	
	         }
	      }		
			else if (notification.type == ClusterNotification.TYPE_BIND)
			{
				String queueName = (String)notification.data;
				
				if (trace) { log.trace(this + " bind of queue " + queueName); }
				
				if (notification.nodeID == this.nodeID)
				{
					//Local bind
					
					if (trace) { log.trace(this + " Local bind"); }
					
					ensureAllConnectionsCreated();
					
					//Loook for remote queues
					
					Collection bindings = postOffice.getAllBindingsForQueueName(queueName);
					
					Iterator iter = bindings.iterator();
					
					if (trace) { log.trace(this + " Looking for remote bindings"); }
					
					while (iter.hasNext())
					{
						Binding binding = (Binding)iter.next();
						
						if (trace) { log.trace(this + " Remote binding is " + binding); }
						
						//This will only create it if it doesn't already exist
						
						if (binding.queue.getNodeID() != this.nodeID)
					   {							
							if (trace) { log.trace(this + " Creating sucker"); }
					   
							createSucker(queueName, binding.queue.getNodeID());
						}
					}										
				}
				else
				{
					//Remote bind
					
					if (trace) { log.trace(this + " Remote bind"); }
					
					ensureAllConnectionsCreated();
										
					//Look for local queue
					
					Binding localBinding = postOffice.getBindingForQueueName(queueName);
												
					if (localBinding == null)
					{
						//This is ok - the queue was deployed on the remote node before being deployed on the local node - do nothing for now
						if (trace) { log.trace(this + " There's no local binding"); }
					}
					else
					{
						//The queue has already been deployed on the local node so create a sucker
						
						if (trace) { log.trace(this + " Creating sucker"); }
						
						createSucker(queueName, notification.nodeID);
					}					
				}
			}
			else if (notification.type == ClusterNotification.TYPE_UNBIND)
			{		
				String queueName = (String)notification.data;
				
				if (notification.nodeID == this.nodeID)
				{
					//Local unbind
					
					//We need to remove any suckers corresponding to remote nodes
					
					removeAllSuckers(queueName);
				}
				else
				{
					//Remote unbind
					
					//We need to remove the sucker corresponding to the remote queue
					
					removeSucker(queueName, notification.nodeID);					
				}
			}
		}
		catch (Exception e)
		{
			log.error("Failed to process notification", e);
		}		
	}
	
	public String toString()
	{
		return "ClusterConnectionManager:" + System.identityHashCode(this) + 
		        " xa: " + xa + " nodeID: " + nodeID + " connectionFactoryName: " + connectionFactoryUniqueName;
	}
	
	private void ensureAllConnectionsCreated() throws Exception
	{
		Map updatedReplicantMap = replicator.get(Replicator.CF_PREFIX + connectionFactoryUniqueName);
		
		// Make sure all connections are created
		
		Iterator iter = updatedReplicantMap.entrySet().iterator();
		
		while (iter.hasNext())
		{
			Map.Entry entry = (Map.Entry)iter.next();
			
			Integer nid = (Integer)entry.getKey();
			
			ClientConnectionFactoryDelegate delegate = (ClientConnectionFactoryDelegate)entry.getValue();
			
			if (connections.get(nid) == null)
			{
				try
				{
   				ConnectionInfo info = new ConnectionInfo(new JBossConnectionFactory(delegate), suckerUser, suckerPassword);
   				
   				log.trace(this + " created connection info " + info);
   				
   				connections.put(nid, info);
   				
   				info.start();
   						         							         			
				}
				catch (Exception e)
				{
					log.error("Failed to start connection info ", e);
				}
			}
		}   	
	}
	
	private void createSucker(String queueName, int nodeID) throws Exception
	{
      log.debug("createSucker " + queueName + " nodeID=" + nodeID);

      ConnectionInfo info = (ConnectionInfo)connections.get(new Integer(nodeID));

		if (info == null)
		{
			if (trace) { log.trace("Cluster pull connection factory has not yet been deployed on node " + nodeID); }

			return;
		}

		ConnectionInfo localInfo = (ConnectionInfo)connections.get(new Integer(this.nodeID));

		if (localInfo == null)
		{
			if (trace) { log.trace("Cluster pull connection factory has not yet been deployed on local node"); }

			return;
		}

		//Only create if it isn't already there

		if (!info.hasSucker(queueName))
		{
			if (trace) { log.trace("Creating Sucker for queue " + queueName + " node " + nodeID); }

			// Need to lookup the local queue

			Binding binding = this.postOffice.getBindingForQueueName(queueName);

			Queue localQueue = binding.queue;
			
			if (localQueue.isClustered())
			{				
			   //Find channel id for remote queue - we need this for doing shared DB optimisation
			   Collection coll = this.postOffice.getAllBindingsForQueueName(queueName);
			   Iterator iter = coll.iterator();
			   long sourceChannelID = -1;
			   while (iter.hasNext())
			   {
			      Binding b = (Binding)iter.next();
			      if (b.queue.getNodeID() == nodeID)
			      {
			         sourceChannelID = b.queue.getChannelID();
			      }
			   }
			   if (sourceChannelID == -1)
			   {
			      throw new IllegalArgumentException("Cannot find source channel id");
			   }
			   			   
				MessageSucker sucker = new MessageSucker(localQueue, info.connection, localInfo.connection,
				                                         xa, preserveOrdering, sourceChannelID);
	
				info.addSucker(sucker);
				
				sucker.start();
						
				if (trace) { log.trace("Started it"); }
			}
		}
		else
		{
			if (trace) { log.trace("Sucker for queue " + queueName + " node " + nodeID + " already exists, not creating it"); }
		}
	}
	
	private void removeSucker(String queueName, int nodeID)
	{
      log.debug("removeSucker " + queueName + " nodeID=" + nodeID);

		ConnectionInfo info = (ConnectionInfo)connections.get(new Integer(nodeID));
		
		if (info == null)
		{
			//This is OK, the ClusterPullConnectionfactory might never have been deployed
			return;
		}
		
		MessageSucker sucker = info.removeSucker(queueName);
		
		if (sucker == null)
		{
			//This is OK too
		}
		else
		{		
			sucker.stop();
		}
	}
	
	private void removeAllSuckers(String queueName)
	{
      log.debug("removeAllSuckers " + queueName);

      Iterator iter = connections.values().iterator();
		
		while (iter.hasNext())
		{
			ConnectionInfo info = (ConnectionInfo)iter.next();
			
			MessageSucker sucker = info.removeSucker(queueName);
			
			//Sucker may not exist - this is ok
			
			if (sucker != null)
			{							
				sucker.stop();
			}
		}
	}
	
	private void createAllSuckers() throws Exception
	{
		Collection allBindings = postOffice.getAllBindings();
		
		Iterator iter = allBindings.iterator();
		
		Map nameMap = new HashMap();
								
		//This can probably be greatly optimised
		
		while (iter.hasNext())
		{
			Binding binding = (Binding)iter.next();
			
			if (binding.queue.isClustered())
			{				
				List queues = (List)nameMap.get(binding.queue.getName());
				
				if (queues == null)
				{
					queues = new ArrayList();
					
					nameMap.put(binding.queue.getName(), queues);
				}
				
				queues.add(binding.queue);
			}
		}
		
		iter = nameMap.entrySet().iterator();
		
		while (iter.hasNext())
		{
			Map.Entry entry = (Map.Entry)iter.next();
			
			String queueName = (String)entry.getKey();
			
			List queues = (List)entry.getValue();
			
			//Find a local queue if any
			
			Iterator iter2 = queues.iterator();
			
			Queue localQueue = null;
			
			while (iter2.hasNext())
			{
				Queue queue = (Queue)iter2.next();
				
				if (queue.getNodeID() == this.nodeID)
				{
					localQueue = queue;
					
					break;
				}
			}
			
			if (localQueue != null)
			{
				iter2 = queues.iterator();
				
				while (iter2.hasNext())
				{
					Queue queue = (Queue)iter2.next();
					
					if (queue.getNodeID() != this.nodeID && queue.isClustered())
					{
						//Now we have found a local and remote with matching names - so we can create a sucker
																		
						createSucker(queueName, queue.getNodeID());
					}
				}
			}					
		}
	}
				
	class ConnectionInfo
	{
		private JBossConnectionFactory connectionFactory;
		
		private JBossConnection connection;
		
		private Map suckers;
		
		private boolean started;
		
		private String suckerUser;
		
		private String suckerPassword;
		
		ConnectionInfo(JBossConnectionFactory connectionFactory, String suckerUser, String suckerPassword) throws Exception
		{
			this.connectionFactory = connectionFactory;
			
			this.suckers = new HashMap();
			
			this.suckerUser = suckerUser;
			
			this.suckerPassword = suckerPassword;
		}
		
		synchronized void start() throws Exception
		{			
			if (started)			
			{
				return;
			}
			
			if (connection == null)
		   {
				connection = (JBossConnection)connectionFactory.createConnection(suckerUser, suckerPassword);			
		   }
			
			connection.start();
			
			started = true;
		}
		
		synchronized void stop() throws Exception
		{
			if (!started)
			{
				return;
			}
			
			connection.stop();
			
			started = false;
		}
		
		synchronized void resetAllSuckers()
		{
			Iterator iter = suckers.values().iterator();
			
			while (iter.hasNext())
			{
				MessageSucker sucker = (MessageSucker)iter.next();
				
				sucker.setConsuming(false);
			}
		}
		
		synchronized void closeAllSuckers()
		{
			Iterator iter = suckers.values().iterator();
			
			while (iter.hasNext())
			{
				MessageSucker sucker = (MessageSucker)iter.next();
				
				sucker.stop();
			}
			
			suckers.clear();
		}
		
		synchronized void close()
		{
			closeAllSuckers();			
			
			//Note we use a timed callable since remoting has a habit of hanging on attempting to close
			//We do not want this to hang the system - especially failover
			
			Callable callable = new Callable() { public Object call()
			{
				try
				{
					connection.close();
				}
				catch (JMSException ignore)
				{					
				}
				return null;
		    } };
			
			Callable timedCallable = new TimedCallable(callable, CLOSE_TIMEOUT);
			
			try
			{
				timedCallable.call();
			}
			catch (Throwable t)
			{
				//Ignore - the server might have already closed - so this is ok
			}
			
			connection = null;
			
			started = false;
		}
		
		synchronized boolean hasSucker(String queueName)
		{
			return suckers.containsKey(queueName);
		}
		
		synchronized void addSucker(MessageSucker sucker)
		{
			if (suckers.containsKey(sucker.getQueueName()))
			{
				throw new IllegalStateException("Already has sucker for queue " + sucker.getQueueName());
			}
			
			suckers.put(sucker.getQueueName(), sucker);
		}
		
		synchronized MessageSucker removeSucker(String queueName)
		{
			MessageSucker sucker = (MessageSucker)suckers.remove(queueName);
			
			return sucker;
		}		
	}	
}
