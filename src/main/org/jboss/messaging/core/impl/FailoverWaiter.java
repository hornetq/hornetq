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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.ClusterNotification;
import org.jboss.messaging.core.contract.ClusterNotificationListener;
import org.jboss.messaging.core.impl.tx.TransactionRepository;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>22 Jun 2007
 *
 * $Id: $
 *
 */
public class FailoverWaiter implements ClusterNotificationListener
{
   private static final Logger log = Logger.getLogger(FailoverWaiter.class);
	
	private int failingOverFor = -1;
	
	private int failedOverFor = -1;
	
   private Object failoverStatusLock;
   
   private long failoverStartTimeout;
   
   private long failoverCompleteTimeout;
   
   private int nodeID;
     
   private TransactionRepository txRepository;
   
   public FailoverWaiter(int nodeID, long failoverStartTimeout, long failoverCompleteTimeout,
   		                TransactionRepository txRepository)
   {
      failoverStatusLock = new Object();
      
      this.nodeID = nodeID;
      
      this.failoverStartTimeout = failoverStartTimeout;
      
      this.failoverCompleteTimeout = failoverCompleteTimeout;
      
      this.txRepository = txRepository;
   }
   
   /*
    * Wait for failover from the specified node to complete.
    */
   public int waitForFailover(int failedNodeID) throws Exception
   {   	
   	log.trace("Waiting for failover for " + failedNodeID +
   			    " failingOverFor: " + failingOverFor + " failedOverFor: " + failedOverFor);   	
   	
      // This node may be failing over for another node - in which case we must wait for that to be
      // complete.
   	
   	// TODO deal with multiple failover cascades
   	
   	//First wait for failover to start
   	synchronized (failoverStatusLock)
   	{
         long startToWait = failoverStartTimeout;
            		
   		while (startToWait > 0 && failingOverFor != failedNodeID && failedOverFor != failedNodeID)
   		{
   			long start = System.currentTimeMillis(); 
            try
            {
               log.debug(this + " blocking on the failover lock, waiting for failover to start");
               failoverStatusLock.wait(startToWait);
               log.debug(this + " releasing the failover lock, checking again whether failover started ...");
            }
            catch (InterruptedException ignore)
            {                  
            }
            startToWait -= System.currentTimeMillis() - start;  
   		}
   		
   		if (failingOverFor != failedNodeID && failedOverFor != failedNodeID)
   		{
   			//Timed out
   			log.debug("Timed out waiting for failover to start");
   			
   			return -1;
   		}
   	}
   	
   	//Wait for failover to complete
   	synchronized (failoverStatusLock)
   	{
   		long completeToWait = failoverCompleteTimeout;
   		
   		while (completeToWait > 0 && failedOverFor != failedNodeID)
   		{
   			long start = System.currentTimeMillis(); 
            try
            {
               log.debug(this + " blocking on the failover lock, waiting for failover to complete");
               failoverStatusLock.wait(completeToWait);
               log.debug(this + " releasing the failover lock, checking again whether failover completed ...");
            }
            catch (InterruptedException ignore)
            {                  
            }
            completeToWait -= System.currentTimeMillis() - start;  
   		}
   		
   		if (failedOverFor != failedNodeID)
   		{
   			//Timed out
   			log.debug("Timed out waiting for failover to complete");
   			
   			return -1;
   		}
   	}
   	
   	return nodeID;
   }

	public void notify(ClusterNotification notification)
	{
		if (notification.type == ClusterNotification.TYPE_FAILOVER_START)
		{
			synchronized (failoverStatusLock)
			{
				failingOverFor = notification.nodeID;

				failoverStatusLock.notifyAll();
			}
		}
		else if (notification.type == ClusterNotification.TYPE_FAILOVER_END)
		{
			//	We prompt txRepository to load any prepared txs - so we can take over
			// responsibility for in doubt transactions from other nodes
			try
			{
				txRepository.loadPreparedTransactions();
			}
			catch (Exception e)
			{
				log.error("Failed to load prepared transactions", e);
			}

			synchronized (failoverStatusLock)
			{									
				failedOverFor = failingOverFor;

				failingOverFor = -1;

				failoverStatusLock.notifyAll();
			}
		}
		else if (notification.type == ClusterNotification.TYPE_NODE_JOIN)
		{
			synchronized (failoverStatusLock)
			{
				//A node that we previously failed over for has been restarted so we wipe the failover status
				//It is vital that we do this otherwise if the resurrected node subsequently fails again
				//when connections try to reconnect they will think that failover is already complete
				if (notification.nodeID == failedOverFor)
				{
					failedOverFor = -1;
					
					failoverStatusLock.notifyAll();
				}
			}
		}
	}	
}
