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

import java.util.Iterator;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Distributor;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.Receiver;
import org.jboss.messaging.core.impl.tx.Transaction;

/**
 *  
 * This distributor is used when distributing to consumers of clustered queues.
 * 
 * It maintains two round robin distributors - one corresponding to the remote receivers and one corresponding to the local receivers
 * 
 * The local receivers always take priority over the remote receivers
 *  
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1 $</tt>
 * $Id: $
 */
public class ClusterRoundRobinDistributor implements Distributor
{
   // Constants ------------------------------------------------------------------------------------
	
   private static final Logger log = Logger.getLogger(ClusterRoundRobinDistributor.class);
	
   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
   
   private Distributor localDistributor;
   
   private Distributor remoteDistributor;
   
   private boolean preserveOrdering;
   
   // Constructors ---------------------------------------------------------------------------------

   public ClusterRoundRobinDistributor(Distributor local, Distributor remote, boolean preserveOrdering)
   {
      localDistributor = local;
      
      remoteDistributor = remote;
   }

   // Distributor implementation ------------------------------------------------------------------------
   
   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {             
      //First try the local distributor
   	
   	log.info("** first trying with local distributor");
   	
   	Delivery del = localDistributor.handle(observer, ref, tx);
   	
   	log.info("*** local distributor returned " + del);
   	
   	if (del == null)
   	{
   		//If no local distributor takes the ref then we try the remote distributor
   		
   		log.info("** preserve ordering is " + preserveOrdering);
   		
   		if (preserveOrdering)
   		{
   			if (ref.getMessage().getHeader(Message.CLUSTER_SUCKED) != null)
   			{
   				//The message has already been sucked once - don't suck it again
   				
   				return null;
   			}
   			else
   			{
   				//Add the header - so it doesn't get sucked more than once
   				
   				ref.getMessage().putHeader(Message.CLUSTER_SUCKED, new Integer(333));
   			}
   		}
   		
   		log.info("*** sending to remote distributor");
   		
   		String wib = (String)ref.getMessage().getHeader("wib");
   		if (wib == null)
   		{
   			wib = "nodes:";
   		}
   		wib += ((MessagingQueue)observer).getNodeID() + "-";
   		ref.getMessage().putHeader("wib", wib);
   		
   		del = remoteDistributor.handle(observer, ref, tx);
   		
   		log.info("** remote distributor returned " + del);
   	}
   	
   	return del;
   }
   
   public synchronized boolean add(Receiver r)
   {            
   	//FIXME - get the absraction right so this is not necessary
      throw new IllegalStateException("Not used!");     
   }

   public synchronized boolean remove(Receiver r)
   {      
   	//FIXME - get the absraction right so this is not necessary
      throw new java.lang.IllegalStateException("Not used!"); 
   }
   
   public synchronized void clear()
   {
      localDistributor.clear();
      
      remoteDistributor.clear(); 
   }

   public synchronized boolean contains(Receiver r)
   {
      return localDistributor.contains(r) || remoteDistributor.contains(r);     
   }

   public synchronized Iterator iterator()
   {
   	//We only count the local ones
      return localDistributor.iterator();      
   }
   
   public synchronized int getNumberOfReceivers()
   {
      return localDistributor.getNumberOfReceivers() + remoteDistributor.getNumberOfReceivers();
   }

   // Public ---------------------------------------------------------------------------------------
   
   public void addLocal(Receiver r)
   {
   	localDistributor.add(r);
   }
   
   public boolean removeLocal(Receiver r)
   {
   	return localDistributor.remove(r);
   }
   
   public void addRemote(Receiver r)
   {
   	remoteDistributor.add(r);
   }
   
   public boolean removeRemote(Receiver r)
   {
   	return remoteDistributor.remove(r);
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   
   // Inner classes --------------------------------------------------------------------------------
}

