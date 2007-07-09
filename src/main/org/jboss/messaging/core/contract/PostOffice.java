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
package org.jboss.messaging.core.contract;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.impl.tx.Transaction;

/**
 * 
 * A post office holds bindings of queues to conditions.
 * 
 * When routing a reference, the post office routes the reference to any binding whose condition matches
 * the condition specified in the call to route(...)
 * 
 * A queue can only be bound with one condition in the post office
 * 
 * Each queue must have a unique name and channel ID
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public interface PostOffice extends MessagingComponent
{
	/**
	 * Get the name of the post office
	 * 
	 * @return The name of this post office
	 */
   String getOfficeName();
   
   /**
    * Add a binding to the post office
    * @param binding The binding to add
    * @param allNodes Add this binding on ALL nodes?
    * @throws Exception
    */
   boolean addBinding(Binding binding, boolean allNodes) throws Exception;

   /**
    * Remove a binding from the post office
    * @param queueName The queue name that identifies the binding
    * @param allNodes Remove this binding from ALL node?
    * @throws Throwable
    */
   Binding removeBinding(String queueName, boolean allNodes) throws Throwable;
   
   /**
    * Route a reference.
    *
    * @param condition - the message will be routed to queue(s) if the specified condition matches the
    *        condition of the bindings.
    * @param tx - the transaction or null if not in the context of a transaction.
    *
    * @return true if reference was accepted by at least one queue.
    */
   boolean route(MessageReference ref, Condition condition, Transaction tx) throws Exception; 
   
   /**
    * Get all queues that match the condition
    * @param condition The condition
    * @param localOnly Only retrieve local queues ?
    * @return
    * @throws Exception
    */
   Collection getQueuesForCondition(Condition condition, boolean localOnly) throws Exception;
   
   /**
    * Get the binding with the specified queue name
    * @param queueName
    * @return
    * @throws Exception
    */
   Binding getBindingForQueueName(String queueName) throws Exception;
   
   /**
    * Get the binding with the specified channel ID
    * @param channelID
    * @return
    * @throws Exception
    */
   Binding getBindingForChannelID(long channelID) throws Exception;
   
   /**
    * Get all bindings with the specified queue name (They will be on different nodes)
    * @param queueName
    * @return
    * @throws Exception
    */
   Collection getAllBindingsForQueueName(String queueName) throws Exception;
   
   /**
    * Get all the bindings
    * @return
    * @throws Exception
    */
   Collection getAllBindings() throws Exception;
   
   /**
    * Is this post office clustered?
    * 
    * @return true If the post office is clustered
    */
   boolean isClustered();   

   /**
    * Get the failover map
    * @return
    */
   Map getFailoverMap();
   
   /**
    * Get a set of nodes in the cluster
    * @return
    */
   Set nodeIDView();
   
   //FIXME - these do not belong here - only here temporarily until we implement generic Handler/Message abstraction
   
   void sendReplicateDeliveryMessage(String queueName, String sessionID, long messageID, long deliveryID, boolean reply, boolean sync)
   	throws Exception;

	void sendReplicateAckMessage(String queueName, long messageID) throws Exception;
	
	boolean isFirstNode();
	
	
	//For testing only
	Map getRecoveryArea(String queueName);
   
   int getRecoveryMapSize(String queueName);
}

