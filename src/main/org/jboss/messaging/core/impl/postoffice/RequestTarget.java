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
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.contract.Message;
import org.jgroups.Address;

/**
 * 
 * This interface contains the methods that ClusterRequest instances can call
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2421 $</tt>
 *
 * $Id: PostOfficeInternal.java 2421 2007-02-25 00:06:06Z timfox $
 *
 */
interface RequestTarget
{		
   void addBindingFromCluster(MappingInfo mapping, boolean allNodes) throws Exception;
   
   void removeBindingFromCluster(MappingInfo mapping, boolean allNodes) throws Throwable;
 
   void handleNodeJoined(int nodeId, PostOfficeAddressInfo info) throws Exception;
   
   void handleNodeLeft(int nodeId) throws Exception;
   
   void putReplicantLocally(int nodeId, Serializable key, Serializable replicant) throws Exception;
   
   boolean removeReplicantLocally(int nodeId, Serializable key) throws Exception;
   
   void routeFromCluster(Message message, String routingKeyText, Set queueNames) throws Exception;
   
   //TODO - these don't belong here
   
   void handleReplicateDelivery(int nodeID, String queueName, String sessionID, long messageID,
   		                       long deliveryID, Address replyAddress) throws Exception;
   
   void handleReplicateAck(int nodeID, String queueName, long messageID) throws Exception;
   
   void handleReplicateDeliveryAck(String sessionID, long deliveryID) throws Exception;
   
   void handleAckAllReplicatedDeliveries(int nodeID) throws Exception;
   
   void handleAddAllReplicatedDeliveries(int nodeID, Map deliveries) throws Exception;
   
   void handleGetReplicatedDeliveries(String queueName, Address returnAddress) throws Exception;
}
